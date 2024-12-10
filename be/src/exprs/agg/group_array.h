// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/hash_set.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/function_context.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "util/defer_op.h"

namespace starrocks {

template <LogicalType LT>
struct GroupArrayAggregateState {
    using CppType = RunTimeCppType<LT>;

    void update(CppType key) {
        if (limit_size != 0 && items.size() >= limit_size) {
            return;
        }
        items.emplace_back(key); 
    }

    void update_slice(MemPool* mem_pool, Slice key) {
        if (limit_size != 0 && items.size() >= limit_size) {
            return;
        }
        uint8_t* pos = mem_pool->allocate(key.size);
        memcpy(pos, key.data, key.size);
        items.emplace_back(Slice(pos, key.size));
    }

    std::vector<CppType> items;
    int32_t limit_size = 0;
    int32_t null_count = 0;
};

template <LogicalType LT>
class GroupArrayAggregateFunction
        : public AggregateFunctionBatchHelper<GroupArrayAggregateState<LT>, GroupArrayAggregateFunction<LT>> {
public:
    using InputCppType = RunTimeCppType<LT>;
    using InputColumnType = RunTimeColumnType<LT>;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {

        const auto& column = down_cast<const InputColumnType&>(*columns[0]);

        if constexpr (IsSlice<InputCppType>) {
            this->data(state).update_slice(ctx->mem_pool(), column.get_slice(row_num));
        } else {
            this->data(state).update(column.get_data()[row_num]);
        }

        if (ctx->get_num_args() == 2 && this->data(state).limit_size == 0) {
            if (ctx->is_notnull_constant_column(1)) {
                auto const_column_limit_size = ctx->get_constant_column(1);
                int32_t limit_size = ColumnHelper::get_const_value<TYPE_INT>(const_column_limit_size);
                this->data(state).limit_size = limit_size;
            }
        }
    }

    void process_null(FunctionContext* ctx, AggDataPtr __restrict state) const override {
        this->data(state).null_count++;
        // NOTE: need to parse limit size, otherwise, it will be overwritten in merge
        if (ctx->get_num_args() == 2 && this->data(state).limit_size == 0) {
            if (ctx->is_notnull_constant_column(1)) {
                auto const_column_limit_size = ctx->get_constant_column(1);
                this->data(state).limit_size = ColumnHelper::get_const_value<TYPE_INT>(const_column_limit_size);
            }
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());
        Slice slice = column->get(row_num).get_slice();
        auto data_ptr = slice.data;
        int32_t limit_size = *reinterpret_cast<int32_t*>(data_ptr);
        data_ptr = data_ptr + sizeof(int32_t);

        int32_t null_count = *reinterpret_cast<int32_t*>(data_ptr);
        data_ptr = data_ptr + sizeof(int32_t);

        size_t items_size = *reinterpret_cast<size_t*>(data_ptr);
        data_ptr = data_ptr + sizeof(size_t);

        for (int i = 0; i < items_size; i++) {
            if constexpr (IsSlice<InputCppType>) {
                size_t char_size = *reinterpret_cast<size_t*>(data_ptr);
                data_ptr = data_ptr + sizeof(size_t);
                this->data(state).update_slice(ctx->mem_pool(), Slice(data_ptr, char_size ));
                data_ptr = data_ptr + char_size;

            } else {
                this->data(state).update(*reinterpret_cast<InputCppType*>(data_ptr + i * sizeof(InputCppType)));
            }
        }
        this->data(state).limit_size = limit_size;
        this->data(state).null_count += null_count;
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        const auto& items_vector = this->data(state).items;
        if (items_vector.empty() && this->data(state).null_count == 0) {
            return;
        }

        if constexpr (IsSlice<InputCppType>) {
            auto* column = down_cast<BinaryColumn*>(to);
            Bytes& bytes = column->get_bytes();
            size_t old_size = bytes.size();
            size_t items_size = items_vector.size();
            // should serialize: limit_size, null_count, vector size, all vector element.
            size_t new_size = old_size + sizeof(int32_t) + sizeof(int32_t) + sizeof(size_t);

            size_t char_size = 0;
            for (int i=0; i<items_size; i++) {
                char_size = char_size + items_vector[i].get_size() + sizeof(size_t);
            }
            bytes.resize(new_size + char_size);

            auto data_ptr = bytes.data() + old_size;
            memcpy(data_ptr, &(this->data(state).limit_size), sizeof(int32_t));
            data_ptr = data_ptr + sizeof(int32_t);

            memcpy(data_ptr, &(this->data(state).null_count), sizeof(int32_t));
            data_ptr = data_ptr + sizeof(int32_t);

            memcpy(data_ptr, &items_size, sizeof(size_t));
            data_ptr = data_ptr + sizeof(size_t);

            for (int i=0; i<items_size; i++) {
                size_t value_size = items_vector[i].get_size();
                memcpy(data_ptr, &value_size, sizeof(size_t));
                data_ptr = data_ptr + sizeof(size_t);

                memcpy(data_ptr, items_vector[i].get_data(), value_size);
                data_ptr = data_ptr + value_size;
            }
            column->get_offset().emplace_back(new_size + char_size);
            return;
        }
        auto* column = down_cast<BinaryColumn*>(to);
        Bytes& bytes = column->get_bytes();
        size_t old_size = bytes.size();
        size_t items_size = items_vector.size();
        // should serialize: limit_size, null_count, vector size, all vector element.
        size_t new_size = old_size + sizeof(int32_t) + sizeof(int32_t) + sizeof(size_t) + items_size * sizeof(InputCppType);
        bytes.resize(new_size);

        auto data_ptr = bytes.data() + old_size;
        memcpy(data_ptr, &(this->data(state).limit_size), sizeof(int32_t));
        data_ptr = data_ptr + sizeof(int32_t);

        memcpy(data_ptr, &(this->data(state).null_count), sizeof(int32_t));
        data_ptr = data_ptr + sizeof(int32_t);

        memcpy(data_ptr, &items_size, sizeof(size_t));
        data_ptr = data_ptr + sizeof(size_t);
        if (items_size != 0) {
            memcpy(data_ptr, items_vector.data(), items_size * sizeof(InputCppType));
        }
        column->get_offset().emplace_back(new_size);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        const auto& data = this->data(state).items;
        auto* column = down_cast<ArrayColumn*>(to);
        auto& offsets = column->offsets_column()->get_data();
        auto& elements_column = column->elements_column();

        int32_t limit_size = std::min(this->data(state).limit_size, static_cast<int32_t>(data.size()) + this->data(state).null_count);
        if (limit_size <= 0) {
            limit_size = data.size() + this->data(state).null_count;
        }

        int32_t i = 0;
        for (; i < limit_size; i++) {
            if (i >= data.size()) {
                break;
            }
            elements_column->append_datum(data[i]);
        }
        elements_column->append_nulls(limit_size - i);
        offsets.emplace_back(offsets.back() + limit_size);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size, ColumnPtr* dst) const override {
        if (chunk_size <= 0) {
            return;
        }
        auto* dst_column = down_cast<BinaryColumn*>((*dst).get());
        Bytes& bytes = dst_column->get_bytes();
        size_t old_size = bytes.size();

        int32_t limit_size = 0;
        InputColumnType* src_column;
        auto const_column_limit_size = ctx->get_constant_column(1);
        if (const_column_limit_size != nullptr) {
            limit_size = ColumnHelper::get_const_value<TYPE_INT>(const_column_limit_size);
        }

        NullableColumn* nullable_column = nullptr;
        if (src[0]->is_nullable()) {
            nullable_column = down_cast<NullableColumn*>(src[0].get());
            src_column = down_cast<InputColumnType*>(nullable_column->data_column().get());
        } else {
            src_column = down_cast<InputColumnType*>(src[0].get());
        }
        // must resize after write data
        // format: limit_size, null_count, vector_size, value_size , value_data
        int new_bytes_size = 0;
        for (int i = 0; i < chunk_size; i++) {
            new_bytes_size += sizeof(int32_t) + sizeof(int32_t) + sizeof(size_t);
            if (nullable_column != nullptr && nullable_column->is_null(i)) {
                continue;
            }
            if constexpr (IsSlice<InputCppType>) {
                Slice value = src_column->get_slice(i);
                new_bytes_size += value.size + sizeof(size_t);
            } else {
                new_bytes_size += sizeof(InputCppType);
            }
        }
        bytes.resize(new_bytes_size);
        dst_column->get_offset().resize(chunk_size + 1);

        for (int i = 0; i < chunk_size; ++i) {
            int32_t null_count = 0;
            size_t vector_size = 1;
            if (nullable_column != nullptr && nullable_column->is_null(i)) {
                null_count = 1;
                vector_size = 0;
            }
            memcpy(bytes.data() + old_size, &limit_size, sizeof(int32_t));
            memcpy(bytes.data() + old_size + sizeof(int32_t), &null_count, sizeof(int32_t));
            memcpy(bytes.data() + old_size + sizeof(int32_t) * 2, &vector_size, sizeof(size_t));
            old_size += sizeof(int32_t) * 2 + sizeof(size_t);
            if (vector_size == 1) {
                if constexpr (IsSlice<InputCppType>) {
                    Slice value = src_column->get_slice(i);
                    memcpy(bytes.data() + old_size, &value.size, sizeof(size_t));
                    memcpy(bytes.data() + old_size + sizeof(size_t), value.data, value.size);
                    old_size += value.size + sizeof(size_t);
                } else {
                    std::vector<InputCppType> items_vector;
                    items_vector.emplace_back(src_column->get_data()[i]);
                    memcpy(bytes.data() + old_size, items_vector.data(), sizeof(InputCppType));
                    old_size +=  sizeof(InputCppType);
                }
            }
            dst_column->get_offset()[i + 1] = old_size;
        }
    }

    std::string get_name() const override { return "group_array"; }
};

} // namespace starrocks::vectorized
