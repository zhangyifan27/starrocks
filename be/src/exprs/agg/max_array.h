// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/function_context.h"
#include "types/logical_type.h"

namespace starrocks {


template <LogicalType PT>
struct MaxArrayAggregateState {
    using ColumnType = RunTimeColumnType<PT>;
    using CppType = RunTimeCppType<PT>;

    void update(MemPool* mem_pool, const ArrayColumn* column, size_t offset, size_t size) {
        auto& input_element = down_cast<const NullableColumn&>(column->elements());
        if (is_null) {
            data_column.append(input_element.data_column_ref(), offset, size);
            null_column.append(input_element.null_column_ref(), offset, size);
            is_null = false;
        } else {
            size_t min_len = std::min(size, data_column.size());
            bool input_larger = false;
            for (size_t i = 0; i < min_len; ++i) {
                size_t input_element_index = i + offset;
                if (input_element.is_null(input_element_index) && !null_column.is_null(i)) {
                    break;
                }
                if (input_element.is_null(input_element_index) && null_column.is_null(i)) {
                    continue;
                }
                if (!input_element.is_null(input_element_index) && null_column.is_null(i)) {
                    input_larger = true;
                    break;
                }
                CppType input_elem = input_element.get(input_element_index).get<CppType>();
                CppType cur_elem = data_column.get_data()[i];
                if (input_elem == cur_elem) {
                    continue;
                }
                if (input_elem > cur_elem) {
                    input_larger = true;
                }
                break;
            }
            if (input_larger) {
                data_column.reset_column();
                data_column.append(input_element.data_column_ref(), offset, size);
                null_column.reset_column();
                null_column.append(input_element.null_column_ref(), offset, size);
            }
        }
    }

    const ColumnType* get_data_column() {
        return &data_column;
    }

    const NullColumn* get_null_column() {
        return &null_column;
    }


    bool is_null = true;
    ColumnType data_column;
    NullColumn null_column;
};

template <LogicalType LT>
class MaxArrayAggregateFunction
        : public AggregateFunctionBatchHelper<MaxArrayAggregateState<LT>,
                                              MaxArrayAggregateFunction<LT>> {
public:
    using InputColumnType = RunTimeColumnType<LT>;

    void update_state(FunctionContext* ctx, const ArrayColumn* input_column, AggDataPtr __restrict state,
                      size_t row_num) const {
        // Array element is nullable, so we need to extract the data from nullable column first
        auto offset_size = input_column->get_element_offset_size(row_num);
        auto& array_element = down_cast<const NullableColumn&>(input_column->elements());
        size_t element_null_count = array_element.null_count(offset_size.first, offset_size.second);
        DCHECK_LE(element_null_count, offset_size.second);
        if (element_null_count == offset_size.second) return;
        this->data(state).update(ctx->mem_pool(), input_column, offset_size.first, offset_size.second);
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        const auto* input_column = down_cast<const ArrayColumn*>(columns[0]);
        update_state(ctx, input_column, state, row_num);
    }

    void process_null(FunctionContext* ctx, AggDataPtr __restrict state) const override {
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        const auto* input_column = down_cast<const ArrayColumn*>(column);
        update_state(ctx, input_column, state, row_num);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& state_impl = this->data(const_cast<AggDataPtr>(state));
        auto* input_column = down_cast<ArrayColumn*>(to);
        auto* array_element = down_cast<NullableColumn*>(input_column->elements_column().get());
        auto* element_data_column = down_cast<InputColumnType*>(array_element->mutable_data_column());
        auto* null_column = down_cast<NullColumn*>(array_element->mutable_null_column());
        if (!state_impl.is_null) {
            element_data_column->append(*(state_impl.get_data_column()), 0, state_impl.get_data_column()->size());
            null_column->append(*(state_impl.get_null_column()), 0, state_impl.get_null_column()->size());
            input_column->offsets_column()->append(state_impl.get_data_column()->size());
        } else {
            input_column->offsets_column()->append(0);
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        return serialize_to_column(ctx, state, to);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        (*dst)->append(*(src[0].get()));
    }

    std::string get_name() const override { return "max_array"; }
};
} // namespace starrocks
