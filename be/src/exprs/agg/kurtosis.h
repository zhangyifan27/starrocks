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

#include <cmath>

#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/moment.h"
#include "exprs/arithmetic_operation.h"
#include "exprs/helpers/serialize_helpers.hpp"
#include "types/logical_type.h"

namespace starrocks {

using KurtosisAggregateState = MomentCalculator<long double, 4>;

template <LogicalType LT, bool is_sample, typename T = RunTimeCppType<LT>>
class KurtosisAggregateFunction
        : public AggregateFunctionBatchHelper<KurtosisAggregateState, KurtosisAggregateFunction<LT, is_sample, T>> {
public:
    using InputColumnType = RunTimeColumnType<LT>;
    using InputCppType = T;
    using ResultColumnType = RunTimeColumnType<TYPE_DOUBLE>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        DCHECK(ctx->get_num_args() == 1);

        const auto* col = down_cast<const InputColumnType*>(columns[0]);

        InputCppType value = col->get_data()[row_num];

        this->data(state).update(value);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());
        const uint8_t* serialized_data = reinterpret_cast<const uint8_t*>(column->get(row_num).get_slice().data);
        KurtosisAggregateState other(serialized_data);
        this->data(state).merge(other);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_binary());
        auto* column = down_cast<BinaryColumn*>(to);
        Bytes& bytes = column->get_bytes();

        size_t old_size = bytes.size();
        size_t new_size = old_size + this->data(state).serialized_size();
        bytes.resize(new_size);
        column->get_offset().emplace_back(new_size);
        uint8_t* serialized_data = bytes.data() + old_size;
        this->data(state).serialize(serialized_data);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_numeric() || to->is_decimal());

        auto* column = down_cast<ResultColumnType*>(to);
        double kurtosis = this->data(state).template kurtosis<is_sample>();
        column->append(kurtosis);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        DCHECK((*dst)->is_binary());
        auto* dst_column = down_cast<BinaryColumn*>((*dst).get());

        std::vector<const Column*> cols;
        std::for_each(src.begin(), src.end(), [&cols](const ColumnPtr& col) { cols.emplace_back(col.get()); });
        for (size_t i = 0; i < chunk_size; ++i) {
            KurtosisAggregateState state;
            update(ctx, cols.data(), reinterpret_cast<AggDataPtr>(&state), i);
            Bytes& bytes = dst_column->get_bytes();
            size_t old_size = bytes.size();
            size_t new_size = old_size + state.serialized_size();
            bytes.resize(new_size);
            dst_column->get_offset().emplace_back(new_size);
            uint8_t* serialized_data = bytes.data() + old_size;
            state.serialize(serialized_data);
            DCHECK_EQ(serialized_data, new_size + bytes.data());
        }
    }

    std::string get_name() const override {
        if constexpr (is_sample) {
            return "kurt_samp";
        } else {
            return "kurt_pop";
        }
    }
};

} // namespace starrocks
