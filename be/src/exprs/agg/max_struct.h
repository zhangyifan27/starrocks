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

#include <limits>
#include <type_traits>

#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_traits.h"
#include "gutil/casts.h"
#include "util/raw_container.h"

namespace starrocks {

struct MaxStructSemiState {
    int compare_struct(const Column& column, size_t offset, const ColumnPtr& current_max) {
        const auto* struct_column = down_cast<const StructColumn*>(&column);
        return struct_column->compare_at(offset, 0, *current_max, -1);
    }

    void update(FunctionContext* ctx, const Column& column, size_t offset) {
        if (!has_value || compare_struct(column, offset, data_column) > 0) {
            data_column = ctx->create_column(*ctx->get_arg_type(0), false);
            data_column->append(column, offset, 1);
            has_value = true;
        }
    }

    ColumnPtr data_column = nullptr;
    bool has_value = false;
};

class MaxStructSemiAggregateFunction final
        : public AggregateFunctionBatchHelper<MaxStructSemiState, MaxStructSemiAggregateFunction> {
public:
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state, size_t row_num) const override {
        this->data(state).update(ctx, *columns[0], row_num);
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        this->data(state).update(ctx, *columns[0], 0);
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        this->data(state).update(ctx, *column, row_num);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (this->data(state).data_column != nullptr) {
            to->append(*(this->data(state).data_column.get()));
        }
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        *dst = src[0];
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (this->data(state).data_column != nullptr) {
            to->append(*(this->data(state).data_column.get()));
        }
    }

    std::string get_name() const override { return "max_struct"; }
};

} // namespace starrocks
