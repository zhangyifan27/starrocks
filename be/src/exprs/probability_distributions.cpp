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

#include "probability_distributions.h"

#include <boost/math/distributions/normal.hpp>

#include "column/column_builder.h"
#include "column/column_viewer.h"

namespace starrocks {

StatusOr<ColumnPtr> ProbabilityDistributions::percent_point_function(FunctionContext* context, const Columns& columns) {
    if (columns.size() != 1) {
        return Status::InvalidArgument("percent_point_function() requires 1 argument, but got " +
                                       std::to_string(columns.size()));
    }
    size_t num_rows = columns[0]->size();
    if (num_rows == 0) {
        return ColumnBuilder<TYPE_DOUBLE>(0).build(false);
    }
    ColumnBuilder<TYPE_DOUBLE> result_builder(num_rows);
    ColumnViewer<TYPE_DOUBLE> viewer(columns[0]);
    for (size_t i = 0; i < num_rows; ++i) {
        if (viewer.is_null(i)) {
            result_builder.append_null();
            continue;
        }
        double percentile = viewer.value(i);
        double mean = 0;
        double stddev = 1;
        boost::math::normal_distribution<> dist(mean, stddev);
        try {
            double x = boost::math::quantile(dist, percentile);
            result_builder.append(x);
        } catch (const std::exception& e) {
            return Status::InvalidArgument(e.what());
        }
    }
    return result_builder.build(false);
}

StatusOr<ColumnPtr> ProbabilityDistributions::gauss_error_function(FunctionContext* context, const Columns& columns) {
    if (columns.size() != 1) {
        return Status::InvalidArgument("gauss_error_function() requires 1 argument, but got " +
                                       std::to_string(columns.size()));
    }
    size_t num_rows = columns[0]->size();
    if (num_rows == 0) {
        return ColumnBuilder<TYPE_DOUBLE>(0).build(false);
    }
    ColumnBuilder<TYPE_DOUBLE> result_builder(num_rows);
    ColumnViewer<TYPE_DOUBLE> viewer(columns[0]);
    for (size_t i = 0; i < num_rows; ++i) {
        if (viewer.is_null(i)) {
            result_builder.append_null();
            continue;
        }
        double z = viewer.value(i);
        try {
            double x = boost::math::erf(z);
            result_builder.append(x);
        } catch (const std::exception& e) {
            return Status::InvalidArgument(e.what());
        }
    }
    return result_builder.build(false);
}

} // namespace starrocks
