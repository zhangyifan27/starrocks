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

#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/any_value.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "exprs/agg/group_concat.h"
#include "exprs/agg/group_array.h"
#include "exprs/agg/percentile_cont.h"
#include "types/logical_type.h"
#include "util/percentile_value.h"

namespace starrocks {

struct PercentileDiscDispatcher {
    template <LogicalType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_datetime<pt> || lt_is_date<pt> || lt_is_arithmetic<pt> || lt_is_string<pt> ||
                      lt_is_decimal_of_any_version<pt>) {
            resolver->add_aggregate_mapping_variadic<pt, pt, PercentileState<pt>>(
                    "percentile_disc", false, AggregateFactory::MakePercentileDiscAggregateFunction<pt>());
        }
    }
};

void AggregateFuncResolver::register_others() {
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_DOUBLE>("percentile_approx", false,
                                                            AggregateFactory::MakePercentileApproxAggregateFunction());
    add_aggregate_mapping_notnull<TYPE_DOUBLE, TYPE_DOUBLE>("percentile_approx", false,
                                                            AggregateFactory::MakePercentileApproxAggregateFunction());
    add_aggregate_mapping<TYPE_PERCENTILE, TYPE_PERCENTILE, PercentileValue>(
            "percentile_union", false, AggregateFactory::MakePercentileUnionAggregateFunction());

    add_aggregate_mapping_variadic<TYPE_DOUBLE, TYPE_DOUBLE, PercentileState<TYPE_DOUBLE>>(
            "percentile_cont", false, AggregateFactory::MakePercentileContAggregateFunction<TYPE_DOUBLE>());
    add_aggregate_mapping_variadic<TYPE_DATETIME, TYPE_DATETIME, PercentileState<TYPE_DATETIME>>(
            "percentile_cont", false, AggregateFactory::MakePercentileContAggregateFunction<TYPE_DATETIME>());
    add_aggregate_mapping_variadic<TYPE_DATE, TYPE_DATE, PercentileState<TYPE_DATE>>(
            "percentile_cont", false, AggregateFactory::MakePercentileContAggregateFunction<TYPE_DATE>());

    for (auto type : sortable_types()) {
        type_dispatch_all(type, PercentileDiscDispatcher(), this);
    }

    add_aggregate_mapping_variadic<TYPE_CHAR, TYPE_VARCHAR, GroupConcatAggregateState>(
            "group_concat", false, AggregateFactory::MakeGroupConcatAggregateFunction<TYPE_CHAR>());
    add_aggregate_mapping_variadic<TYPE_VARCHAR, TYPE_VARCHAR, GroupConcatAggregateState>(
            "group_concat", false, AggregateFactory::MakeGroupConcatAggregateFunction<TYPE_VARCHAR>());

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_ARRAY, GroupArrayAggregateState<TYPE_BOOLEAN>, AggregateFunctionPtr, false>(
        "group_array", false, AggregateFactory::MakeGroupArrayAggregateFunction<TYPE_BOOLEAN>());
    add_aggregate_mapping<TYPE_TINYINT, TYPE_ARRAY, GroupArrayAggregateState<TYPE_TINYINT>, AggregateFunctionPtr, false>(
        "group_array", false, AggregateFactory::MakeGroupArrayAggregateFunction<TYPE_TINYINT>());
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_ARRAY, GroupArrayAggregateState<TYPE_SMALLINT>, AggregateFunctionPtr, false>(
        "group_array", false, AggregateFactory::MakeGroupArrayAggregateFunction<TYPE_SMALLINT>());
    add_aggregate_mapping<TYPE_INT, TYPE_ARRAY, GroupArrayAggregateState<TYPE_INT>, AggregateFunctionPtr, false>(
        "group_array", false, AggregateFactory::MakeGroupArrayAggregateFunction<TYPE_INT>());
    add_aggregate_mapping<TYPE_BIGINT, TYPE_ARRAY, GroupArrayAggregateState<TYPE_BIGINT>, AggregateFunctionPtr, false>(
        "group_array", false, AggregateFactory::MakeGroupArrayAggregateFunction<TYPE_BIGINT>());
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_ARRAY, GroupArrayAggregateState<TYPE_LARGEINT>, AggregateFunctionPtr, false>(
        "group_array", false, AggregateFactory::MakeGroupArrayAggregateFunction<TYPE_LARGEINT>());
    add_aggregate_mapping<TYPE_FLOAT, TYPE_ARRAY, GroupArrayAggregateState<TYPE_FLOAT>, AggregateFunctionPtr, false>(
        "group_array", false, AggregateFactory::MakeGroupArrayAggregateFunction<TYPE_FLOAT>());
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_ARRAY, GroupArrayAggregateState<TYPE_DOUBLE>, AggregateFunctionPtr, false>(
        "group_array", false, AggregateFactory::MakeGroupArrayAggregateFunction<TYPE_DOUBLE>());
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_ARRAY, GroupArrayAggregateState<TYPE_VARCHAR>, AggregateFunctionPtr, false>(
        "group_array", false, AggregateFactory::MakeGroupArrayAggregateFunction<TYPE_VARCHAR>());
    add_aggregate_mapping<TYPE_CHAR, TYPE_ARRAY, GroupArrayAggregateState<TYPE_CHAR>, AggregateFunctionPtr, false>(
        "group_array", false, AggregateFactory::MakeGroupArrayAggregateFunction<TYPE_CHAR>());
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_ARRAY, GroupArrayAggregateState<TYPE_DECIMAL32>, AggregateFunctionPtr, false>(
        "group_array", false, AggregateFactory::MakeGroupArrayAggregateFunction<TYPE_DECIMAL32>());
    add_aggregate_mapping<TYPE_DATETIME, TYPE_ARRAY, GroupArrayAggregateState<TYPE_DATETIME>, AggregateFunctionPtr, false>(
        "group_array", false, AggregateFactory::MakeGroupArrayAggregateFunction<TYPE_DATETIME>());
    add_aggregate_mapping<TYPE_DATE, TYPE_ARRAY, GroupArrayAggregateState<TYPE_DATE>, AggregateFunctionPtr, false>(
        "group_array", false, AggregateFactory::MakeGroupArrayAggregateFunction<TYPE_DATE>());

    add_array_mapping<TYPE_ARRAY, TYPE_VARCHAR>("dict_merge");
    add_array_mapping<TYPE_ARRAY, TYPE_ARRAY>("retention");

    // sum, avg, distinct_sum use decimal128 as intermediate or result type to avoid overflow
    add_decimal_mapping<TYPE_DECIMAL32, TYPE_DECIMAL128>("decimal_multi_distinct_sum");
    add_decimal_mapping<TYPE_DECIMAL64, TYPE_DECIMAL128>("decimal_multi_distinct_sum");
    add_decimal_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128>("decimal_multi_distinct_sum");

    // This first type is the 4th type input of windowfunnel.
    // And the 1st type is BigInt, 2nd is datetime, 3rd is mode(default 0).
    add_array_mapping<TYPE_INT, TYPE_INT>("window_funnel");
    add_array_mapping<TYPE_BIGINT, TYPE_INT>("window_funnel");
    add_array_mapping<TYPE_DATETIME, TYPE_INT>("window_funnel");
    add_array_mapping<TYPE_DATE, TYPE_INT>("window_funnel");

    add_general_mapping<AnyValueSemiState>("any_value", false, AggregateFactory::MakeAnyValueSemiAggregateFunction());
    add_general_mapping_notnull("array_agg2", false, AggregateFactory::MakeArrayAggAggregateFunctionV2());
    add_general_mapping_notnull("group_concat2", false, AggregateFactory::MakeGroupConcatAggregateFunctionV2());
}

} // namespace starrocks
