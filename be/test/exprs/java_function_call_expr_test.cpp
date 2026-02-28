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

#include "exprs/java_function_call_expr.h"

#include <gtest/gtest.h>

namespace starrocks {

TEST(JavaFunctionCallExprTest, ComputeUdfBatchSize) {
    EXPECT_EQ(0UL, compute_udf_batch_size(0, 0, 1024));
    EXPECT_EQ(4096UL, compute_udf_batch_size(4096 * 100, 4096, -1));

    size_t total_bytes = 4096 * 100;
    EXPECT_EQ(1024UL, compute_udf_batch_size(total_bytes, 4096, 100 * 1024));
    EXPECT_EQ(102UL, compute_udf_batch_size(total_bytes, 4096, 10 * 1024));
    EXPECT_EQ(1UL, compute_udf_batch_size(total_bytes, 4096, 50));
}

} // namespace starrocks
