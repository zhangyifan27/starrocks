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

#include "percentile_functions.h"

namespace starrocks {

class ProbabilityDistributions {
public:
    /**
     * percentile point function
     *
     * @param: [pencentile]
     * @paramType: [DobuleColumn]
     * @return: DoubleColumn
     */
    DEFINE_VECTORIZED_FN(percent_point_function);

    /**
     * gauss error function
     *
     * @param: [z]
     * @paramType: [DobuleColumn]
     * @return: DoubleColumn
     */
    DEFINE_VECTORIZED_FN(gauss_error_function);
};

} // namespace starrocks
