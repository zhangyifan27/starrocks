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
#include <limits>
#include <type_traits>

#include "common/compiler_util.h"
#include "exprs/helpers/serialize_helpers.hpp"
#include "types/logical_type.h"

namespace starrocks {

/**
    Calculating univariate central moments, refer to https://en.wikipedia.org/wiki/Moment_(mathematics)
    Levels:
        level 1: mean
        level 2: variance 
        level 3: skewness. refer to https://en.wikipedia.org/wiki/Skewness
        level 4: kurtosis. refer to https://en.wikipedia.org/wiki/Kurtosis
*/
template <typename T, size_t level>
class MomentCalculator {
public:
    MomentCalculator() = default;

    MomentCalculator(const uint8_t*& buffer) { deserialize(buffer); }

    MomentCalculator(std::array<double, level> const& m, size_t count) : _n(count) {
        for (size_t i = 0; i < level; i++) _m[i] = m[i];
    }

    [[gnu::always_inline]] inline void update(T x) {
        _n++;
        update_m(x, x);
    }

    template <size_t cur_level = 1>
    [[gnu::always_inline]] inline void update_m(T x, T mul) {
        _m[cur_level - 1] += x;
        if constexpr (cur_level < level) {
            update_m<cur_level + 1>(x * mul, mul);
        }
    }

    template <size_t cur_level = 1>
    [[gnu::always_inline]] inline void merge(MomentCalculator const& other) {
        if constexpr (cur_level == 1) {
            _n += other._n;
        }
        _m[cur_level - 1] += other._m[cur_level - 1];
        if constexpr (cur_level < level) {
            merge<cur_level + 1>(other);
        }
    }

    template <size_t cur_level = 1>
    [[gnu::always_inline]] inline void serialize(uint8_t*& buffer) const {
        if constexpr (cur_level == 1) {
            SerializeHelpers::serialize(_n, buffer);
        }
        SerializeHelpers::serialize(_m[cur_level - 1], buffer);
        if constexpr (cur_level < level) {
            serialize<cur_level + 1>(buffer);
        }
    }

    template <size_t cur_level = 1>
    [[gnu::always_inline]] inline void deserialize(const uint8_t*& buffer) {
        if constexpr (cur_level == 1) {
            SerializeHelpers::deserialize(buffer, _n);
        }
        SerializeHelpers::deserialize(buffer, _m[cur_level - 1]);
        if constexpr (cur_level < level) {
            deserialize<cur_level + 1>(buffer);
        }
    }

    template <size_t cur_level = 1>
    [[gnu::always_inline]] inline size_t serialized_size() const {
        size_t size = 0;
        if constexpr (cur_level == 1) {
            size += SerializeHelpers::serialized_size(_n);
        }
        size += SerializeHelpers::serialized_size(_m[cur_level - 1]);
        if constexpr (cur_level < level) {
            size += serialized_size<cur_level + 1>();
        }
        return size;
    }

    template <size_t cur_level = 1>
    [[gnu::always_inline]] inline void reset() {
        if constexpr (cur_level == 1) {
            _n = 0;
        }
        _m[cur_level - 1] = 0.0;
        if constexpr (cur_level < level) {
            reset<cur_level + 1>();
        }
    }

    [[gnu::always_inline]] inline size_t n() const { return _n; }

    [[gnu::always_inline]] inline double mean() const { return m<1>() / _n; }

    template <bool is_sample>
    [[gnu::always_inline]] inline double variance() const {
        static_assert(level >= 2);
        if constexpr (is_sample) {
            if (UNLIKELY(_n == 0)) {
                return std::numeric_limits<double>::quiet_NaN();
            }
            if (UNLIKELY(_n == 1)) {
                return std::numeric_limits<double>::infinity();
            }
            return (m<2>() - m<1>() * m<1>() / _n) / (_n - 1);
        } else {
            if (UNLIKELY(_n == 0)) {
                return std::numeric_limits<double>::infinity();
            }
            return (m<2>() - m<1>() * m<1>() / _n) / _n;
        }
    }

    template <bool is_sample>
    [[gnu::always_inline]] inline double skewness() const {
        static_assert(level >= 3);
        if (UNLIKELY(_n == 0)) {
            return std::numeric_limits<double>::infinity();
        }
        double var = variance<is_sample>();
        if (UNLIKELY(!std::isfinite(var) || !(var > 0))) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        double moment3 = central_moment<3>();
        return moment3 / std::pow(var, 1.5);
    }

    template <bool is_sample>
    [[gnu::always_inline]] inline double kurtosis() const {
        static_assert(level >= 4);
        if (UNLIKELY(_n == 0)) {
            return std::numeric_limits<double>::infinity();
        }
        double var = variance<is_sample>();
        if (UNLIKELY(!std::isfinite(var) || !(var > 0))) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        double moment4 = central_moment<4>();
        return moment4 / (var * var);
    }

    template <size_t moment_level>
    [[gnu::always_inline]] double central_moment() const {
        static_assert(level >= moment_level);
        // central_moment M_k = E[(X - E[X])^k] = \sum_i((x_i - x_bar)^k / n)
        //                where x_bar = \sum_i(x_i / n)

        if (UNLIKELY(_n == 0)) {
            return std::numeric_limits<double>::infinity();
        }
        if constexpr (moment_level == 1) {
            // M_1 = \sum_i(x_i - x_bar) / n = 0
            return 0;
        } else if constexpr (moment_level == 2) {
            // M_2 = \sum_i((x_i - x_bar)^2) / n
            //     = (\sum_i(x_i^2 - 2 * x_i * x_bar + x_bar^2)) / n
            //     = (\sum_i(x_i^2) - 2 * \sum_i(x_i) * \sum_i(x_i) / n + n * (\sum_i(x_i) / n)^2) / n
            //     = (\sum_i(x_i^2) - \sum_i(x_i)^2 / n) / n
            return (m<2>()                  //
                    - m<1>() * m<1>() / _n) //
                   / _n;
        } else if constexpr (moment_level == 3) {
            // M_3 = \sum_i((x_i - x_bar)^3) / n
            //     = (\sum_i(x_i^3 - 3 * x_i^2 * x_bar + 3 * x_i * x_bar^2 - x_bar^3)) / n
            //     = (\sum_i(x_i^3) - 3 * \sum_i(x_i^2) * \sum_i(x_i) / n + 3 * \sum_i(x_i) * \sum_i(x_i) / n^2 - n * (\sum_i(x_i) / n)^3) / n
            //     = (\sum_i(x_i^3) - 3 * \sum_i(x_i^2) * \sum_i(x_i) / n + 2 * \sum_i(x_i)^3 / n^2) / n
            return (m<3>()                                      //
                    - 3 * m<2>() * m<1>() / _n                  //
                    + 2 * m<1>() * m<1>() * m<1>() / (_n * _n)) //
                   / _n;
        } else if constexpr (moment_level == 4) {
            // M_4 = \sum_i((x_i - x_bar)^4) / n
            //     = (\sum_i(x_i^4 - 4 * x_i^3 * x_bar + 6 * x_i^2 * x_bar^2 - 4 * x_i * x_bar^3 + x_bar^4)) / n
            //     = (\sum_i(x_i^4) - 4 * \sum_i(x_i^3) * \sum_i(x_i) / n + 6 * \sum_i(x_i^2) * \sum_i(x_i)^2 / n^2 - 4 * \sum_i(x_i) * \sum_i(x_i)^3 / n^3 + n * (\sum_i(x_i) / n)^4) / n
            //     = (\sum_i(x_i^4) - 4 * \sum_i(x_i^3) * \sum_i(x_i) / n + 6 * \sum_i(x_i^2) * \sum_i(x_i)^2 / n^2 - 3 * \sum_i(x_i)^4 / n^3) / n
            return (m<4>()                                                    //
                    - 4 * m<3>() * m<1>() / _n                                //
                    + 6 * m<2>() * m<1>() * m<1>() / (_n * _n)                //
                    - 3 * m<1>() * m<1>() * m<1>() * m<1>() / (_n * _n * _n)) //
                   / _n;
        } else {
            __builtin_unreachable();
        }
    }

    template <size_t i>
    [[gnu::always_inline]] T m() const {
        static_assert(i <= level);
        return _m[i - 1];
    }

private:
    std::array<T, level> _m{}; // \sum(x^i) of each level.
    size_t _n{0};              // number of samples.
};

} // namespace starrocks
