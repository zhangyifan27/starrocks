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

#include <velocypack/Builder.h>

#include <boost/math/distributions.hpp>
#include <cctype>
#include <cmath>
#include <ios>
#include <iterator>
#include <limits>
#include <random>
#include <sstream>

#include "column/const_column.h"
#include "column/json_column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "delta_method.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/ttest_common.h"
#include "exprs/function_helper.h"
#include "exprs/helpers/math_helpers.hpp"
#include "exprs/helpers/serialize_helpers.hpp"
#include "gutil/casts.h"
#include "types/logical_type.h"
#include "util/json.h"
#include "util/orlp/pdqsort.h"

namespace starrocks {

class BucketQuantileContainer {
public:
    constexpr static int kNumSamples = 128;
    static constexpr uint32_t kNumBuckets = 128;
    static constexpr uint32_t kBucketDivisor = (1u << (32u - 7u));

    BucketQuantileContainer() = default;

    Status add(double x, std::string const& treatment, int64_t uin) {
        if (!_groups.count(treatment)) {
            if (_groups.size() >= 2) {
                return Status::InvalidArgument("only support 2 groups");
            }
            _groups[treatment].resize(kNumBuckets);
        }
        uint64_t bucket_id = _hash(uin) / kBucketDivisor;
        _groups[treatment][bucket_id].push_back(x);
        return Status::OK();
    }

    void serialize(uint8_t*& data) {
        sort_if_not_sorted();
        SerializeHelpers::serialize_all(data, _groups);
    }

    void deserialize(const uint8_t*& data) { SerializeHelpers::deserialize_all(data, _groups); }

    size_t serialized_size() const { return SerializeHelpers::serialized_size(_groups); }

    Status merge(BucketQuantileContainer& other) {
        sort_if_not_sorted();
        DCHECK(other.is_sorted());
        for (auto&& [treatment, buckets] : other._groups) {
            if (_groups.count(treatment) == 0) {
                if (_groups.size() >= 2) {
                    return Status::InvalidArgument("There are too many groups.");
                }
                _groups[treatment] = std::move(buckets);
                continue;
            }
            auto& this_buckets = _groups[treatment];
            for (uint32_t bucket_id = 0; bucket_id < kNumBuckets; ++bucket_id) {
                std::vector<double> result_bucket;
                std::merge(this_buckets[bucket_id].begin(), this_buckets[bucket_id].end(), buckets[bucket_id].begin(),
                           buckets[bucket_id].end(), std::back_inserter(result_bucket));
                this_buckets[bucket_id] = std::move(result_bucket);
            }
        }
        return Status::OK();
    }

    bool is_sorted() const {
        for (auto&& [treatment, buckets] : _groups) {
            for (uint32_t bucket_id = 0; bucket_id < kNumBuckets; ++bucket_id) {
                if (!std::is_sorted(buckets[bucket_id].begin(), buckets[bucket_id].end())) {
                    return false;
                }
            }
        }
        return true;
    }

    void sort_if_not_sorted() {
        for (auto&& [treatment, buckets] : _groups) {
            for (uint32_t bucket_id = 0; bucket_id < kNumBuckets; ++bucket_id) {
                if (std::is_sorted(buckets[bucket_id].begin(), buckets[bucket_id].end())) {
                    continue;
                }
                pdqsort(buckets[bucket_id].begin(), buckets[bucket_id].end());
            }
        }
    }

    Status finalize() {
        sort_if_not_sorted();
        if (_groups.size() < 2) {
            return Status::InvalidArgument("There is not enough groups.");
        }
        if (_groups.size() > 2) {
            return Status::InvalidArgument("logical error: There are too many groups.");
        }
        for (auto&& p : _groups) {
            auto const& treatment = p.first;
            auto const& buckets = p.second;
            uint64_t index = 0;
            for (uint32_t bucket_id = 0; bucket_id < kNumBuckets; ++bucket_id) {
                auto& bucket = buckets[bucket_id];
                if (bucket.empty()) {
                    return Status::InvalidArgument("Some of the buckets are empty.");
                }
                _start_index[treatment].emplace_back(index);
                index += bucket.size();
            }
            DCHECK(_start_index[treatment].size() == kNumBuckets);
            _sorted_index[treatment].resize(index);
            std::iota(_sorted_index[treatment].begin(), _sorted_index[treatment].end(), 0);
            pdqsort(_sorted_index[treatment].begin(), _sorted_index[treatment].end(),
                    [&](uint64_t lhs, uint64_t rhs) { return get_data(treatment, lhs) < get_data(treatment, rhs); });
            _count[treatment] = index;
        }
        return Status::OK();
    }

    std::vector<double> get_quantiles(std::string const& treatment, std::vector<double> const& percentiles) {
        std::vector<double> result;
        uint64_t total_count = _count[treatment];
        DCHECK(total_count > 0);
        for (auto&& percentile : percentiles) {
            result.emplace_back(get_quantile(percentile, total_count, [&](uint64_t n) {
                return get_data(treatment, _sorted_index[treatment][n]);
            }));
        }
        return result;
    }

    std::vector<double> get_bucket_quantiles(std::string const& treatment, uint32_t bucket_id,
                                             std::vector<double> const& percentiles) {
        std::vector<double> result;
        auto& bucket = _groups[treatment][bucket_id];
        uint64_t total_count = bucket.size();
        DCHECK(total_count > 0);
        for (auto&& percentile : percentiles) {
            result.emplace_back(get_quantile(percentile, total_count, [&](uint64_t n) { return bucket[n]; }));
        }
        return result;
    }

    double get_data(std::string const& treatment, uint64_t pos) {
        auto it = std::upper_bound(_start_index[treatment].begin(), _start_index[treatment].end(), pos);
        DCHECK(it != _start_index[treatment].begin());
        --it;
        uint64_t bucket_index = std::distance(_start_index[treatment].begin(), it);
        uint64_t start = *it;
        uint64_t index = pos - start;
        DCHECK(bucket_index < kNumBuckets);
        return _groups[treatment][bucket_index][index];
    }

    double get_count(std::string const& treatment) { return _count[treatment]; }

    std::array<std::string, 2> get_treatments() {
        DCHECK(_groups.size() == 2);
        std::array<std::string, 2> result;
        auto it = _groups.begin();
        result[0] = it->first;
        ++it;
        result[1] = it->first;
        return result;
    }

private:
    template <typename F>
    double get_quantile(double percentile, uint64_t size, F&& f) {
        DCHECK(std::isfinite(percentile) && percentile > 0 && percentile < 1);
        double h = percentile * (size + 1);
        auto n = static_cast<uint64_t>(h);
        if (n >= size) {
            return f(size - 1);
        }
        if (n < 1) {
            return f(0);
        }
        auto now = f(n - 1);
        auto next = f(n);

        // R6 linear interpolation
        return now + (h - n) * (next - now);
    }

    MathHelpers::MurmurHash3 _hash{0};

    // treatment -> bucket -> values
    std::map<std::string, std::vector<std::vector<double>>> _groups;

    std::map<std::string, std::vector<uint64_t>> _start_index; // only works when querying quantiles

    std::map<std::string, std::vector<uint64_t>> _sorted_index; // only works when querying quantiles

    std::map<std::string, uint64_t> _count; // only works when querying quantiles
};

class QuantileTtestCalculator {
public:
    QuantileTtestCalculator(double ctrl_quant, double treat_quant, double ctrl_se, double treat_se, double ctrl_cnt,
                            double treat_cnt, double alpha, double power, double mde)
            : _ctrl_quant(ctrl_quant),
              _treat_quant(treat_quant),
              _ctrl_se(ctrl_se),
              _treat_se(treat_se),
              _ctrl_cnt(ctrl_cnt),
              _treat_cnt(treat_cnt),
              _alpha(alpha),
              _power(power),
              _mde(mde) {}

    double abs_diff() const { return std::abs(_ctrl_quant - _treat_quant); }

    double rela_diff() const {
        if (std::abs(_ctrl_quant) > 1e-6) {
            return _treat_quant / _ctrl_quant - 1;
        }
        if (std::abs(_treat_quant) < 1e-6) {
            return 0;
        }
        return std::numeric_limits<double>::infinity();
    }

    double rela_diff_se() const {
        auto rela_diff_ = rela_diff();
        if (!std::isfinite(rela_diff_)) {
            return std::numeric_limits<double>::infinity();
        }
        return std::sqrt(_ctrl_se * _ctrl_se + _treat_se * _treat_se) * (rela_diff_ + 1);
    }

    double pvalue() const {
        auto rela_diff_ = rela_diff();
        if (std::abs(rela_diff_) < 1e-6) {
            return 1;
        }
        auto rela_diff_se_ = rela_diff_se();
        if (std::abs(rela_diff_se_) < 1e-6 || std::isinf(rela_diff_se_) || std::isinf(rela_diff_)) {
            return 0;
        }
        double t_stat = rela_diff_ / rela_diff_se_;
        return (1 - _cdf(std::abs(t_stat))) * 2;
    }

    double rela_diff_confidence_interval_lower() const {
        auto rela_diff_ = rela_diff();
        if (!std::isfinite(rela_diff_)) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        auto rela_diff_se_ = rela_diff_se();
        if (!std::isfinite(rela_diff_se_)) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        return rela_diff_ - _ppf(1 - _alpha / 2) * rela_diff_se_;
    }

    double rela_diff_confidence_interval_upper() const {
        auto rela_diff_ = rela_diff();
        if (!std::isfinite(rela_diff_)) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        auto rela_diff_se_ = rela_diff_se();
        if (!std::isfinite(rela_diff_se_)) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        return rela_diff_ + _ppf(1 - _alpha / 2) * rela_diff_se_;
    }

    double abs_diff_confidence_interval_lower() const {
        auto abs_diff_ = abs_diff();
        if (!std::isfinite(abs_diff_)) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        auto rela_diff_se_ = rela_diff_se();
        if (!std::isfinite(rela_diff_se_)) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        return abs_diff_ - _ppf(1 - _alpha / 2) * rela_diff_se_ * _ctrl_quant;
    }

    double abs_diff_confidence_interval_upper() const {
        auto abs_diff_ = abs_diff();
        if (!std::isfinite(abs_diff_)) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        auto rela_diff_se_ = rela_diff_se();
        if (!std::isfinite(rela_diff_se_)) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        return abs_diff_ + _ppf(1 - _alpha / 2) * rela_diff_se_ * _ctrl_quant;
    }

    double test_power() const {
        auto rela_diff_se_ = rela_diff_se();
        if (std::abs(rela_diff_se_) < 1e-6) {
            return 1;
        }
        if (!std::isfinite(rela_diff_se_)) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        double temp = _mde / rela_diff_se_;
        double cdf1 = _cdf(_ppf(1 - _alpha / 2) - temp);
        double cdf2 = _cdf(_ppf(_alpha / 2) - temp);
        return 1 - cdf1 + cdf2;
    }

    int64_t recom_sample_size() const {
        double z_half_alpha_right = _ppf(1 - _alpha / 2);
        double beta_point = _ppf(1 - _power);
        double rela_diff_se_ = rela_diff_se();
        if (!std::isfinite(rela_diff_se_)) {
            return -1;
        }
        double size = rela_diff_se_ * rela_diff_se_ * _treat_cnt * (z_half_alpha_right - beta_point) *
                      (z_half_alpha_right - beta_point) / (_mde * _mde);
        return static_cast<int64_t>(std::ceil(size));
    }

    double std_samp() const { return _treat_se * std::sqrt(_treat_cnt); }
    double ctrl_quant() const { return _ctrl_quant; }
    double treat_quant() const { return _treat_quant; }
    double ctrl_cnt() const { return _ctrl_cnt; }
    double treat_cnt() const { return _treat_cnt; }

private:
    static double _cdf(double p) {
        boost::math::normal normal_dist(0, 1);
        return cdf(normal_dist, p);
    }

    static double _ppf(double v) {
        boost::math::normal normal_dist(0, 1);
        return quantile(normal_dist, v);
    }

    double _ctrl_quant;
    double _treat_quant;
    double _ctrl_se;
    double _treat_se;
    [[maybe_unused]] double _ctrl_cnt;
    double _treat_cnt;
    double _alpha;
    double _power;
    double _mde;
};

class QuantileTestAggregateState {
public:
    QuantileTestAggregateState() = default;

    QuantileTestAggregateState(const uint8_t*& data) { deserialize(data); }

    Status init(std::vector<double> const& percentiles, int32_t bootstrap_times, double alpha, double power,
                double mde) {
        if (percentiles.empty()) {
            return Status::InvalidArgument("percentiles should not be empty.");
        }
        for (auto percentile : percentiles) {
            if (!std::isfinite(percentile)) {
                return Status::InvalidArgument(
                        fmt::format("`percentiles` should be a finite number in (0, 1), but get `{}`.", percentile));
            }
            if (percentile <= 0 || percentile >= 1) {
                return Status::InvalidArgument(
                        fmt::format("`percentiles` should be in (0, 1), but get `{}`.", percentile).c_str());
            }
        }
        if (bootstrap_times <= 1) {
            return Status::InvalidArgument("bootstrap_times should be no less than 2.");
        }
        if (bootstrap_times >= 100000) {
            return Status::InvalidArgument("bootstrap_times should be no greater than 100'000.");
        }
        if (!std::isfinite(alpha)) {
            return Status::InvalidArgument("`alpha` should be a finite number.");
        }
        if (alpha <= 0 || alpha >= 1) {
            return Status::InvalidArgument("alpha should be in (0, 1).");
        }
        if (!std::isfinite(power)) {
            return Status::InvalidArgument("`power` should be a finite number.");
        }
        if (power <= 0 || power >= 1) {
            return Status::InvalidArgument("power should be in (0, 1).");
        }
        if (!std::isfinite(mde)) {
            return Status::InvalidArgument("`mde` should be a finite number.");
        }
        if (mde <= 0) {
            return Status::InvalidArgument("mde should be positive.");
        }
        this->percentiles = percentiles;
        this->bootstrap_times = bootstrap_times;
        this->alpha = alpha;
        this->power = power;
        this->mde = mde;
        return Status::OK();
    }

    Status update(double x, std::string const& treatment, int64_t uin) { return _container.add(x, treatment, uin); }

    Status merge(QuantileTestAggregateState& other) {
        if (percentiles != other.percentiles) {
            return Status::InvalidArgument("Logical Error: percentiles should be the same.");
        }
        if (bootstrap_times != other.bootstrap_times) {
            return Status::InvalidArgument("Logical Error: bootstrap_times should be the same.");
        }
        if (alpha != other.alpha) {
            return Status::InvalidArgument("Logical Error: alpha should be the same.");
        }
        if (power != other.power) {
            return Status::InvalidArgument("Logical Error: power should be the same.");
        }
        if (mde != other.mde) {
            return Status::InvalidArgument("Logical Error: mde should be the same.");
        }
        return _container.merge(other._container);
    }

    Status finalize() { return _container.finalize(); }

    std::vector<double> get_quantiles(std::string const& treatment, std::vector<double> const& percentiles) {
        return _container.get_quantiles(treatment, percentiles);
    }

    std::vector<double> get_bucket_quantiles(std::string const& treatment, uint32_t bucket_id,
                                             std::vector<double> const& percentiles) {
        return _container.get_bucket_quantiles(treatment, bucket_id, percentiles);
    }

    void serialize(uint8_t*& data) {
        _container.serialize(data);
        SerializeHelpers::serialize_all(data, percentiles, bootstrap_times, alpha, power, mde);
    }

    void deserialize(const uint8_t*& data) {
        _container.deserialize(data);
        SerializeHelpers::deserialize_all(data, percentiles, bootstrap_times, alpha, power, mde);
    }

    size_t serialized_size() const {
        return _container.serialized_size() +
               SerializeHelpers::serialized_size_all(percentiles, bootstrap_times, alpha, power, mde);
    }

    bool is_uninitialized() const { return percentiles.empty(); }

    void build_result(vpack::Builder& builder) {
        vpack::ObjectBuilder obj_builder(&builder);
        builder.add("causal-function", to_json(AllInSqlFunctions::quantile_test));
        JsonSchemaFormatter schema;
        schema.add_field("causal-function", "string");
        auto st = _container.finalize();
        if (!st.ok()) {
            builder.add("error", to_json(st.to_string()));
            schema.add_field("error", "string");
            builder.add("schema", to_json(schema.print()));
            return;
        }
        auto treatments = _container.get_treatments();
        std::array<std::vector<double>, 2> quantiles;
        for (int i = 0; i < 2; ++i) {
            quantiles[i] = get_quantiles(treatments[i], percentiles);
        }
        std::array<std::vector<std::vector<double>>, 2> bucket_quantiles;
        for (int i = 0; i < 2; ++i) {
            bucket_quantiles[i].resize(BucketQuantileContainer::kNumBuckets);
            for (uint32_t bucket_id = 0; bucket_id < BucketQuantileContainer::kNumBuckets; ++bucket_id) {
                bucket_quantiles[i][bucket_id] = get_bucket_quantiles(treatments[i], bucket_id, percentiles);
            }
        }
        std::array<std::vector<double>, 2> quants_stds;
        std::mt19937 gen(0);
        for (int i = 0; i < 2; ++i) {
            for (int j = 0; j < percentiles.size(); ++j) {
                auto&& bucket = bucket_quantiles[i];
                std::vector<double> quants(BucketQuantileContainer::kNumBuckets);
                for (uint32_t bucket_id = 0; bucket_id < BucketQuantileContainer::kNumBuckets; ++bucket_id) {
                    quants[bucket_id] = bucket[bucket_id][j];
                }
                std::uniform_int_distribution<uint32_t> dist(0, BucketQuantileContainer::kNumBuckets - 1);
                double sum_x = 0, sum_xx = 0;
                for (int k = 0; k < bootstrap_times; ++k) {
                    double avg = 0;
                    for (int t = 0; t < BucketQuantileContainer::kNumSamples; ++t) {
                        avg += quants[dist(gen)];
                    }
                    avg /= BucketQuantileContainer::kNumSamples;
                    sum_x += avg;
                    sum_xx += avg * avg;
                }
                // calculate the standard error of avgs
                // numpy.std(..., ddof=1)
                double std = std::sqrt((sum_xx - sum_x * sum_x / bootstrap_times) / (bootstrap_times - 1));
                DCHECK(std::isfinite(std));
                quants_stds[i].emplace_back(std);
            }
        }
        schema.add_field("metrics", "array");
        schema.add_array_field("metrics", "percentile", "double");
        schema.add_array_field("metrics", "treatment", "string");
        schema.add_array_field("metrics", "quantile", "double");
        schema.add_array_field("metrics", "std_samp", "double");
        schema.add_array_field("metrics", "p-value", "double");
        schema.add_array_field("metrics", "abs_diff", "double");
        schema.add_array_field("metrics", "abs_diff_confidence_interval", "array");
        schema.add_array_field("metrics", "rela_diff", "double");
        schema.add_array_field("metrics", "rela_diff_confidence_interval", "array");
        schema.add_array_field("metrics", "test_power", "double");
        schema.add_array_field("metrics", "recom_sample_size", "int");
        {
            vpack::ArrayBuilder array_builder(&builder, "metrics");
            for (int i = 0; i < percentiles.size(); ++i) {
                auto percentile = percentiles[i];
                auto ctrl = treatments[0];
                auto quantile_x = quantiles[0][i];
                auto quants_std_x = quants_stds[0][i];
                auto count_x = _container.get_count(treatments[0]);
                for (int treat = 0; treat < 2; ++treat) {
                    auto treatment = treatments[treat];
                    auto quantile_y = quantiles[treat][i];
                    auto quants_std_y = quants_stds[treat][i];
                    auto count_y = _container.get_count(treatments[treat]);
                    QuantileTtestCalculator calculator(quantile_x, quantile_y, quants_std_x, quants_std_y, count_x,
                                                       count_y, alpha, power, mde);
                    vpack::ObjectBuilder control_obj_builder(&builder);
                    builder.add("percentile", to_json(percentile));
                    builder.add("treatment", to_json(treatment));
                    builder.add("quantile", to_json(calculator.treat_quant()));
                    builder.add("std_samp", to_json(calculator.std_samp()));
                    // keep these fileds empty for control group
                    if (treat == 0) {
                        builder.add("p-value", to_json(""));
                        builder.add("abs_diff", to_json(""));
                        builder.add("abs_diff_confidence_interval", to_json(""));
                        builder.add("rela_diff", to_json(""));
                        builder.add("rela_diff_confidence_interval", to_json(""));
                        builder.add("test_power", to_json(""));
                        builder.add("recom_sample_size", to_json(""));
                    } else {
                        builder.add("p-value", to_json(calculator.pvalue()));
                        builder.add("abs_diff", to_json(calculator.abs_diff()));
                        {
                            vpack::ArrayBuilder abs_diff_ci_builder(&builder, "abs_diff_confidence_interval");
                            builder.add(to_json(calculator.abs_diff_confidence_interval_lower()));
                            builder.add(to_json(calculator.abs_diff_confidence_interval_upper()));
                        }
                        builder.add("rela_diff", to_json(calculator.rela_diff()));
                        {
                            vpack::ArrayBuilder rela_diff_ci_builder(&builder, "rela_diff_confidence_interval");
                            builder.add(to_json(calculator.rela_diff_confidence_interval_lower()));
                            builder.add(to_json(calculator.rela_diff_confidence_interval_upper()));
                        }
                        builder.add("test_power", to_json(calculator.test_power()));
                        builder.add("recom_sample_size", to_json(calculator.recom_sample_size()));
                    }
                }
            }
        }
        builder.add("schema", to_json(schema.print()));
    }

private:
    BucketQuantileContainer _container;

    std::vector<double> percentiles;
    int32_t bootstrap_times = 1000;
    double alpha = 0.05;
    double power = 0.8;
    double mde = 0.01;
};

class QuantileTestAggregateFunction
        : public AggregateFunctionBatchHelper<QuantileTestAggregateState, QuantileTestAggregateFunction> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override { DCHECK(false); }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        //Y, treatment, percentiles, uin[, num_bootstrap=500[, alpha=0.05[, power=0.8[, mde=0.01]]]]
        if (this->data(state).is_uninitialized()) {
            std::vector<double> percentiles;
            const Column* percentiles_col = columns[2];
            auto percentile_datums = FunctionHelper::get_data_of_array(percentiles_col, row_num);
            if (!percentile_datums.has_value()) {
                ctx->set_error("Internal Error: fail to get `percentiles`.");
                return;
            }
            for (auto&& datum : percentile_datums.value()) {
                if (datum.is_null()) {
                    ctx->set_error("Error: `percentiles` should not contain null.");
                    return;
                }
                auto percentile = datum.get_double();
                percentiles.emplace_back(percentile);
            }
            int64_t bootstrap_times = 500;
            if (ctx->get_num_args() >= 5) {
                const Column* bootstrap_times_col = columns[4];
                if (!FunctionHelper::get_data_of_column<RunTimeColumnType<TYPE_BIGINT>>(bootstrap_times_col, row_num,
                                                                                        bootstrap_times)) {
                    ctx->set_error("Internal Error: fail to get `bootstrap_times`.");
                    return;
                }
            }
            double alpha = 0.05;
            if (ctx->get_num_args() >= 6) {
                const Column* alpha_col = columns[5];
                if (!FunctionHelper::get_data_of_column<RunTimeColumnType<TYPE_DOUBLE>>(alpha_col, row_num, alpha)) {
                    ctx->set_error("Internal Error: fail to get `alpha`.");
                    return;
                }
            }
            double power = 0.8;
            if (ctx->get_num_args() >= 7) {
                const Column* power_col = columns[6];
                if (!FunctionHelper::get_data_of_column<RunTimeColumnType<TYPE_DOUBLE>>(power_col, row_num, power)) {
                    ctx->set_error("Internal Error: fail to get `power`.");
                    return;
                }
            }
            double mde = 0.01;
            if (ctx->get_num_args() >= 8) {
                const Column* mde_col = columns[7];
                if (!FunctionHelper::get_data_of_column<RunTimeColumnType<TYPE_DOUBLE>>(mde_col, row_num, mde)) {
                    ctx->set_error("Internal Error: fail to get `mde`.");
                    return;
                }
            }
            auto st = this->data(state).init(percentiles, bootstrap_times, alpha, power, mde);
            if (!st.ok()) {
                ctx->set_error(st.to_string().c_str());
                return;
            }
        }
        double x;
        const Column* x_col = columns[0];
        if (!FunctionHelper::get_data_of_column<RunTimeColumnType<TYPE_DOUBLE>>(x_col, row_num, x)) {
            ctx->set_error("Internal Error: fail to get `x`.");
            return;
        }
        if (std::isnan(x) || std::isinf(x)) {
            return;
        }
        std::string treatment;
        const Column* treatment_col = columns[1];
        if (!FunctionHelper::get_data_of_column<RunTimeColumnType<TYPE_VARCHAR>>(treatment_col, row_num, treatment)) {
            ctx->set_error("Internal Error: fail to get `treatment`.");
            return;
        }
        int64_t uin;
        const Column* uin_col = columns[3];
        if (!FunctionHelper::get_data_of_column<RunTimeColumnType<TYPE_BIGINT>>(uin_col, row_num, uin)) {
            ctx->set_error("Internal Error: fail to get `uin`.");
            return;
        }
        auto st = this->data(state).update(x, treatment, uin);
        if (!st.ok()) {
            ctx->set_error(st.to_string().c_str());
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        column = FunctionHelper::unwrap_if_nullable<const Column*>(column, row_num);
        if (column == nullptr) {
            ctx->set_error("Internal Error: fail to get intermediate data.");
            return;
        }
        DCHECK(column->is_binary());
        const uint8_t* serialized_data = reinterpret_cast<const uint8_t*>(column->get(row_num).get_slice().data);
        if (this->data(state).is_uninitialized()) {
            this->data(state).deserialize(serialized_data);
            return;
        }
        QuantileTestAggregateState other(serialized_data);
        auto st = this->data(state).merge(other);
        if (!st.ok()) {
            ctx->set_error(st.to_string().c_str());
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (to->is_nullable()) {
            auto* dst_nullable_col = down_cast<NullableColumn*>(to);
            dst_nullable_col->null_column_data().emplace_back(false);
            to = dst_nullable_col->data_column().get();
        }
        auto* column = down_cast<BinaryColumn*>(to);
        Bytes& bytes = column->get_bytes();
        size_t old_size = bytes.size();
        size_t new_size = old_size + this->data(state).serialized_size();
        bytes.resize(new_size);
        column->get_offset().emplace_back(new_size);
        uint8_t* serialized_data = bytes.data() + old_size;
        const_cast<QuantileTestAggregateState&>(this->data(state)).serialize(serialized_data);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        if (to->is_nullable()) {
            auto* dst_nullable_col = down_cast<NullableColumn*>(to);
            if (this->data(state).is_uninitialized()) {
                ctx->set_error("Internal Error: state not initialized.");
                return;
            }
            dst_nullable_col->null_column_data().emplace_back(false);
            to = dst_nullable_col->data_column().get();
        }
        if (this->data(state).is_uninitialized()) {
            ctx->set_error("Internal Error: state not initialized.");
            return;
        }
        DCHECK(to->is_json());
        vpack::Builder result_builder;
        const_cast<QuantileTestAggregateState&>(this->data(state)).build_result(result_builder);
        auto slice = result_builder.slice();
        JsonValue result_json(slice);
        down_cast<JsonColumn*>(to)->append(std::move(result_json));
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {}

    std::string get_name() const override { return std::string(AllInSqlFunctions::quantile_test); }
};

} // namespace starrocks
