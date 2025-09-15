#include <gtest/gtest.h>
#include "exec/pipeline/pipeline_builder.h"

namespace starrocks {
namespace pipeline {

class PrepareBucketToPartitionTest : public testing::TestWithParam<std::tuple<std::vector<uint32_t>, size_t, size_t>> {};

TEST_P(PrepareBucketToPartitionTest, test_bucket_allocation) {
    auto [bucket_list, bucket_size, dop] = GetParam();
    auto result = PipelineBuilderContext::prepare_bucket_to_partition(bucket_list, bucket_size, dop);

    // 统计每个partition分配的bucket数量
    std::vector<size_t> partition_counts(dop, 0);
    for (auto bucket : bucket_list) {
        partition_counts[result[bucket]]++;
    }

    // 检查分配是否均匀
    size_t min_count = *std::min_element(partition_counts.begin(), partition_counts.end());
    size_t max_count = *std::max_element(partition_counts.begin(), partition_counts.end());
    EXPECT_LE(max_count - min_count, 1) << "Bucket分配不均匀";
    
    // 检查所有bucket都被正确分配
    for (auto bucket : bucket_list) {
        EXPECT_LT(result[bucket], dop) << "Bucket " << bucket << "分配到了无效的partition";
    }
}

INSTANTIATE_TEST_SUITE_P(
    BucketAllocationTests,
    PrepareBucketToPartitionTest,
    testing::Values(
        // 基本测试用例
        std::make_tuple(std::vector<uint32_t>{0, 1, 2, 3}, 4, 2),
        std::make_tuple(std::vector<uint32_t>{0, 1, 2, 3, 4, 5}, 6, 3),
        // 边界测试用例
        std::make_tuple(std::vector<uint32_t>{}, 4, 2),
        std::make_tuple(std::vector<uint32_t>{0}, 1, 1),
        // 不均匀分配测试用例
        std::make_tuple(std::vector<uint32_t>{0, 1, 2, 3, 4}, 5, 2),
        std::make_tuple(std::vector<uint32_t>{0, 1, 2, 3, 4, 5, 6}, 7, 3),
        // 大dop测试用例
        std::make_tuple(std::vector<uint32_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 10, 4)
    )
);

} // namespace pipeline
} // namespace starrocks