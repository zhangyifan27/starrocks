#include "gtest/gtest.h"
#include "exec/pipeline/exchange/shuffler.h"

namespace starrocks::pipeline {

class ShufflerTest : public ::testing::Test {
protected:
    void SetUp() override {
    }

    void TearDown() override {
    }
};

TEST_F(ShufflerTest, test_bucket_shuffler) {
    // 准备测试数据
    std::vector<uint32_t> bucket_to_partition = {1, 3, 2, 0}; // bucket_id到partition_id的映射
    BucketShufflerPartitioner shuffler(bucket_to_partition);

    // 测试数据
    std::vector<uint32_t> hash_values = {0, 1, 2, 3, 4, 5, 6, 7}; // 模拟hash值
    std::vector<uint32_t> channel_ids(hash_values.size(), 0);
    size_t num_rows = hash_values.size();

    // 执行exchange_shuffle
    shuffler.exchange_shuffle(channel_ids, hash_values, num_rows);

    // 验证结果
    // bucket_id = hash_value % bucket_to_partition.size()
    // channel_id = bucket_to_partition[bucket_id]
    EXPECT_EQ(channel_ids[0], 1); // 0 % 4 = 0 → bucket_to_partition[0] = 1
    EXPECT_EQ(channel_ids[1], 3); // 1 % 4 = 1 → bucket_to_partition[1] = 3
    EXPECT_EQ(channel_ids[2], 2); // 2 % 4 = 2 → bucket_to_partition[2] = 2
    EXPECT_EQ(channel_ids[3], 0); // 3 % 4 = 3 → bucket_to_partition[3] = 0
    EXPECT_EQ(channel_ids[4], 1); // 4 % 4 = 0 → bucket_to_partition[0] = 1
    EXPECT_EQ(channel_ids[5], 3); // 5 % 4 = 1 → bucket_to_partition[1] = 3
    EXPECT_EQ(channel_ids[6], 2); // 6 % 4 = 2 → bucket_to_partition[2] = 2
    EXPECT_EQ(channel_ids[7], 0); // 7 % 4 = 3 → bucket_to_partition[3] = 0

    // 测试local_exchange_shuffle，应该和exchange_shuffle结果相同
    std::vector<uint32_t> local_channel_ids(hash_values.size(), 0);
    shuffler.local_exchange_shuffle(local_channel_ids, hash_values, num_rows);
    EXPECT_EQ(channel_ids, local_channel_ids);
}

TEST_F(ShufflerTest, test_bucket_shuffler_with_different_sizes) {
    // 测试bucket_to_partition.size()与channel size不一致的情况
    std::vector<uint32_t> bucket_to_partition = {1, 3, 2}; // 只有3个bucket
    auto shuffler = ExchangeShufflerFactory::create_partitioner(
        false,  // compatibility
        false,  // is_two_level_shuffle
        TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED,  // partition_type
        4,  // num_channels (与bucket_to_partition.size()不同)
        1,  // num_shuffles_per_channel
        bucket_to_partition  // bucket_to_partition
    );
    ASSERT_NE(shuffler, nullptr);

    // 测试数据
    std::vector<uint32_t> hash_values = {0, 1, 2, 3, 4, 5, 6, 7}; // 模拟hash值
    std::vector<uint32_t> channel_ids(hash_values.size(), 0);
    size_t num_rows = hash_values.size();

    // 执行exchange_shuffle
    shuffler->exchange_shuffle(channel_ids, hash_values, num_rows);

    // 验证结果
    // bucket_id = hash_value % bucket_to_partition.size() (3)
    // channel_id = bucket_to_partition[bucket_id]
    EXPECT_EQ(channel_ids[0], 1); // 0 % 3 = 0 → bucket_to_partition[0] = 1
    EXPECT_EQ(channel_ids[1], 3); // 1 % 3 = 1 → bucket_to_partition[1] = 3
    EXPECT_EQ(channel_ids[2], 2); // 2 % 3 = 2 → bucket_to_partition[2] = 2
    EXPECT_EQ(channel_ids[3], 1); // 3 % 3 = 0 → bucket_to_partition[0] = 1
    EXPECT_EQ(channel_ids[4], 3); // 4 % 3 = 1 → bucket_to_partition[1] = 3
    EXPECT_EQ(channel_ids[5], 2); // 5 % 3 = 2 → bucket_to_partition[2] = 2
    EXPECT_EQ(channel_ids[6], 1); // 6 % 3 = 0 → bucket_to_partition[0] = 1
    EXPECT_EQ(channel_ids[7], 3); // 7 % 3 = 1 → bucket_to_partition[1] = 3

    // 测试local_exchange_shuffle，应该和exchange_shuffle结果相同
    std::vector<uint32_t> local_channel_ids(hash_values.size(), 0);
    shuffler->local_exchange_shuffle(local_channel_ids, hash_values, num_rows);
    EXPECT_EQ(channel_ids, local_channel_ids);
}

} // namespace starrocks::pipeline
