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

#include "exec/arrow_scanner.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "testutil/desc_tbl_helper.h"

#include <gtest/gtest.h>
#include "testutil/assert.h"

#include <memory>
#include <utility>
#include <arrow/result.h>

namespace starrocks {

class ArrowScannerTest : public ::testing::Test {
    void SetUp() override {
        _env = ExecEnv::GetInstance();
        _env->_load_stream_mgr = new LoadStreamMgr();
    }

    int32_t date32_to_int(int32_t days_since_epoch) {
        std::tm tm = {};
        tm.tm_year = 70; // 1970
        tm.tm_mon = 0;
        tm.tm_mday = 1 + days_since_epoch;
        std::time_t t = timegm(&tm);
        std::tm* gmt = gmtime(&t);
        int32_t y = gmt->tm_year + 1900;
        int32_t m = gmt->tm_mon + 1;
        int32_t d = gmt->tm_mday;
        return y * 10000 + m * 100 + d;
    }

    std::string random_name(size_t length = 8) {
        static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        static std::mt19937 rng(std::random_device{}());
        static std::uniform_int_distribution<> dist(0, sizeof(charset) - 2);
        std::string result;
        for (size_t i = 0; i < length; ++i) {
            result += charset[dist(rng)];
        }
        return result;
    }

    std::vector<uint8_t> random_binary(size_t length = 16) {
        static std::random_device rd;
        static std::mt19937 gen(rd());
        static std::uniform_int_distribution<> dis(0, 255);

        std::vector<uint8_t> binary(length);
        for (size_t i = 0; i < length; ++i) {
            binary[i] = static_cast<uint8_t>(dis(gen));
        }
        return binary;
    }

    bool append_data_into_pipe(const std::shared_ptr<StreamLoadPipe>& pipe, const uint8_t* data, size_t len) {
        const size_t CHUNK_SIZE = 1024; // 1KB
        size_t offset = 0;
        while (offset < len) {
            size_t this_chunk = std::min(CHUNK_SIZE, len - offset);
            Status st = pipe->append(reinterpret_cast<const char*>(data + offset), this_chunk);
            if (!st.ok()) {
                return false;
            }
            offset += this_chunk;
        }
        return true;
    }

    starrocks::TExpr create_column_ref(int32_t slot_id, const TypeDescriptor& type_desc, bool is_nullable) {
        starrocks::TExpr e = starrocks::TExpr();
        e.nodes.emplace_back(TExprNode());
        e.nodes[0].__set_type(type_desc.to_thrift());
        e.nodes[0].__set_node_type(TExprNodeType::SLOT_REF);
        e.nodes[0].__set_is_nullable(is_nullable);
        e.nodes[0].__set_slot_ref(TSlotRef());
        e.nodes[0].slot_ref.__set_slot_id((::starrocks::TSlotId)slot_id);
        return e;
    }

    std::unique_ptr<ArrowScanner> create_arrow_scanner(
            const SlotTypeDescInfoArray& slot_infos,
            const std::unordered_map<size_t, ::starrocks::TExpr>& dst_slot_exprs,
            const std::vector<TBrokerRangeDesc>& ranges, TBrokerScanRangeParams* params) {
        /// Init RuntimeState
        auto query_globals = TQueryGlobals();
        query_globals.time_zone = "UTC";
        RuntimeState* state = _obj_pool.add(new RuntimeState(TUniqueId(), TQueryOptions(), query_globals, _env));
        auto* desc_tbl = DescTblHelper::generate_desc_tbl(state, _obj_pool, {slot_infos, {}});
        state->set_desc_tbl(desc_tbl);
        state->init_instance_mem_tracker();

        std::vector<TupleDescriptor*> tuples;
        desc_tbl->get_tuple_descs(&tuples);
        const auto num_tuples = tuples.size();
        params->src_tuple_id = 0;
        params->dest_tuple_id = num_tuples - 1;
        const auto* src_tuple = desc_tbl->get_tuple_descriptor(params->src_tuple_id);
        const auto* dst_tuple = desc_tbl->get_tuple_descriptor(params->dest_tuple_id);
        for (int i = 0; i < src_tuple->slots().size(); i++) {
            auto& src_slot = src_tuple->slots()[i];
            auto& dst_slot = dst_tuple->slots()[i];
            if (dst_slot_exprs.count(i)) {
                params->expr_of_dest_slot[dst_slot->id()] = dst_slot_exprs.at(i);
            } else {
                params->expr_of_dest_slot[dst_slot->id()] =
                        create_column_ref(src_slot->id(), src_slot->type(), src_slot->is_nullable());
            }
        }

        for (int i = 0; i < src_tuple->slots().size(); i++) {
            params->src_slot_ids.emplace_back(i);
        }

        RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));
        ScannerCounter* counter = _obj_pool.add(new ScannerCounter());

        TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
        broker_scan_range->params = *params;
        broker_scan_range->ranges = ranges;

        return std::make_unique<ArrowScanner>(state, profile, *broker_scan_range, counter);
    }

    void validate(std::unique_ptr<ArrowScanner>& scanner, const size_t expect_num_rows,
                  const std::function<void(const ChunkPtr&)>& check_func) {
        ASSERT_OK(scanner->open());
        size_t num_rows = 0;
        while (true) {
            auto res = scanner->get_next();
            if (!res.ok() && res.status().is_end_of_file()) {
                ASSERT_EQ(expect_num_rows, num_rows);
                break;
            }
            if (!res.ok()) {
                std::cout << "Unexpected status:" << res.status().to_string() << std::endl;
            }
            ChunkPtr chunk = res.value();
            if (chunk == nullptr) {
                ASSERT_EQ(expect_num_rows, num_rows);
                break;
            }

            ASSERT_TRUE(chunk->num_rows() > 0);
            num_rows += chunk->num_rows();
            check_func(chunk);
        }
        ASSERT_GT(scanner->TEST_scanner_counter()->file_read_count, 0);
        ASSERT_GT(scanner->TEST_scanner_counter()->file_read_ns, 0);
        scanner->close();
    }

    /////////////////////////////////////// UTs ///////////////////////////////////////
    void testThreeColumnWithMutilRecordBatches(size_t num_rows, size_t record_batch_size, size_t arrow_buffer_minSize, size_t arrow_buffer_maxSize, size_t pipe_chunk_size){
        auto ctx = new StreamLoadContext(_env);
        ctx->ref();
        // total buffer size is 2 * 1024 * 1024, which should be large enough to store all data.
        // pipe_chunk_size is each buffer size in StreamLoadPipe
        auto pipe = std::make_shared<StreamLoadPipe>(2 * 1024 * 1024, pipe_chunk_size);
        _env->load_stream_mgr()->put(ctx->id, pipe);
        ctx->body_sink = pipe;

        // 1. Generate arrow data
        // 1.1 Generate mock data
        std::vector<int32_t> ids(num_rows);
        std::vector<std::string> names(num_rows);
        std::vector<int32_t> scores(num_rows);

        for (int i = 0; i < num_rows; ++i) {
            ids[i] = i + 1;
            names[i] = random_name();
            scores[i] = rand() % 101;
        }

        // 1.2 Create Arrow Table and Split data into 3 RecordBatches
        std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
                arrow::field("id", arrow::int32()),
                arrow::field("name", arrow::utf8()),
                arrow::field("score", arrow::int32())
        };
        auto schema = std::make_shared<arrow::Schema>(schema_vector);

        size_t batch_size = num_rows / record_batch_size;
        std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;

        for (int batch_num = 0; batch_num < record_batch_size; ++batch_num) {
            size_t start = batch_num * batch_size;
            size_t end = (batch_num == record_batch_size-1) ? num_rows : (batch_num + 1) * batch_size;

            arrow::Int32Builder id_builder, score_builder;
            arrow::StringBuilder name_builder;

            for (int i = start; i < end; ++i) {
                ASSERT_OK(id_builder.Append(ids[i]));
                ASSERT_OK(name_builder.Append(names[i]));
                ASSERT_OK(score_builder.Append(scores[i]));
            }

            std::shared_ptr<arrow::Array> id_array, name_array, score_array;
            ASSERT_OK(id_builder.Finish(&id_array));
            ASSERT_OK(name_builder.Finish(&name_array));
            ASSERT_OK(score_builder.Finish(&score_array));

            auto record_batch = arrow::RecordBatch::Make(
                    schema, end - start, {id_array, name_array, score_array});

            record_batches.push_back(record_batch);
        }

        // 1.3 Write RecordBatches to Arrow stream
        auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();

        arrow::ipc::IpcWriteOptions options;
        auto maybe_codec = arrow::util::Codec::Create(arrow::Compression::ZSTD, 3).ValueOrDie();
        options.codec = std::move(maybe_codec);

        auto writer = arrow::ipc::MakeStreamWriter(sink.get(), schema, options).ValueOrDie();
        for (const auto& batch : record_batches) {
            ASSERT_OK(writer->WriteRecordBatch(*batch));
        }
        ASSERT_OK(writer->Close());

        auto buffer = sink->Finish().ValueOrDie();
        auto compressed_bytes = buffer->data();
        size_t compressed_bytes_length = buffer->size();

        // 2. put arrow data into pipe
        append_data_into_pipe(pipe, compressed_bytes, compressed_bytes_length);
        pipe->finish();

        // 3. create arrow scanner with load id
        std::vector<TBrokerRangeDesc> ranges;
        TBrokerRangeDesc range;
        range.__set_load_id(ctx->id.to_thrift());
        range.__set_file_type(TFileType::FILE_STREAM);
        range.__set_format_type(TFileFormatType::FORMAT_ARROW_STREAM);
        range.__set_num_of_columns_from_file(3);
        ranges.push_back(range);

        TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());

        //auto slot_infos = select_columns({},true);
        SlotTypeDescInfoArray slot_infos;
        slot_infos.emplace_back("id", TypeDescriptor::from_logical_type(TYPE_INT), true);
        slot_infos.emplace_back("name", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
        slot_infos.emplace_back("score", TypeDescriptor::from_logical_type(TYPE_INT), true);

        std::unique_ptr<ArrowScanner> arrow_scanner = create_arrow_scanner(slot_infos, {}, ranges, params);
        arrow_scanner->setKMinBufferSize(arrow_buffer_minSize);
        arrow_scanner->setKMaxBufferSize(arrow_buffer_maxSize);

        // 4. valid data
        ASSERT_OK(arrow_scanner->open());
        size_t read_rows = 0;
        while (true) {
            auto res = arrow_scanner->get_next();
            if (!res.ok() && res.status().is_end_of_file()) {
                ASSERT_EQ(num_rows, read_rows);
                break;
            }
            if (!res.ok()) {
                std::cout << "Unexpected status:" << res.status().to_string() << std::endl;
            }
            ChunkPtr chunk = res.value();
            if (chunk == nullptr) {
                ASSERT_EQ(num_rows, read_rows);
                break;
            }

            ASSERT_TRUE(chunk->num_rows() > 0);

            auto& columns = chunk->columns();
            int col_idx = 0;
            for (auto& col : columns) {
                ASSERT_TRUE(!col->is_nullable() || !col->is_constant());
                for (int i = 0; i < chunk->num_rows(); i++) {
                    auto val = col->get(i);
                    switch (col_idx) {
                        case 0:
                            ASSERT_EQ(val.get_int32(),ids[i+read_rows]);
                            break;
                        case 1:
                            ASSERT_EQ(0, strncmp(names[i+read_rows].c_str(), val.get_slice().data, val.get_slice().size));
                            break;
                        case 2:
                            ASSERT_EQ(val.get_int32(),scores[i+read_rows]);
                            break;
                        default:
                            FAIL();
                            break;
                    }
                }
                col_idx++;
            }
            read_rows += chunk->num_rows();
        }
        ASSERT_GT(arrow_scanner->TEST_scanner_counter()->file_read_count, 0);
        ASSERT_GT(arrow_scanner->TEST_scanner_counter()->file_read_ns, 0);
        arrow_scanner->close();
    }

    void testFifteenColumnWithMutilRecordBatches(size_t num_rows, size_t record_batch_size, size_t arrow_buffer_minSize, size_t arrow_buffer_maxSize, size_t pipe_chunk_size) {
        auto ctx = new StreamLoadContext(_env);
        ctx->ref();
        // total buffer size is 8 * 1024 * 1024, which should be large enough to store all data.
        // pipe_chunk_size is each buffer size in StreamLoadPipe
        auto pipe = std::make_shared<StreamLoadPipe>(8 * 1024 * 1024, pipe_chunk_size);
        _env->load_stream_mgr()->put(ctx->id, pipe);
        ctx->body_sink = pipe;

        // 1. Generate arrow data
        // 1.1 Generate mock data
        std::vector<int8_t> tinyint_col(num_rows);
        std::vector<int16_t> smallint_col(num_rows);
        std::vector<int32_t> int_col(num_rows);
        std::vector<int64_t> bigint_col(num_rows);
        std::vector<arrow::Decimal128> largeint_col(num_rows);
        std::vector<float> float_col(num_rows);
        std::vector<double> double_col(num_rows);
        std::vector<bool> boolean_col(num_rows);
        std::vector<std::string> char_col(num_rows);
        std::vector<std::string> varchar_col(num_rows);
        std::vector<std::string> string_col(num_rows);
        std::vector<arrow::Date32Type::c_type> date_col(num_rows);
        std::vector<arrow::TimestampType::c_type> datetime_col(num_rows);
        std::vector<std::vector<int32_t>> array_col(num_rows);
        std::vector<std::string> json_col(num_rows);

        static std::random_device rd;
        static std::mt19937 gen(rd());
        std::uniform_int_distribution<int8_t> tinyint_dist(-128, 127);
        std::uniform_int_distribution<int16_t> smallint_dist(-32768, 32767);
        std::uniform_int_distribution<int64_t> bigint_dist(-9223372036854775807, 9223372036854775807);
        std::uniform_real_distribution<double> decimal_dist(-1000000, 1000000);
        std::uniform_real_distribution<float> float_dist(-1000, 1000);
        std::uniform_real_distribution<double> double_dist(-10000, 10000);
        std::bernoulli_distribution bool_dist(0.5);

        for (int i = 0; i < num_rows; ++i) {
            tinyint_col[i] = tinyint_dist(gen);
            smallint_col[i] = smallint_dist(gen);
            int_col[i] = i + 1;
            bigint_col[i] = bigint_dist(gen);

            // Largeint (128-bit) as Decimal128
            largeint_col[i] = arrow::Decimal128::FromBigEndian(
                    random_binary(16).data(), 16).ValueOrDie();

            float_col[i] = float_dist(gen);
            double_col[i] = double_dist(gen);
            boolean_col[i] = bool_dist(gen);

            char_col[i] = random_name(100);
            varchar_col[i] = random_name(10 + (i % 91)); // 10-100 length
            string_col[i] = random_name(50 + (i % 151)); // 50-200 length

            // Date (days since epoch)
            date_col[i] = i;

            // Timestamp (microseconds since epoch)
            datetime_col[i] = i * 1000000LL;

            // Array
            int array_size = 1 + (i % 5);
            array_col[i].resize(array_size);
            for (int j = 0; j < array_size; ++j) {
                array_col[i][j] = 1 + (i + j) % 100;
            }

            // JSON (as string)
            json_col[i] = "{\"name\":\"" + random_name() + "\",\"value\":" +
                          std::to_string(1 + (i % 100)) + "}";
        }

        // 1.2 Create Arrow Table and Split data into 3 RecordBatches
        // Create Arrow arrays
        auto schema = arrow::schema({
                arrow::field("tinyint_col", arrow::int8()),
                arrow::field("smallint_col", arrow::int16()),
                arrow::field("int_col", arrow::int32()),
                arrow::field("bigint_col", arrow::int64()),
                arrow::field("largeint_col", arrow::decimal(38, 0)),
                arrow::field("float_col", arrow::float32()),
                arrow::field("double_col", arrow::float64()),
                arrow::field("boolean_col", arrow::boolean()),
                arrow::field("char_col", arrow::fixed_size_binary(100)),
                arrow::field("varchar_col", arrow::utf8()),
                arrow::field("string_col", arrow::utf8()),
                arrow::field("date_col", arrow::date32()),
                arrow::field("datetime_col", arrow::timestamp(arrow::TimeUnit::MICRO)),
                arrow::field("array_col", arrow::list(arrow::int32())),
                arrow::field("json_col", arrow::utf8())
        });

        size_t batch_size = num_rows / record_batch_size;
        std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;

        for (int batch_num = 0; batch_num < record_batch_size; ++batch_num) {
            size_t start = batch_num * batch_size;
            size_t end = (batch_num == record_batch_size - 1) ? num_rows : (batch_num + 1) * batch_size;

            arrow::Int8Builder tinyint_builder;
            arrow::Int16Builder smallint_builder;
            arrow::Int32Builder int_builder;
            arrow::Int64Builder bigint_builder;
            arrow::Decimal128Builder largeint_builder(arrow::decimal(38, 0));
            arrow::Decimal128Builder decimal_builder(arrow::decimal(10, 6));
            arrow::FloatBuilder float_builder;
            arrow::DoubleBuilder double_builder;
            arrow::BooleanBuilder boolean_builder;
            arrow::FixedSizeBinaryBuilder char_builder(arrow::fixed_size_binary(100));
            arrow::StringBuilder varchar_builder;
            arrow::StringBuilder string_builder;
            arrow::Date32Builder date_builder;
            arrow::TimestampBuilder datetime_builder(arrow::timestamp(arrow::TimeUnit::MICRO),arrow::default_memory_pool());
            arrow::ListBuilder array_builder(arrow::default_memory_pool(),
                                             std::make_shared<arrow::Int32Builder>());
            arrow::StringBuilder json_builder;

            // Append values to builders
            for (int i = start; i < end; ++i) {
                ASSERT_OK(tinyint_builder.Append(tinyint_col[i]));
                ASSERT_OK(smallint_builder.Append(smallint_col[i]));
                ASSERT_OK(int_builder.Append(int_col[i]));
                ASSERT_OK(bigint_builder.Append(bigint_col[i]));
                ASSERT_OK(largeint_builder.Append(arrow::Decimal128(largeint_col[i])));
                ASSERT_OK(float_builder.Append(float_col[i]));
                ASSERT_OK(double_builder.Append(double_col[i]));
                ASSERT_OK(boolean_builder.Append(boolean_col[i]));
                ASSERT_OK(char_builder.Append(char_col[i]));
                ASSERT_OK(varchar_builder.Append(varchar_col[i]));
                ASSERT_OK(string_builder.Append(string_col[i]));
                ASSERT_OK(date_builder.Append(date_col[i]));
                ASSERT_OK(datetime_builder.Append(datetime_col[i]));
                ASSERT_OK(array_builder.Append());
                auto value_builder = static_cast<arrow::Int32Builder*>(array_builder.value_builder());
                ASSERT_OK(value_builder->AppendValues(array_col[i].data(), array_col[i].size()));
                ASSERT_OK(json_builder.Append(json_col[i]));
            }

            std::shared_ptr<arrow::Array> tinyint_array, smallint_array, int_array, bigint_array;
            std::shared_ptr<arrow::Array> largeint_array, float_array, double_array;
            std::shared_ptr<arrow::Array> boolean_array, char_array, varchar_array, string_array;
            std::shared_ptr<arrow::Array> date_array, datetime_array, array_array, json_array;

            ASSERT_OK(tinyint_builder.Finish(&tinyint_array));
            ASSERT_OK(smallint_builder.Finish(&smallint_array));
            ASSERT_OK(int_builder.Finish(&int_array));
            ASSERT_OK(bigint_builder.Finish(&bigint_array));
            ASSERT_OK(largeint_builder.Finish(&largeint_array));
            ASSERT_OK(float_builder.Finish(&float_array));
            ASSERT_OK(double_builder.Finish(&double_array));
            ASSERT_OK(boolean_builder.Finish(&boolean_array));
            ASSERT_OK(char_builder.Finish(&char_array));
            ASSERT_OK(varchar_builder.Finish(&varchar_array));
            ASSERT_OK(string_builder.Finish(&string_array));
            ASSERT_OK(date_builder.Finish(&date_array));
            ASSERT_OK(datetime_builder.Finish(&datetime_array));
            ASSERT_OK(array_builder.Finish(&array_array));
            ASSERT_OK(json_builder.Finish(&json_array));

            auto record_batch = arrow::RecordBatch::Make(
                    schema, end - start, {tinyint_array, smallint_array, int_array, bigint_array,
                                          largeint_array, float_array, double_array,
                                          boolean_array, char_array, varchar_array, string_array,
                                          date_array, datetime_array, array_array, json_array});

            record_batches.push_back(record_batch);
        }

        // 1.3 Write to Arrow stream with ZSTD compression
        auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();

        arrow::ipc::IpcWriteOptions options;
        auto maybe_codec = arrow::util::Codec::Create(arrow::Compression::ZSTD, 3).ValueOrDie();
        options.codec = std::move(maybe_codec);

        auto writer = arrow::ipc::MakeStreamWriter(sink.get(), schema, options).ValueOrDie();
        for (const auto& batch : record_batches) {
            ASSERT_OK(writer->WriteRecordBatch(*batch));
        }
        ASSERT_OK(writer->Close());

        auto buffer = sink->Finish().ValueOrDie();
        auto compressed_bytes = buffer->data();
        size_t compressed_bytes_length = buffer->size();

        // 2. put arrow data into pipe
        append_data_into_pipe(pipe, compressed_bytes, compressed_bytes_length);
        pipe->finish();

        // 3. create arrow scanner with load id
        std::vector<TBrokerRangeDesc> ranges;
        TBrokerRangeDesc range;
        range.__set_load_id(ctx->id.to_thrift());
        range.__set_file_type(TFileType::FILE_STREAM);
        range.__set_format_type(TFileFormatType::FORMAT_ARROW_STREAM);
        range.__set_num_of_columns_from_file(15);
        ranges.push_back(range);

        TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());

        SlotTypeDescInfoArray slot_infos;
        slot_infos.emplace_back("tinyint_col", TypeDescriptor::from_logical_type(TYPE_TINYINT), true);
        slot_infos.emplace_back("smallint_col", TypeDescriptor::from_logical_type(TYPE_SMALLINT), true);
        slot_infos.emplace_back("int_col", TypeDescriptor::from_logical_type(TYPE_INT), true);
        slot_infos.emplace_back("bigint_col", TypeDescriptor::from_logical_type(TYPE_BIGINT), true);
        slot_infos.emplace_back("largeint_col", TypeDescriptor::from_logical_type(TYPE_LARGEINT), true);
        slot_infos.emplace_back("float_col", TypeDescriptor::from_logical_type(TYPE_FLOAT), true);
        slot_infos.emplace_back("double_col", TypeDescriptor::from_logical_type(TYPE_DOUBLE), true);
        slot_infos.emplace_back("boolean_col", TypeDescriptor::from_logical_type(TYPE_BOOLEAN), true);
        slot_infos.emplace_back("char_col", TypeDescriptor::from_logical_type(TYPE_CHAR), true);
        slot_infos.emplace_back("varchar_col", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
        slot_infos.emplace_back("string_col", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
        slot_infos.emplace_back("date_col", TypeDescriptor::from_logical_type(TYPE_DATE), true);
        slot_infos.emplace_back("datetime_col", TypeDescriptor::from_logical_type(TYPE_DATETIME), true);

        // array info
        TypeDescriptor t_arr=TypeDescriptor::from_logical_type(TYPE_ARRAY);
        t_arr.children.emplace_back(TYPE_INT);
        slot_infos.emplace_back("array_col", t_arr, true);
        slot_infos.emplace_back("json_col", TypeDescriptor::from_logical_type(TYPE_JSON), true);

        std::unique_ptr<ArrowScanner> arrow_scanner = create_arrow_scanner(slot_infos, {}, ranges, params);
        arrow_scanner->setKMinBufferSize(arrow_buffer_minSize);
        arrow_scanner->setKMaxBufferSize(arrow_buffer_maxSize);

        // 4. valid data
        ASSERT_OK(arrow_scanner->open());
        size_t read_rows = 0;
        while (true) {
            auto res = arrow_scanner->get_next();
            if (!res.ok() && res.status().is_end_of_file()) {
                ASSERT_EQ(num_rows, read_rows);
                break;
            }
            if (!res.ok()) {
                std::cout << "Unexpected status:" << res.status().to_string() << std::endl;
            }
            ChunkPtr chunk = res.value();
            if (chunk == nullptr) {
                ASSERT_EQ(num_rows, read_rows);
                break;
            }

            ASSERT_TRUE(chunk->num_rows() > 0);

            auto& columns = chunk->columns();
            int col_idx = 0;
            for (auto& col : columns) {
                ASSERT_TRUE(!col->is_nullable() || !col->is_constant());
                for (int i = 0; i < chunk->num_rows(); i++) {
                    auto val = col->get(i);
                    switch (col_idx) {
                        case 0:
                            ASSERT_EQ(val.get_int8(),tinyint_col[i+read_rows]);
                            break;
                        case 1:
                            ASSERT_EQ(val.get_int16(),smallint_col[i+read_rows]);
                            break;
                        case 2:
                            ASSERT_EQ(val.get_int32(),int_col[i+read_rows]);
                            break;
                        case 3:
                            ASSERT_EQ(val.get_int64(),bigint_col[i+read_rows]);
                            break;
                        case 4: {
                            int128_t int128_value = 0;
                            memcpy(&int128_value, largeint_col[i + read_rows].ToBytes().data(), 16);
                            ASSERT_EQ(val.get_int128(), int128_value);
                            break;
                        }
                        case 5:
                            ASSERT_EQ(val.get_float(),float_col[i+read_rows]);
                            break;
                        case 6:
                            ASSERT_EQ(val.get_double(),double_col[i+read_rows]);
                            break;
                        case 7:
                            ASSERT_EQ(val.get<bool>(),boolean_col[i+read_rows]);
                            break;
                        case 8:
                            ASSERT_EQ(0, strncmp(char_col[i+read_rows].c_str(), val.get_slice().data, val.get_slice().size));
                            break;
                        case 9:
                            ASSERT_EQ(0, strncmp(varchar_col[i+read_rows].c_str(), val.get_slice().data, val.get_slice().size));
                            break;
                        case 10:
                            ASSERT_EQ(0, strncmp(string_col[i+read_rows].c_str(), val.get_slice().data, val.get_slice().size));
                            break;
                        case 11:
                            ASSERT_EQ(val.get_date().to_date_literal(), date32_to_int(date_col[i+read_rows]));
                            break;
                        case 12:
                            ASSERT_EQ(val.get_timestamp().to_unix_second(), (datetime_col[i+read_rows]/1000000LL)+28800);
                            break;
                        case 13:
                            ASSERT_EQ(val.get_array().size(), array_col[i+read_rows].size());
                            break;
                        case 14: {
                            std::string s = val.get_json()->to_string().value();
                            s.erase(std::remove(s.begin(), s.end(), ' '), s.end()); // remove all white spaces
                            ASSERT_EQ(s, json_col[i + read_rows]);
                            break;
                        }
                        default:
                            FAIL();
                            break;
                    }
                }
                col_idx++;
            }
            read_rows += chunk->num_rows();
        }
        ASSERT_GT(arrow_scanner->TEST_scanner_counter()->file_read_count, 0);
        ASSERT_GT(arrow_scanner->TEST_scanner_counter()->file_read_ns, 0);
        arrow_scanner->close();
    }


private:
    ExecEnv* _env;
    ObjectPool _obj_pool;
};

TEST_F(ArrowScannerTest, three_columns) {
        auto ctx = new StreamLoadContext(_env);
        ctx->ref();
        auto pipe = std::make_shared<StreamLoadPipe>();
        _env->load_stream_mgr()->put(ctx->id, pipe);
        ctx->body_sink = pipe;

        // 1. Generate arrow data
        // 1.1 Generate mock data
        size_t num_rows = 5000;
        std::vector<int32_t> ids(num_rows);
        std::vector<std::string> names(num_rows);
        std::vector<int32_t> scores(num_rows);

        for (int i = 0; i < num_rows; ++i) {
            ids[i] = i + 1;
            names[i] = random_name();
            scores[i] = rand() % 101;
        }

        // 1.2 Create Arrow Table
        arrow::Int32Builder id_builder, score_builder;
        arrow::StringBuilder name_builder;

        for (int i = 0; i < num_rows; ++i) {
            ASSERT_OK(id_builder.Append(ids[i]));
            ASSERT_OK(name_builder.Append(names[i]));
            ASSERT_OK(score_builder.Append(scores[i]));
        }

        std::shared_ptr<arrow::Array> id_array, name_array, score_array;
        ASSERT_OK(id_builder.Finish(&id_array));
        ASSERT_OK(name_builder.Finish(&name_array));
        ASSERT_OK(score_builder.Finish(&score_array));

        std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
                arrow::field("id", arrow::int32()),
                arrow::field("name", arrow::utf8()),
                arrow::field("score", arrow::int32())
        };
        auto schema = std::make_shared<arrow::Schema>(schema_vector);

        std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {id_array, name_array, score_array});

        // 1.3 Write to Arrow stream with ZSTD compression
        auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();

        arrow::ipc::IpcWriteOptions options;
        auto maybe_codec = arrow::util::Codec::Create(arrow::Compression::ZSTD, 3).ValueOrDie();
        options.codec = std::move(maybe_codec);

        auto writer = arrow::ipc::MakeStreamWriter(sink.get(), table->schema(), options).ValueOrDie();
        ASSERT_OK(writer->WriteTable(*table));
        ASSERT_OK(writer->Close());

        auto buffer = sink->Finish().ValueOrDie();
        auto compressed_bytes = buffer->data();
        size_t compressed_bytes_length = buffer->size();

        // 2. put arrow data into pipe
        append_data_into_pipe(pipe, compressed_bytes, compressed_bytes_length);
        pipe->finish();

        // 3. create arrow scanner with load id
        std::vector<TBrokerRangeDesc> ranges;
        TBrokerRangeDesc range;
        range.__set_load_id(ctx->id.to_thrift());
        range.__set_file_type(TFileType::FILE_STREAM);
        range.__set_format_type(TFileFormatType::FORMAT_ARROW_STREAM);
        range.__set_num_of_columns_from_file(3);
        ranges.push_back(range);

        TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());

        //auto slot_infos = select_columns({},true);
        SlotTypeDescInfoArray slot_infos;
        slot_infos.emplace_back("id", TypeDescriptor::from_logical_type(TYPE_INT), true);
        slot_infos.emplace_back("name", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
        slot_infos.emplace_back("score", TypeDescriptor::from_logical_type(TYPE_INT), true);

        std::unique_ptr<ArrowScanner> arrow_scanner = create_arrow_scanner(slot_infos, {}, ranges, params);

        // 4. valid data
        ASSERT_OK(arrow_scanner->open());
        size_t read_rows = 0;
        while (true) {
            auto res = arrow_scanner->get_next();
            if (!res.ok() && res.status().is_end_of_file()) {
                ASSERT_EQ(num_rows, read_rows);
                break;
            }
            if (!res.ok()) {
                std::cout << "Unexpected status:" << res.status().to_string() << std::endl;
            }
            ChunkPtr chunk = res.value();
            if (chunk == nullptr) {
                ASSERT_EQ(num_rows, read_rows);
                break;
            }

            ASSERT_TRUE(chunk->num_rows() > 0);

            auto& columns = chunk->columns();
            int col_idx = 0;
            for (auto& col : columns) {
                ASSERT_TRUE(!col->is_nullable() || !col->is_constant());
                for (int i = 0; i < chunk->num_rows(); i++) {
                    auto val = col->get(i);
                    switch (col_idx) {
                        case 0:
                            ASSERT_EQ(val.get_int32(),ids[i+read_rows]);
                            break;
                        case 1:
                            ASSERT_EQ(0, strncmp(names[i+read_rows].c_str(), val.get_slice().data, val.get_slice().size));
                            break;
                        case 2:
                            ASSERT_EQ(val.get_int32(),scores[i+read_rows]);
                            break;
                        default:
                            FAIL();
                            break;
                    }
                }
                col_idx++;
            }
            read_rows += chunk->num_rows();
        }
        ASSERT_GT(arrow_scanner->TEST_scanner_counter()->file_read_count, 0);
        ASSERT_GT(arrow_scanner->TEST_scanner_counter()->file_read_ns, 0);
        arrow_scanner->close();
}


// 5000 rows, 3 record batch, 1024 bytes each buffer in stream load pipe
// arrow reader buffer size is  (128 * 1024, 8 * 1024 * 1024), which is large enough to store all record batches in one time
TEST_F(ArrowScannerTest, few_cols_test1) {
    testThreeColumnWithMutilRecordBatches(5000, 3, 128 * 1024, 8 * 1024 * 1024, 1024);
    // only one large buffer in stream load pipe
    testThreeColumnWithMutilRecordBatches(5000, 3, 128 * 1024, 8 * 1024 * 1024,  8 * 1024 * 1024);
}

// 5000 rows, 3 record batch, 1024 bytes each buffer in stream load pipe
// arrow reader buffer size is  (1024, 8 * 1024 * 1024), which is not large enough to store all record batches in one time
TEST_F(ArrowScannerTest, few_cols_test2) {
    testThreeColumnWithMutilRecordBatches(5000, 3, 1024, 8 * 1024 * 1024, 1024);
    // only one large buffer in stream load pipe
    testThreeColumnWithMutilRecordBatches(5000, 3, 1024, 8 * 1024 * 1024, 8 * 1024 * 1024);
}

// 5000 rows, 1 record batch, 1024 bytes each buffer in stream load pipe
// arrow reader buffer size is  (128 * 1024, 8 * 1024 * 1024), which is large enough to store all record batches in one time
TEST_F(ArrowScannerTest, few_cols_test3) {
    testThreeColumnWithMutilRecordBatches(5000, 1, 128 * 1024, 8 * 1024 * 1024, 1024);
    // only one large buffer in stream load pipe
    testThreeColumnWithMutilRecordBatches(5000, 1, 128 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024);

}

// 5000 rows, 1 record batch, 1024 bytes each buffer in stream load pipe
// arrow reader buffer size is  (1024, 8 * 1024 * 1024), which is not large enough to store all record batches in one time
TEST_F(ArrowScannerTest, few_cols_test4) {
    testThreeColumnWithMutilRecordBatches(5000, 1, 1024, 8 * 1024 * 1024, 1024);
    // only one large buffer in stream load pipe
    testThreeColumnWithMutilRecordBatches(5000, 1, 1024, 8 * 1024 * 1024, 8 * 1024 * 1024);

}

// 10000 rows, 3 record batch, 1024 bytes each buffer in stream load pipe
// arrow reader buffer size is  (128 * 1024, 8 * 1024 * 1024), which is large enough to store all record batches in one time
TEST_F(ArrowScannerTest, few_cols_test5) {
    testThreeColumnWithMutilRecordBatches(10000, 3, 128 * 1024, 8 * 1024 * 1024, 1024);
    // only one large buffer in stream load pipe
    testThreeColumnWithMutilRecordBatches(10000, 3, 128 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024);
}

// 10000 rows, 3 record batch, 1024 bytes each buffer in stream load pipe
// arrow reader buffer size is  (1024, 8 * 1024 * 1024), which is not large enough to store all record batches in one time
TEST_F(ArrowScannerTest, few_cols_test6) {
    testThreeColumnWithMutilRecordBatches(10000, 3, 1024, 8 * 1024 * 1024, 1024);
    // only one large buffer in stream load pipe
    testThreeColumnWithMutilRecordBatches(10000, 3, 1024, 8 * 1024 * 1024, 8 * 1024 * 1024);
}

// 10000 rows, 1 record batch, 1024 bytes each buffer in stream load pipe
// arrow reader buffer size is  (128 * 1024, 8 * 1024 * 1024), which is large enough to store all record batches in one time
TEST_F(ArrowScannerTest, few_cols_test7) {
    testThreeColumnWithMutilRecordBatches(10000, 1, 128 * 1024, 8 * 1024 * 1024, 1024);
    // only one large buffer in stream load pipe
    testThreeColumnWithMutilRecordBatches(10000, 1, 128 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024);
}

// 10000 rows, 1 record batch, 1024 bytes each buffer in stream load pipe
// arrow reader buffer size is  (1024, 8 * 1024 * 1024), which is not large enough to store all record batches in one time
TEST_F(ArrowScannerTest, few_cols_test8) {
    testThreeColumnWithMutilRecordBatches(10000, 1, 1024, 8 * 1024 * 1024, 1024);
    // only one large buffer in stream load pipe
    testThreeColumnWithMutilRecordBatches(10000, 1, 1024, 8 * 1024 * 1024, 8 * 1024 * 1024);
}

// 5000 rows, 3 record batch, 1024 bytes each buffer in stream load pipe
// arrow reader buffer size is  (5 * 1024 * 1024, 8 * 1024 * 1024), which is large enough to store all record batches in one time
TEST_F(ArrowScannerTest, many_cols_test1) {
    testFifteenColumnWithMutilRecordBatches(5000, 3, 5 * 1024 * 1024, 8 * 1024 * 1024, 1024);
    // only one large buffer in stream load pipe
    testFifteenColumnWithMutilRecordBatches(5000, 3, 5 * 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024);
}

// 5000 rows, 3 record batch, 1024 bytes each buffer in stream load pipe
// arrow reader buffer size is  (1024, 8 * 1024 * 1024), which is not large enough to store all record batches in one time
TEST_F(ArrowScannerTest, many_cols_test2) {
    testFifteenColumnWithMutilRecordBatches(5000, 3, 1024, 8 * 1024 * 1024, 1024);
    // only one large buffer in stream load pipe
    testFifteenColumnWithMutilRecordBatches(5000, 3, 1024, 8 * 1024 * 1024, 8 * 1024 * 1024);
}

// 5000 rows, 1 record batch, 1024 bytes each buffer in stream load pipe
// arrow reader buffer size is  (5 * 1024 * 1024, 8 * 1024 * 1024), which is large enough to store all record batches in one time
TEST_F(ArrowScannerTest, many_cols_test3) {
    testFifteenColumnWithMutilRecordBatches(5000, 1, 5 * 1024 * 1024, 8 * 1024 * 1024, 1024);
    // only one large buffer in stream load pipe
    testFifteenColumnWithMutilRecordBatches(5000, 1, 5 * 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024);
}

// 5000 rows, 1 record batch, 1024 bytes each buffer in stream load pipe
// arrow reader buffer size is  (1024, 8 * 1024 * 1024), which is not large enough to store all record batches in one time
TEST_F(ArrowScannerTest, many_cols_test4) {
    testFifteenColumnWithMutilRecordBatches(5000, 1, 1024, 8 * 1024 * 1024, 1024);
    // only one large buffer in stream load pipe
    testFifteenColumnWithMutilRecordBatches(5000, 1, 1024, 8 * 1024 * 1024, 8 * 1024 * 1024);
}

// 10000 rows, 3 record batch, 1024 bytes each buffer in stream load pipe
// arrow reader buffer size is  (5 * 1024 * 1024, 8 * 1024 * 1024), which is large enough to store all record batches in one time
TEST_F(ArrowScannerTest, many_cols_test5) {
    testFifteenColumnWithMutilRecordBatches(10000, 3, 5 * 1024 * 1024, 8 * 1024 * 1024, 1024);
    // only one large buffer in stream load pipe
    testFifteenColumnWithMutilRecordBatches(10000, 3, 5 * 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024);
}

// 10000 rows, 3 record batch, 1024 bytes each buffer in stream load pipe
// arrow reader buffer size is  (1024, 8 * 1024 * 1024), which is not large enough to store all record batches in one time
TEST_F(ArrowScannerTest, many_cols_test6) {
    testFifteenColumnWithMutilRecordBatches(10000, 3, 1024, 8 * 1024 * 1024, 1024);
    // only one large buffer in stream load pipe
    testFifteenColumnWithMutilRecordBatches(10000, 3, 1024, 8 * 1024 * 1024, 8 * 1024 * 1024);
}

// 10000 rows, 1 record batch, 1024 bytes each buffer in stream load pipe
// arrow reader buffer size is  (5 * 1024 * 1024, 8 * 1024 * 1024), which is large enough to store all record batches in one time
TEST_F(ArrowScannerTest, many_cols_test7) {
    testFifteenColumnWithMutilRecordBatches(10000, 1, 5 * 1024 * 1024, 8 * 1024 * 1024, 1024);
    // only one large buffer in stream load pipe
    testFifteenColumnWithMutilRecordBatches(10000, 1, 5 * 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024);
}

// 10000 rows, 1 record batch, 1024 bytes each buffer in stream load pipe
// arrow reader buffer size is  (1024, 8 * 1024 * 1024), which is not large enough to store all record batches in one time
TEST_F(ArrowScannerTest, many_cols_test8) {
    testFifteenColumnWithMutilRecordBatches(10000, 1, 1024, 8 * 1024 * 1024, 1024);
    // only one large buffer in stream load pipe
    testFifteenColumnWithMutilRecordBatches(10000, 1, 1024, 8 * 1024 * 1024, 8 * 1024 * 1024);
}

TEST_F(ArrowScannerTest, multi_columns) {
    auto ctx = new StreamLoadContext(_env);
    ctx->ref();
    auto pipe = std::make_shared<StreamLoadPipe>(2 * 1024 * 1024, 64 * 1024);
    _env->load_stream_mgr()->put(ctx->id, pipe);
    ctx->body_sink = pipe;

    // 1. Generate arrow data
    // 1.1 Generate mock data
    size_t num_rows = 5000;

    std::vector<int8_t> tinyint_col(num_rows);
    std::vector<int16_t> smallint_col(num_rows);
    std::vector<int32_t> int_col(num_rows);
    std::vector<int64_t> bigint_col(num_rows);
    std::vector<arrow::Decimal128> largeint_col(num_rows);
    std::vector<float> float_col(num_rows);
    std::vector<double> double_col(num_rows);
    std::vector<bool> boolean_col(num_rows);
    std::vector<std::string> char_col(num_rows);
    std::vector<std::string> varchar_col(num_rows);
    std::vector<std::string> string_col(num_rows);
    std::vector<arrow::Date32Type::c_type> date_col(num_rows);
    std::vector<arrow::TimestampType::c_type> datetime_col(num_rows);
    std::vector<std::vector<int32_t>> array_col(num_rows);
    std::vector<std::string> json_col(num_rows);

    static std::random_device rd;
    static std::mt19937 gen(rd());
    std::uniform_int_distribution<int8_t> tinyint_dist(-128, 127);
    std::uniform_int_distribution<int16_t> smallint_dist(-32768, 32767);
    std::uniform_int_distribution<int64_t> bigint_dist(-9223372036854775807, 9223372036854775807);
    std::uniform_real_distribution<double> decimal_dist(-1000000, 1000000);
    std::uniform_real_distribution<float> float_dist(-1000, 1000);
    std::uniform_real_distribution<double> double_dist(-10000, 10000);
    std::bernoulli_distribution bool_dist(0.5);

    for (int i = 0; i < num_rows; ++i) {
        tinyint_col[i] = tinyint_dist(gen);
        smallint_col[i] = smallint_dist(gen);
        int_col[i] = i + 1;
        bigint_col[i] = bigint_dist(gen);

        // Largeint (128-bit) as Decimal128
        largeint_col[i] = arrow::Decimal128::FromBigEndian(
            random_binary(16).data(), 16).ValueOrDie();

        float_col[i] = float_dist(gen);
        double_col[i] = double_dist(gen);
        boolean_col[i] = bool_dist(gen);

        char_col[i] = random_name(100);
        varchar_col[i] = random_name(10 + (i % 91)); // 10-100 length
        string_col[i] = random_name(50 + (i % 151)); // 50-200 length

        // Date (days since epoch)
        date_col[i] = i;

        // Timestamp (microseconds since epoch)
        datetime_col[i] = i * 1000000LL;

        // Array
        int array_size = 1 + (i % 5);
        array_col[i].resize(array_size);
        for (int j = 0; j < array_size; ++j) {
            array_col[i][j] = 1 + (i + j) % 100;
        }

        // JSON (as string)
        json_col[i] = "{\"name\":\"" + random_name() + "\",\"value\":" +
                      std::to_string(1 + (i % 100)) + "}";
    }

    // 1.2 Create Arrow Table
        // Create Arrow arrays
        arrow::Int8Builder tinyint_builder;
        arrow::Int16Builder smallint_builder;
        arrow::Int32Builder int_builder;
        arrow::Int64Builder bigint_builder;
        arrow::Decimal128Builder largeint_builder(arrow::decimal(38, 0));
        arrow::Decimal128Builder decimal_builder(arrow::decimal(10, 6));
        arrow::FloatBuilder float_builder;
        arrow::DoubleBuilder double_builder;
        arrow::BooleanBuilder boolean_builder;
        arrow::FixedSizeBinaryBuilder char_builder(arrow::fixed_size_binary(100));
        arrow::StringBuilder varchar_builder;
        arrow::StringBuilder string_builder;
        arrow::Date32Builder date_builder;
        arrow::TimestampBuilder datetime_builder(arrow::timestamp(arrow::TimeUnit::MICRO),arrow::default_memory_pool());
        arrow::ListBuilder array_builder(arrow::default_memory_pool(),
                                         std::make_shared<arrow::Int32Builder>());
        arrow::StringBuilder json_builder;

        // Append values to builders
        for (int i = 0; i < num_rows; ++i) {
            ASSERT_OK(tinyint_builder.Append(tinyint_col[i]));
            ASSERT_OK(smallint_builder.Append(smallint_col[i]));
            ASSERT_OK(int_builder.Append(int_col[i]));
            ASSERT_OK(bigint_builder.Append(bigint_col[i]));
            ASSERT_OK(largeint_builder.Append(arrow::Decimal128(largeint_col[i])));
            ASSERT_OK(float_builder.Append(float_col[i]));
            ASSERT_OK(double_builder.Append(double_col[i]));
            ASSERT_OK(boolean_builder.Append(boolean_col[i]));
            ASSERT_OK(char_builder.Append(char_col[i]));
            ASSERT_OK(varchar_builder.Append(varchar_col[i]));
            ASSERT_OK(string_builder.Append(string_col[i]));
            ASSERT_OK(date_builder.Append(date_col[i]));
            ASSERT_OK(datetime_builder.Append(datetime_col[i]));
            ASSERT_OK(array_builder.Append());
            auto value_builder = static_cast<arrow::Int32Builder*>(array_builder.value_builder());
            ASSERT_OK(value_builder->AppendValues(array_col[i].data(), array_col[i].size()));
            ASSERT_OK(json_builder.Append(json_col[i]));
        }

        std::shared_ptr<arrow::Array> tinyint_array, smallint_array, int_array, bigint_array;
        std::shared_ptr<arrow::Array> largeint_array, float_array, double_array;
        std::shared_ptr<arrow::Array> boolean_array, char_array, varchar_array, string_array;
        std::shared_ptr<arrow::Array> date_array, datetime_array, array_array;
        std::shared_ptr<arrow::Array> json_array;

        ASSERT_OK(tinyint_builder.Finish(&tinyint_array));
        ASSERT_OK(smallint_builder.Finish(&smallint_array));
        ASSERT_OK(int_builder.Finish(&int_array));
        ASSERT_OK(bigint_builder.Finish(&bigint_array));
        ASSERT_OK(largeint_builder.Finish(&largeint_array));
        ASSERT_OK(float_builder.Finish(&float_array));
        ASSERT_OK(double_builder.Finish(&double_array));
        ASSERT_OK(boolean_builder.Finish(&boolean_array));
        ASSERT_OK(char_builder.Finish(&char_array));
        ASSERT_OK(varchar_builder.Finish(&varchar_array));
        ASSERT_OK(string_builder.Finish(&string_array));
        ASSERT_OK(date_builder.Finish(&date_array));
        ASSERT_OK(datetime_builder.Finish(&datetime_array));
        ASSERT_OK(array_builder.Finish(&array_array));
        ASSERT_OK(json_builder.Finish(&json_array));

        auto schema = arrow::schema({
                                            arrow::field("tinyint_col", arrow::int8()),
                                            arrow::field("smallint_col", arrow::int16()),
                                            arrow::field("int_col", arrow::int32()),
                                            arrow::field("bigint_col", arrow::int64()),
                                            arrow::field("largeint_col", arrow::decimal(38, 0)),
                                            arrow::field("float_col", arrow::float32()),
                                            arrow::field("double_col", arrow::float64()),
                                            arrow::field("boolean_col", arrow::boolean()),
                                            arrow::field("char_col", arrow::fixed_size_binary(100)),
                                            arrow::field("varchar_col", arrow::utf8()),
                                            arrow::field("string_col", arrow::utf8()),
                                            arrow::field("date_col", arrow::date32()),
                                            arrow::field("datetime_col", arrow::timestamp(arrow::TimeUnit::MICRO)),
                                            arrow::field("array_col", arrow::list(arrow::int32())),
                                            arrow::field("json_col", arrow::utf8())
                                    });

        auto table = arrow::Table::Make(schema, {
                tinyint_array, smallint_array, int_array, bigint_array, largeint_array,
                float_array, double_array, boolean_array, char_array,
                varchar_array, string_array, date_array, datetime_array,
                array_array, json_array
        }, num_rows);

    // 1.3 Write to Arrow stream with ZSTD compression
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();

    arrow::ipc::IpcWriteOptions options;
    auto maybe_codec = arrow::util::Codec::Create(arrow::Compression::ZSTD, 3).ValueOrDie();
    options.codec = std::move(maybe_codec);

    auto writer = arrow::ipc::MakeStreamWriter(sink.get(), table->schema(), options).ValueOrDie();
    ASSERT_OK(writer->WriteTable(*table));
    ASSERT_OK(writer->Close());

    auto buffer = sink->Finish().ValueOrDie();
    auto compressed_bytes = buffer->data();
    size_t compressed_bytes_length = buffer->size();

    // 2. put arrow data into pipe
    append_data_into_pipe(pipe, compressed_bytes, compressed_bytes_length);
    pipe->finish();

    // 3. create arrow scanner with load id
    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_load_id(ctx->id.to_thrift());
    range.__set_file_type(TFileType::FILE_STREAM);
    range.__set_format_type(TFileFormatType::FORMAT_ARROW_STREAM);
    range.__set_num_of_columns_from_file(15);
    ranges.push_back(range);

    TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());

    SlotTypeDescInfoArray slot_infos;
    slot_infos.emplace_back("tinyint_col", TypeDescriptor::from_logical_type(TYPE_TINYINT), true);
    slot_infos.emplace_back("smallint_col", TypeDescriptor::from_logical_type(TYPE_SMALLINT), true);
    slot_infos.emplace_back("int_col", TypeDescriptor::from_logical_type(TYPE_INT), true);
    slot_infos.emplace_back("bigint_col", TypeDescriptor::from_logical_type(TYPE_BIGINT), true);
    slot_infos.emplace_back("largeint_col", TypeDescriptor::from_logical_type(TYPE_LARGEINT), true);
    slot_infos.emplace_back("float_col", TypeDescriptor::from_logical_type(TYPE_FLOAT), true);
    slot_infos.emplace_back("double_col", TypeDescriptor::from_logical_type(TYPE_DOUBLE), true);
    slot_infos.emplace_back("boolean_col", TypeDescriptor::from_logical_type(TYPE_BOOLEAN), true);
    slot_infos.emplace_back("char_col", TypeDescriptor::from_logical_type(TYPE_CHAR), true);
    slot_infos.emplace_back("varchar_col", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
    slot_infos.emplace_back("string_col", TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
    slot_infos.emplace_back("date_col", TypeDescriptor::from_logical_type(TYPE_DATE), true);
    slot_infos.emplace_back("datetime_col", TypeDescriptor::from_logical_type(TYPE_DATETIME), true);

    // array info
    TypeDescriptor t_arr=TypeDescriptor::from_logical_type(TYPE_ARRAY);
    t_arr.children.emplace_back(TYPE_INT);
    slot_infos.emplace_back("array_col", t_arr, true);
    slot_infos.emplace_back("json_col", TypeDescriptor::from_logical_type(TYPE_JSON), true);

    std::unique_ptr<ArrowScanner> arrow_scanner = create_arrow_scanner(slot_infos, {}, ranges, params);
    arrow_scanner->setKMaxBufferSize(4 * 512 * 1024L);

    // 4. valid data
    ASSERT_OK(arrow_scanner->open());
    size_t read_rows = 0;
    while (true) {
        auto res = arrow_scanner->get_next();
        if (!res.ok() && res.status().is_end_of_file()) {
            ASSERT_EQ(num_rows, read_rows);
            break;
        }
        if (!res.ok()) {
            std::cout << "Unexpected status:" << res.status().to_string() << std::endl;
        }
        ChunkPtr chunk = res.value();
        if (chunk == nullptr) {
            ASSERT_EQ(num_rows, read_rows);
            break;
        }

        ASSERT_TRUE(chunk->num_rows() > 0);

        auto& columns = chunk->columns();
        int col_idx = 0;
        for (auto& col : columns) {
            ASSERT_TRUE(!col->is_nullable() || !col->is_constant());
            for (int i = 0; i < chunk->num_rows(); i++) {
                auto val = col->get(i);
                switch (col_idx) {
                    case 0:
                        ASSERT_EQ(val.get_int8(),tinyint_col[i+read_rows]);
                        break;
                    case 1:
                        ASSERT_EQ(val.get_int16(),smallint_col[i+read_rows]);
                        break;
                    case 2:
                        ASSERT_EQ(val.get_int32(),int_col[i+read_rows]);
                        break;
                    case 3:
                        ASSERT_EQ(val.get_int64(),bigint_col[i+read_rows]);
                        break;
                    case 4: {
                        int128_t int128_value = 0;
                        memcpy(&int128_value, largeint_col[i + read_rows].ToBytes().data(), 16);
                        ASSERT_EQ(val.get_int128(), int128_value);
                        break;
                    }
                    case 5:
                        ASSERT_EQ(val.get_float(),float_col[i+read_rows]);
                        break;
                    case 6:
                        ASSERT_EQ(val.get_double(),double_col[i+read_rows]);
                        break;
                    case 7:
                        ASSERT_EQ(val.get<bool>(),boolean_col[i+read_rows]);
                        break;
                    case 8:
                        ASSERT_EQ(0, strncmp(char_col[i+read_rows].c_str(), val.get_slice().data, val.get_slice().size));
                        break;
                    case 9:
                        ASSERT_EQ(0, strncmp(varchar_col[i+read_rows].c_str(), val.get_slice().data, val.get_slice().size));
                        break;
                    case 10:
                        ASSERT_EQ(0, strncmp(string_col[i+read_rows].c_str(), val.get_slice().data, val.get_slice().size));
                        break;
                    case 11:
                        ASSERT_EQ(val.get_date().to_date_literal(), date32_to_int(date_col[i+read_rows]));
                        break;
                    case 12:
                        ASSERT_EQ(val.get_timestamp().to_unix_second(), (datetime_col[i+read_rows]/1000000LL)+28800);
                        break;
                    case 13:
                        ASSERT_EQ(val.get_array().size(), array_col[i+read_rows].size());
                        break;
                    case 14: {
                        std::string s = val.get_json()->to_string().value();
                        s.erase(std::remove(s.begin(), s.end(), ' '), s.end()); // remove all white spaces
                        ASSERT_EQ(s, json_col[i + read_rows]);
                        break;
                    }
                    default:
                        FAIL();
                        break;
                }
            }
            col_idx++;
        }
        read_rows += chunk->num_rows();
    }
    ASSERT_GT(arrow_scanner->TEST_scanner_counter()->file_read_count, 0);
    ASSERT_GT(arrow_scanner->TEST_scanner_counter()->file_read_ns, 0);
    arrow_scanner->close();
}

}