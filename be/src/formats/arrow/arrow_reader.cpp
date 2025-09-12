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

#include "arrow_reader.h"
#include "common/logging.h"

namespace starrocks {
    Status ArrowReader::next_batch(RecordBatchPtr& recordBatch){
        SCOPED_RAW_TIMER(&_counter->read_batch_ns);
        // read one batch from buffer
        arrow::Status st = arrow::Status::Invalid("StreamReader Not initialized");
        do {
            // 1. create arrow stream reader
            RETURN_IF_ERROR(_create_reader());

            // the complete schema information has been read, and the streamReader is successfully created
            DCHECK(streamReader != nullptr);

            // 2. use arrow stream reader to get next record batch
            st = streamReader->ReadNext(&recordBatch);
            if (!st.ok()) {
                // Not enough data for one record batch, get next buffer from pipeline to re-create stream reader
                streamReader.reset();
            }
        } while (!st.ok());
        // read one record batch successfully or EOF

        if (!recordBatch) {
            // no more data
            return Status::EndOfFile("No more arrow stream data of the current scan range.");
        }

        int64_t cur_consumed_idx = _input->Tell().ValueOrDie();
        _buff.set_position_offset(cur_consumed_idx); // erase the consumed bytes

        return Status::OK();
    }

    /**
     * To create a stream reader, it is necessary to ensure that a complete schema is available.
     */
    Status ArrowReader::_create_reader() {
        while (streamReader == nullptr) {
            // 1. get next buffer from pipeline to fill _buff
            RETURN_IF_ERROR(_get_next_buffer());

            // 2. create stream reader with _buff
            ++_counter->create_reader_count;
            SCOPED_RAW_TIMER(&_counter->create_reader_ns);
            auto arrow_buffer = arrow::Buffer::Wrap(
                    reinterpret_cast<const uint8_t*>(_buff.base_ptr()), _buff.limit_offset());
            _input = std::make_shared<arrow::io::BufferReader>(arrow_buffer);
            arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchStreamReader>> res =
                    arrow::ipc::RecordBatchStreamReader::Open(_input);

            if (!res.ok()) {
                std::string msg = res.status().ToString();
                if (msg.find("only read") != std::string::npos) {
                    // schema is large, and there is no enough data to read the complete schema yet.
                    // waiting for the next batch of data
                    continue;
                } else {
                    return Status::RuntimeError("Failed to open arrow RecordBatchStreamReader " + msg);
                }
            }
            streamReader = res.ValueOrDie();

            _schema_offset = _input->Tell().ValueOrDie();
        }
        return Status::OK();
    }

    Status ArrowReader::_get_next_buffer() {
        if (_buff.position_offset() > _schema_offset) {
            // need to retain the schema part for creating stream reader later
            SCOPED_RAW_TIMER(&_counter->compact_buffer_ns);
            _buff.compact(_schema_offset);
        }
        if (_buff.free_space() == 0) {
            RETURN_IF_ERROR(_expand_buffer());
        }
        RETURN_IF_ERROR(_fill_buffer());
        return Status::OK();
    }

    // read data buffer from pipe
    Status ArrowReader::_fill_buffer() {
        ++_counter->file_read_count;
        SCOPED_RAW_TIMER(&_counter->file_read_ns);

        DCHECK(_buff.free_space() > 0);
        Slice s(_buff.limit(), _buff.free_space());
        auto res = _file->read(s.data, s.size);

        // According to the specification of `FileSystem::read`, when reached the end of
        // a file, the returned status will be OK instead of EOF, but here we check
        // EOF also for safety.
        if (res.status().is_end_of_file()) {
            s.size = 0;
        } else if (!res.ok()) {
            return res.status();
        } else {
            s.size = *res;
        }
        _buff.add_limit(s.size);

        if (s.size == 0) {
            // Has reached the end of file and the buffer is empty.
            return Status::EndOfFile(_file->filename());
        } else {
            _state->update_num_bytes_scan_from_source(s.size);
        }
        return Status::OK();
    }

    Status ArrowReader::_expand_buffer() {
        SCOPED_RAW_TIMER(&_counter->expand_buffer_ns);
        if (UNLIKELY(_storage.size() >= _kMaxBufferSize)) {
            return Status::InternalError("Arrow batch record exceed limit " + std::to_string(_kMaxBufferSize));
        }
        size_t new_capacity = std::min(_storage.size() * 2, _kMaxBufferSize);

        // compact buffer before expand
        DCHECK_EQ(_storage.data(), _buff.position()) << "should compact buffer before expand";
        _storage.resize(new_capacity);
        ArrowBuffer new_buff(_storage.data(), _storage.size());
        new_buff.add_limit(_buff.available());
        DCHECK_EQ(_storage.data(), new_buff.position());
        DCHECK_EQ(_buff.available(), new_buff.available());
        _buff = new_buff;
        return Status::OK();
    }
}

