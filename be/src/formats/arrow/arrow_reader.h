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

#include "util/slice.h"
#include "common/status.h"
#include "formats/csv/csv_reader.h"
#include "fs/fs.h"
#include "exec/file_scanner.h"
#include "runtime/runtime_state.h"

#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/io/api.h>
#include <arrow/record_batch.h>

namespace starrocks {
class ArrowReader {
public:
    using ArrowBuffer = CSVBuffer;
    using RecordBatch = arrow::RecordBatch;
    using RecordBatchPtr = std::shared_ptr<RecordBatch>;

    ArrowReader(std::shared_ptr<SequentialFile> file, RuntimeState* state, ScannerCounter* counter, size_t kMinBufferSize, size_t kMaxBufferSize)
    : _counter(counter), _storage(kMinBufferSize), _buff(_storage.data(), _storage.size()), _kMaxBufferSize(kMaxBufferSize) {
        _file = std::move(file);
        _state = state;
    }

    Status next_batch(RecordBatchPtr&);

private:
    std::shared_ptr<SequentialFile> _file;
    ScannerCounter* _counter = nullptr;
    RuntimeState* _state = nullptr;

    std::shared_ptr<arrow::io::BufferReader> _input;
    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> streamReader;

    raw::RawVector<char> _storage;
    ArrowBuffer _buff;
    int64_t _schema_offset;
    size_t _kMaxBufferSize;

    Status _fill_buffer();
    Status _get_next_buffer();
    Status _create_reader();
    Status _expand_buffer();
};

}