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
#include "column/column_helper.h"
#include "column/adaptive_nullable_column.h"
#include "column/chunk.h"
#include "formats/csv/converter.h"
#include "parquet_scanner.h"

namespace starrocks {

ArrowScanner::ArrowScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
        ScannerCounter* counter, bool schema_only)
        : ParquetScanner(state, profile, scan_range, counter, schema_only) {}

Status ArrowScanner::open() {
    RETURN_IF_ERROR(ParquetScanner::open());
    return Status::OK();
}

StatusOr<ChunkPtr> ArrowScanner::get_next() {
    SCOPED_RAW_TIMER(&_counter->total_ns);

    Status st = Status::OK();
    ChunkPtr chunk = _try_to_create_chunk();
    while (true) {
        if (batch_is_exhausted()) {
            st = next_batch();
            if (_scanner_eof) {
                // all scan ranges has been finished
                break;
            }
            if (!st.ok()) {
                // something wrong happens
                return st;
            }

            RETURN_IF_ERROR(_create_chunk(chunk));
        }

        DCHECK(chunk != nullptr);
        DCHECK(_batch != nullptr);
        DCHECK(_batch->num_rows() > 0);

        RETURN_IF_ERROR(append_batch_to_src_chunk(&chunk));

        if (chunk_is_full()) {
            break;
        }
    }

    if (_chunk_start_idx > 0) {
        RETURN_IF_ERROR(finalize_src_chunk(&chunk));
        return std::move(chunk);
    } else {
        return st;
    }
}

ChunkPtr ArrowScanner::_try_to_create_chunk() {
    SCOPED_RAW_TIMER(&_counter->init_chunk_ns);
    if (_src_chunk != nullptr) {
        _chunk_filter.clear();
        return _src_chunk->clone_empty_with_slot();
    }
    return nullptr;
}

/**
 * Notice: This method should be invoked after _batch is initialized.
 */
Status ArrowScanner::_create_chunk(ChunkPtr& chunk) {
    DCHECK(_batch != nullptr);

    if (_src_chunk == nullptr) {
        // use batch data to initialize chunk
        RETURN_IF_ERROR(initialize_src_chunk(&_src_chunk));
        DCHECK(_src_chunk != nullptr);

        chunk = _try_to_create_chunk();
    }
    return Status::OK();
}

Status ArrowScanner::next_batch() {
    Status st = Status::OK();

    while (true) {
        // current scan range ends, then read next scan range
        if (_reader == nullptr) {
            st = open_next_reader();
            if (!st.ok()) {
                // maybe finish reading all scan ranges or something wrong happens
                return st;
            }
        }

        DCHECK(_reader != nullptr);

        // read next arrow batch data of current scan range
        st = _reader->next_batch(_batch);
        if (st.is_end_of_file()) {
            // finish reading current scan range, then read from next scan range
            _reader.reset();
            continue;
        } else if (!st.ok()) {
            // something wrong happens
            return st;
        }
        _batch_start_idx = 0;

        return st;
    }
}

/** Open reader with next scan range. */
Status ArrowScanner::open_next_reader() {
    SCOPED_RAW_TIMER(&_counter->open_reader_ns);
    if (_next_file >= _scan_range.ranges.size()) {
        _scanner_eof = true;
        return Status::EndOfFile("eof");
    }
    std::shared_ptr<SequentialFile> file;
    const TBrokerRangeDesc& range_desc = _scan_range.ranges[_next_file];
    Status st = create_sequential_file(range_desc, _scan_range.broker_addresses[0], _scan_range.params, &file);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to create sequential files: " << st.to_string();
        return st;
    }
    _reader = std::make_unique<ArrowReader>(file, _state, _counter, _kMinBufferSize, _kMaxBufferSize);

    _next_file++;
    // reset _src_chunk, because the chunk schema may be different between scan ranges
    _src_chunk.reset();
    return Status::OK();
}

void ArrowScanner::setKMinBufferSize(size_t kMinBufferSize) {
    _kMinBufferSize = kMinBufferSize;
}

void ArrowScanner::setKMaxBufferSize(size_t kMaxBufferSize) {
    _kMaxBufferSize = kMaxBufferSize;
}


}