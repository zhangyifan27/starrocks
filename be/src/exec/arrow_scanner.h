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

#include "exec/file_scanner.h"
#include "formats/arrow/arrow_reader.h"
#include "formats/csv/converter.h"
#include "exec/arrow_to_starrocks_converter.h"
#include "parquet_scanner.h"

#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>

namespace starrocks {

    class ArrowScanner : public ParquetScanner {
    public:
        ArrowScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                ScannerCounter* counter, bool schema_only = false);

        // Open this scanner, will initialize information needed
        Status open() override;

        StatusOr<ChunkPtr> get_next() override;

        void setKMinBufferSize(size_t kMinBufferSize);

        void setKMaxBufferSize(size_t kMaxBufferSize);


    private:
        using ArrowReaderPtr = std::unique_ptr<ArrowReader>;

        ArrowReaderPtr _reader;
        // empty chunk with schema, used to clone chunk quickly in get_next
        ChunkPtr _src_chunk;

        size_t _kMinBufferSize = 8 * 1024 * 1024L;
        size_t _kMaxBufferSize = config::arrow_reader_buffer_max_size;

        ChunkPtr _try_to_create_chunk();
        Status _create_chunk(ChunkPtr& chunk);
        Status open_next_reader();
        Status next_batch();
    };

}