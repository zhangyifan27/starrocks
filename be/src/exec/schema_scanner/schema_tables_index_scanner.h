// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <string>

#include "exec/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks {

class SchemaTablesIndexScanner : public SchemaScanner {
public:
    SchemaTablesIndexScanner();
    ~SchemaTablesIndexScanner() override;
    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    Status fill_chunk(ChunkPtr* chunk);

    int _tables_index_index = 0;
    TGetTablesIndexResponse _tables_index_response;

    static SchemaScanner::ColumnDesc _s_columns[];
};

} // namespace starrocks::vectorized
