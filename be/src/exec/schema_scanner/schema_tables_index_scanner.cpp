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

#include "exec/schema_scanner/schema_tables_index_scanner.h"

#include <fmt/format.h>

#include "common/logging.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

    SchemaScanner::ColumnDesc SchemaTablesIndexScanner::_s_columns[] = {
            //   name,       type,          size,     is_null
            {"DB_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
            {"TABLE_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
            {"INDEX_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
            {"COLUMN_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
            {"INDEX_TYPE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
            {"COMMENT", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
    };

    SchemaTablesIndexScanner::SchemaTablesIndexScanner()
            : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

    SchemaTablesIndexScanner::~SchemaTablesIndexScanner() = default;

    Status SchemaTablesIndexScanner::start(RuntimeState* state) {
        if (!_is_init) {
            return Status::InternalError("used before initialized.");
        }
        TAuthInfo auth_info;
        if (nullptr != _param->db) {
            auth_info.__set_pattern(*(_param->db));
        }
        if (nullptr != _param->current_user_ident) {
            auth_info.__set_current_user_ident(*(_param->current_user_ident));
        } else {
            if (nullptr != _param->user) {
                auth_info.__set_user(*(_param->user));
            }
            if (nullptr != _param->user_ip) {
                auth_info.__set_user_ip(*(_param->user_ip));
            }
        }
        TGetTablesIndexRequest tables_index_req;
        tables_index_req.__set_auth_info(auth_info);

        if (nullptr != _param->ip && 0 != _param->port) {
            int timeout_ms = state->query_options().query_timeout * 1000;
            RETURN_IF_ERROR(SchemaHelper::get_tables_index(*(_param->ip), _param->port, tables_index_req,
                                                              &_tables_index_response, timeout_ms));
        } else {
            return Status::InternalError("IP or port doesn't exists");
        }
        _tables_index_index = 0;
        return Status::OK();
    }

    Status SchemaTablesIndexScanner::get_next(ChunkPtr* chunk, bool* eos) {
        if (!_is_init) {
            return Status::InternalError("Used before initialized.");
        }
        if (nullptr == chunk || nullptr == eos) {
            return Status::InternalError("input pointer is nullptr.");
        }
        if (_tables_index_index >= _tables_index_response.tables_index.size()) {
            *eos = true;
            return Status::OK();
        }
        *eos = false;
        return fill_chunk(chunk);
    }

    Status SchemaTablesIndexScanner::fill_chunk(ChunkPtr* chunk) {
        const TTableIndexInfo& info = _tables_index_response.tables_index[_tables_index_index];
        const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            if (slot_id < 1 || slot_id > 25) {
                return Status::InternalError(fmt::format("invalid slot id:{}", slot_id));
            }
            ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);

            switch (slot_id) {
                case 1: {
                    // DB_NAME
                    Slice db_name = Slice(info.db_name);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&db_name);
                    break;
                }
                case 2: {
                    // TABLE_NAME
                    Slice table_name = Slice(info.table_name);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&table_name);
                    break;
                }
                case 3: {
                    // INDEX_NAME
                    Slice partition_name = Slice(info.index_name);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&partition_name);
                    break;
                }
                case 4: {
                    // COLUMN
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&info.column_name);
                    break;
                }
                case 5: {
                    // INDEX_TYPE
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&info.index_type);
                    break;
                }
                case 6: {
                    // COMMENT
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&info.comment);
                    break;
                }
                default:
                    break;
            }
        }
        _tables_index_index++;
        return Status::OK();
    }

} // namespace starrocks