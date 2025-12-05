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

package com.starrocks.sql.optimizer.operator;

import com.starrocks.common.Id;
import com.starrocks.common.IdGenerator;

/**
 * relation id
 */
public class ObjectId extends Id<ObjectId> {

    public ObjectId(int id) {
        super(id);
    }

    public static IdGenerator<ObjectId> createGenerator() {
        return new IdGenerator<>() {
            @Override
            public ObjectId getNextId() {
                return new ObjectId(nextId++);
            }

            @Override
            public ObjectId getMaxId() {
                return new ObjectId(nextId - 1);
            }
        };
    }

    @Override
    public String toString() {
        return "ObjectId#" + id;
    }
}