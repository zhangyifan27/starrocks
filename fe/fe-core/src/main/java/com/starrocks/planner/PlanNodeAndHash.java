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

package com.starrocks.planner;

import com.starrocks.sql.optimizer.operator.Operator;

import java.util.Objects;
import java.util.Optional;

/**
 * PlanNode with plan hash.
 */
public class PlanNodeAndHash {
    private final Operator planNode;

    private final Optional<String> hash;

    public PlanNodeAndHash(Operator planNode, Optional<String> hash) {
        this.planNode = Objects.requireNonNull(planNode, "planNode is null");
        this.hash = Objects.requireNonNull(hash, "hash is null");
    }

    public Operator getPlanNode() {
        return planNode;
    }

    public Optional<String> getHash() {
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PlanNodeAndHash other = (PlanNodeAndHash) o;
        return planNode == other.planNode && Objects.equals(hash, other.hash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(System.identityHashCode(planNode), hash);
    }

    @Override
    public String toString() {
        return String.format("plan: %s, hash: %s", planNode, hash);
    }
}
