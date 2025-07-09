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

package com.starrocks.common.util;

import com.starrocks.qe.SessionVariable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RoundRobin<K, N> implements HashRing<K, N> {
    private List<N> nodes = new ArrayList<>();
    private int index = 0;

    public RoundRobin(Collection<N> nodes, long deployedScanRangeOffset) {
        for (N node : nodes) {
            addNode(node);
        }
        if (!nodes.isEmpty()) {
            this.index = (int) (deployedScanRangeOffset % nodes.size());
        }
    }

    @Override
    public void addNode(N node) {
        nodes.add(node);
    }

    @Override
    public void removeNode(N node) {
        nodes.remove(node);
    }

    @Override
    public List<N> get(K key, int distinctNumber) {
        List<N> ans = new ArrayList<>();
        if (nodes.isEmpty()) {
            return ans;
        }
        // Not support rebalance in round-robin, always return one node.
        ans.add(nodes.get(index++));
        if (index >= nodes.size()) {
            index = 0;
        }
        return ans;
    }

    @Override
    public String policy() {
        return SessionVariable.BackendSelectorHashAlgorithm.ROUNDROBIN;
    }
}
