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

package com.starrocks.connector;

import StorageEngineClient.CombineFileSplit;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

public class StorageFormatRemoteFileDesc extends RemoteFileDesc {
    private List<CombineFileSplit> storageFormatSplitsInfo = new ArrayList<CombineFileSplit>();

    public StorageFormatRemoteFileDesc(String fileName, String compression, long length, long modificationTime,
                                       ImmutableList<RemoteFileBlockDesc> blockDescs,
                                       List<CombineFileSplit> storageFormatSplitsInfo) {
        super(fileName, compression, length, modificationTime, blockDescs);
        this.storageFormatSplitsInfo = storageFormatSplitsInfo;
    }

    public static RemoteFileDesc createStorageFormatRemoteFileDesc(long totLength,
                                                                   List<CombineFileSplit> storageFormatSplitsInfo) {
        return new StorageFormatRemoteFileDesc(null, null, totLength, 0, null, storageFormatSplitsInfo);
    }

    public List<CombineFileSplit> getStorageFormatSplitsInfo() {
        return storageFormatSplitsInfo;
    }
}

