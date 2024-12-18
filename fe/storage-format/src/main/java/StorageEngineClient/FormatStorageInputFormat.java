/**
* Tencent is pleased to support the open source community by making TDW available.
* Copyright (C) 2014 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
* this file except in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed 
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
* OF ANY KIND, either express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
package StorageEngineClient;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;

import FormatStorage1.IRecord;

public class FormatStorageInputFormat<K, V> extends
        CombineFileInputFormat<LongWritable, IRecord> {
  public static final Log LOG = LogFactory
          .getLog(FormatStorageInputFormat.class);

  public FormatStorageInputFormat() {
    super();
  }

  @Override
  public RecordReader<LongWritable, IRecord> getRecordReader(InputSplit split,
                                                             JobConf job, Reporter reporter) throws IOException {
    if (split instanceof CombineFileSplit) {
      return new CombineFileRecordReader<LongWritable, IRecord>(job,
              (CombineFileSplit) split, reporter, FormatStorageIRecordReader.class);
    } else {
      FileSplit fileSplit = (FileSplit) split;
      Path[] paths = new Path[]{fileSplit.getPath()};
      long[] lengths = new long[]{fileSplit.getLength()};
      CombineFileSplit combineFileSplit = new CombineFileSplit(job, paths, lengths);
      return new CombineFileRecordReader<LongWritable, IRecord>(job,
              combineFileSplit, reporter, FormatStorageIRecordReader.class);
    }
  }
}
