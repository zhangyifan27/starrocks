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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

import FormatStorage1.IRecord;

@SuppressWarnings("deprecation")
public class FormatStorageInputSplit extends FileSplit implements InputSplit {
  public static final Log LOG = LogFactory
          .getLog(FormatStorageInputSplit.class);
  boolean splitbyline;
  IRecord.IFValue beginkey;
  int recnum;
  boolean wholefileASasplit = false;

  public FormatStorageInputSplit() {
    super((Path) null, 0, 0, (String[]) null);
  }

  public FormatStorageInputSplit(Path filename, long length, int beginline,
                                 int recnum, String[] hosts) {
    super(filename, beginline, length, hosts);
    this.recnum = recnum;
    this.splitbyline = true;
  }

  public FormatStorageInputSplit(Path filename, long length, String[] hosts) {
    super(filename, 0, length, hosts);
    this.splitbyline = true;
    this.wholefileASasplit = true;
  }

  public FormatStorageInputSplit(Path filename, long length,
                                 IRecord.IFValue beginkey, int recnum, String[] hosts) {
    super(filename, 0, length, hosts);
    this.beginkey = beginkey;
    this.recnum = recnum;
    this.splitbyline = false;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.splitbyline = in.readBoolean();
    this.wholefileASasplit = in.readBoolean();
    if (splitbyline) {
      if (!wholefileASasplit) {
        this.recnum = in.readInt();
      }
    } else {
      this.beginkey = new IRecord.IFValue();
      beginkey.readFields(in);
      this.recnum = in.readInt();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeBoolean(splitbyline);
    out.writeBoolean(this.wholefileASasplit);
    if (splitbyline) {
      if (!wholefileASasplit) {
        out.writeInt(recnum);
      }
    } else {
      beginkey.write(out);
      out.writeInt(recnum);
    }
  }
}
