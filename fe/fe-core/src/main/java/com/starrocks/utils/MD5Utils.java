// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.utils;

import org.apache.commons.codec.binary.Hex;
import org.apache.parquet.Strings;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Utils {

    public static String computeChecksum(String fileUrl) throws IOException, NoSuchAlgorithmException {
        if (Strings.isNullOrEmpty(fileUrl)) {
            throw new RuntimeException("object file url is null");
        }
        URL url = new URL(fileUrl);
        URLConnection urlConnection = url.openConnection();
        return computeChecksum(urlConnection.getInputStream());
    }

    public static String computeChecksum(InputStream inputStream) throws NoSuchAlgorithmException, IOException {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        byte[] buf = new byte[4096];
        int bytesRead = 0;
        do {
            bytesRead = inputStream.read(buf);
            if (bytesRead < 0) {
                break;
            }
            digest.update(buf, 0, bytesRead);
        } while (true);

        return Hex.encodeHexString(digest.digest());
    }
}
