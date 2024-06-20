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

import com.google.gson.Gson;
import com.starrocks.common.Config;
import com.starrocks.plugin.ProfileEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ProfileLogReader {
    private static final Logger LOG = LogManager.getLogger(ProfileLogReader.class);
    private static Gson GSON = new Gson();
    private static int BEGIN_INDEX = -1;
    private static String LOG_MESSAGE = "[profiletwo]";

    public static synchronized ProfileEvent getProfile(String queryId) {
        try {
            List<File> files = getProfileFiles();
            for (File file : files) {
                ProfileEvent event = getProfileFiles(file, queryId);
                if (event != null) {
                    return event;
                }
            }
            return null;
        } catch (Throwable e) {
            return null;
        }
    }

    public static ProfileEvent getProfileWithSupersqlTraceId(String supersqlTraceId) {
        return null;
    }

    private static List<File> getProfileFiles() {
        File directory = new File(Config.profile_log_dir);
        if (directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                List<File> list = new ArrayList<>();
                for (File file : files) {
                    if (file.getName().startsWith("fe.profiletwo.log")) {
                        list.add(file);
                    }
                }
                Collections.sort(list, new Comparator<File>() {
                    @Override
                    public int compare(File file1, File file2) {
                        return Long.compare(file2.lastModified(), file1.lastModified());
                    }
                });
                return list;
            } else {
                return Collections.emptyList();
            }
        } else {
            return Collections.emptyList();
        }
    }

    private static ProfileEvent getProfileFiles(File filePath, String queryId) {
        try (FileReader fileReader = new FileReader(filePath);
                BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                //2024-06-13 14:47:49.225+08:00 [profiletwo] {"type":"QUERY","queryId":"d9453a9f-2950-11ef-ad75-fa3a53ddd7b1","profile":""}
                if (BEGIN_INDEX <= 0) {
                    BEGIN_INDEX = line.indexOf(LOG_MESSAGE) + LOG_MESSAGE.length() + 1;
                }
                String profile = line.substring(BEGIN_INDEX);
                ProfileEvent profileEvent = GSON.fromJson(profile, ProfileEvent.class);
                if (profileEvent.getQueryId().equalsIgnoreCase(queryId)) {
                    return profileEvent;
                }
            }
        } catch (Throwable e) {
            LOG.error("Error reading file: " + e.getMessage());
        }
        return null;
    }

}
