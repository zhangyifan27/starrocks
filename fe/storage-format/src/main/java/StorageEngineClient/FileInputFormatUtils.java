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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FileInputFormatUtils {
    public static final Log LOG = LogFactory.getLog(FileInputFormatUtils.class);

    private static final PathFilter hiddenFileFilter = new PathFilter() {
        public boolean accept(Path p) {
            String name = p.getName();
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };

    /**
     * Proxy PathFilter that accepts a path only if all filters given in the
     * constructor do. Used by the listPaths() to apply the built-in
     * hiddenFileFilter together with a user provided one (if any).
     */
    private static class MultiPathFilter implements PathFilter {
        private List<PathFilter> filters;

        public MultiPathFilter(List<PathFilter> filters) {
            this.filters = filters;
        }

        public boolean accept(Path path) {
            for (PathFilter filter : filters) {
                if (!filter.accept(path)) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Get the list of input {@link Path}s for the map-reduce job.
     *
     * @param conf The configuration of the job
     * @return the list of input {@link Path}s for the map-reduce job.
     */
    public static Path[] getInputPaths(JobConf conf) {
        String dirs = conf.get(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR, "");
        String[] list = StringUtils.split(dirs);
        Path[] result = new Path[list.length];
        for (int i = 0; i < list.length; i++) {
            result[i] = new Path(StringUtils.unEscapeString(list[i]));
        }
        return result;
    }

    /** List input directories.
     * Subclasses may override to, e.g., select only files matching a regular
     * expression.
     *
     * @param job the job to list input paths for
     * @return array of FileStatus objects
     * @throws IOException if zero items.
     */
    public static FileStatus[] listStatus(JobConf job) throws IOException {
        Path[] dirs = getInputPaths(job);
        if (dirs.length == 0) {
            throw new IOException("No input paths specified in job");
        }

        // Whether we need to recursive look into the directory structure
        boolean recursive = job.getBoolean(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR_RECURSIVE, false);

        // creates a MultiPathFilter with the hiddenFileFilter and the
        // user provided one (if any).
        List<PathFilter> filters = new ArrayList<PathFilter>();
        filters.add(hiddenFileFilter);
        PathFilter jobFilter = FileInputFormat.getInputPathFilter(job);
        if (jobFilter != null) {
            filters.add(jobFilter);
        }
        PathFilter inputFilter = new MultiPathFilter(filters);

        FileStatus[] result;

        StopWatch sw = new StopWatch().start();
        List<FileStatus> locatedFiles = singleThreadedListStatus(job, dirs, inputFilter, recursive);
        result = locatedFiles.toArray(new FileStatus[locatedFiles.size()]);

        sw.stop();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Time taken to get FileStatuses: " + sw.now(TimeUnit.MILLISECONDS));
        }
        LOG.debug("Total input files to process : " + result.length);
        return result;
    }

    private static List<FileStatus> singleThreadedListStatus(JobConf job, Path[] dirs,
                                                      PathFilter inputFilter, boolean recursive) throws IOException {
        List<FileStatus> result = new ArrayList<FileStatus>();
        List<IOException> errors = new ArrayList<IOException>();
        for (Path p : dirs) {
            FileSystem fs = p.getFileSystem(job);
            FileStatus[] matches = fs.globStatus(p, inputFilter);
            if (matches == null) {
                errors.add(new IOException("Input path does not exist: " + p));
            } else if (matches.length == 0) {
                errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
            } else {
                for (FileStatus globStat : matches) {
                    if (globStat.isDirectory()) {
                        RemoteIterator<FileStatus> iter = fs.listStatusIterator(globStat.getPath());
                        while (iter.hasNext()) {
                            FileStatus stat = iter.next();
                            if (inputFilter.accept(stat.getPath())) {
                                if (recursive && stat.isDirectory()) {
                                    addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
                                } else {
                                    result.add(stat);
                                }
                            }
                        }
                    } else {
                        result.add(globStat);
                    }
                }
            }
        }
        if (!errors.isEmpty()) {
            throw new InvalidInputException(errors);
        }
        return result;
    }

    /**
     * Add files in the input path recursively into the results.
     * @param result
     *          The List to store all files.
     * @param fs
     *          The FileSystem.
     * @param path
     *          The input path.
     * @param inputFilter
     *          The input filter that can be used to filter files/dirs.
     * @throws IOException
     */
    protected static void addInputPathRecursively(List<FileStatus> result,
                                           FileSystem fs, Path path, PathFilter inputFilter)
            throws IOException {
        RemoteIterator<FileStatus> iter = fs.listStatusIterator(path);
        while (iter.hasNext()) {
            FileStatus stat = iter.next();
            if (inputFilter.accept(stat.getPath())) {
                if (stat.isDirectory()) {
                    addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
                } else {
                    result.add(stat);
                }
            }
        }
    }
}
