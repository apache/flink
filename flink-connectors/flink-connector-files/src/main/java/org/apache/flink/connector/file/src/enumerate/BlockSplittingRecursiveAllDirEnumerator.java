/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.file.src.enumerate;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.compression.StandardDeCompressors;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.function.Predicate;

/**
 * This {@code FileEnumerator} enumerates all files under the given paths recursively except the
 * hidden directories, and creates a separate split for each file block.
 *
 * <p>Please note that file blocks are only exposed by some file systems, such as HDFS. File systems
 * that do not expose block information will not create multiple file splits per file, but keep the
 * files as one source split.
 *
 * <p>Files with suffixes corresponding to known compression formats (for example '.gzip', '.bz2',
 * ...) will not be split. See {@link StandardDeCompressors} for a list of known formats and
 * suffixes.
 *
 * <p>Compared to {@link BlockSplittingRecursiveEnumerator}, this enumerator will enumerate all
 * files even through its parent directory is filtered out by the file filter.
 */
@Internal
public class BlockSplittingRecursiveAllDirEnumerator extends BlockSplittingRecursiveEnumerator {

    /** The filter used to skip hidden directories. */
    private final DefaultFileFilter hiddenDirFilter = new DefaultFileFilter();

    /**
     * Creates a new enumerator that enumerates all files whose file path matches the regex except
     * hidden files. Hidden files are considered files where the filename starts with '.' or with
     * '_'.
     *
     * <p>The enumerator does not split files that have a suffix corresponding to a known
     * compression format (for example '.gzip', '.bz2', '.xy', '.zip', ...). See {@link
     * StandardDeCompressors} for details.
     */
    public BlockSplittingRecursiveAllDirEnumerator(String pathPattern) {
        this(
                new RegexFileFilter(pathPattern),
                StandardDeCompressors.getCommonSuffixes().toArray(new String[0]));
    }

    /**
     * Creates a new enumerator that uses the given predicate as a filter for file paths, and avoids
     * splitting files with the given extension (typically to avoid splitting compressed files).
     */
    public BlockSplittingRecursiveAllDirEnumerator(
            final Predicate<Path> fileFilter, final String[] nonSplittableFileSuffixes) {
        super(fileFilter, nonSplittableFileSuffixes);
    }

    @Override
    protected void addSplitsForPath(
            FileStatus fileStatus, FileSystem fs, ArrayList<FileSourceSplit> target)
            throws IOException {
        if (fileStatus.isDir()) {
            if (!hiddenDirFilter.test(fileStatus.getPath())) {
                return;
            }
            final FileStatus[] containedFiles = fs.listStatus(fileStatus.getPath());
            for (FileStatus containedStatus : containedFiles) {
                addSplitsForPath(containedStatus, fs, target);
            }
        } else if (fileFilter.test(fileStatus.getPath())) {
            convertToSourceSplits(fileStatus, fs, target);
            return;
        }
    }
}
