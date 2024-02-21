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
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.function.Predicate;

/**
 * This {@code FileEnumerator} enumerates all files under the given paths recursively except the
 * hidden directories. Each file matched the given regex pattern becomes one split; this enumerator
 * does not split files into smaller "block" units.
 *
 * <p>The default instantiation of this enumerator filters files with the common hidden file
 * prefixes '.' and '_'. A custom file filter can be specified.
 *
 * <p>Compared to {@link NonSplittingRecursiveEnumerator}, this enumerator will enumerate all files
 * even through its parent directory is filtered out by the file filter.
 */
@Internal
public class NonSplittingRecursiveAllDirEnumerator extends NonSplittingRecursiveEnumerator {
    /** The filter used to skip hidden directories. */
    private final DefaultFileFilter hiddenDirFilter = new DefaultFileFilter();

    /**
     * Creates a NonSplittingRegexEnumerator that enumerates all files whose file path matches the
     * regex except hidden files. Hidden files are considered files where the filename starts with
     * '.' or with '_'.
     */
    public NonSplittingRecursiveAllDirEnumerator(String pathRegexPattern) {
        this(new RegexFileFilter(pathRegexPattern));
    }

    /**
     * Creates a NonSplittingRegexEnumerator that enumerates all files whose file path matches the
     * regex. Support to use given custom predicate as a filter for file paths.
     */
    public NonSplittingRecursiveAllDirEnumerator(Predicate<Path> fileFilter) {
        super(fileFilter);
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
