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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This {@code FileEnumerator} enumerates all files under the given paths recursively. Each file
 * becomes one split; this enumerator does not split files into smaller "block" units.
 *
 * <p>The default instantiation of this enumerator filters files with the common hidden file
 * prefixes '.' and '_'. A custom file filter can be specified.
 */
@PublicEvolving
public class NonSplittingRecursiveEnumerator implements FileEnumerator {

    /** The filter predicate to filter out unwanted files. */
    protected final Predicate<Path> fileFilter;

    /**
     * The current Id as a mutable string representation. This covers more values than the integer
     * value range, so we should never overflow.
     */
    private final char[] currentId = "0000000000".toCharArray();

    /**
     * Creates a NonSplittingRecursiveEnumerator that enumerates all files except hidden files.
     * Hidden files are considered files where the filename starts with '.' or with '_'.
     */
    public NonSplittingRecursiveEnumerator() {
        this(new DefaultFileFilter());
    }

    /**
     * Creates a NonSplittingRecursiveEnumerator that uses the given predicate as a filter for file
     * paths.
     */
    public NonSplittingRecursiveEnumerator(Predicate<Path> fileFilter) {
        this.fileFilter = checkNotNull(fileFilter);
    }

    // ------------------------------------------------------------------------

    @Override
    public Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits)
            throws IOException {
        ForkJoinPool pool = null;
        try {
            pool = new ForkJoinPool(100);
            return pool.submit(
                            () ->
                                    Arrays.stream(paths)
                                            .parallel()
                                            .flatMap(this::getSplitsFromPath)
                                            .collect(Collectors.toList()))
                    .get();
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            if (pool != null) {
                pool.shutdown();
            }
        }
    }

    private Stream<FileSourceSplit> getSplitsFromPath(Path path) {
        try {
            final FileSystem fs = path.getFileSystem();
            final FileStatus status = fs.getFileStatus(path);
            ArrayList<FileSourceSplit> splits = new ArrayList<>();
            addSplitsForPath(status, fs, splits);
            return splits.stream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void addSplitsForPath(
            FileStatus fileStatus, FileSystem fs, ArrayList<FileSourceSplit> target)
            throws IOException {
        if (!fileFilter.test(fileStatus.getPath())) {
            return;
        }

        if (!fileStatus.isDir()) {
            convertToSourceSplits(fileStatus, fs, target);
            return;
        }

        target.addAll(
                Arrays.stream(fs.listStatus(fileStatus.getPath()))
                        .parallel()
                        .flatMap(
                                containedStatus -> {
                                    ArrayList<FileSourceSplit> splits = new ArrayList<>();
                                    try {
                                        addSplitsForPath(containedStatus, fs, splits);
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                    return splits.stream();
                                })
                        .collect(Collectors.toList()));
    }

    protected void convertToSourceSplits(
            final FileStatus file, final FileSystem fs, final List<FileSourceSplit> target)
            throws IOException {

        final String[] hosts =
                getHostsFromBlockLocations(fs.getFileBlockLocations(file, 0L, file.getLen()));
        target.add(
                new FileSourceSplit(
                        getNextId(),
                        file.getPath(),
                        0,
                        file.getLen(),
                        file.getModificationTime(),
                        file.getLen(),
                        hosts));
    }

    protected final String getNextId() {
        // because we just increment numbers, we increment the char representation directly,
        // rather than incrementing an integer and converting it to a string representation
        // every time again (requires quite some expensive conversion logic).
        incrementCharArrayByOne(currentId, currentId.length - 1);
        return new String(currentId);
    }

    private static String[] getHostsFromBlockLocations(BlockLocation[] blockLocations)
            throws IOException {
        if (blockLocations.length == 0) {
            return StringUtils.EMPTY_STRING_ARRAY;
        }
        if (blockLocations.length == 1) {
            return blockLocations[0].getHosts();
        }
        final LinkedHashSet<String> allHosts = new LinkedHashSet<>();
        for (BlockLocation block : blockLocations) {
            allHosts.addAll(Arrays.asList(block.getHosts()));
        }
        return allHosts.toArray(new String[allHosts.size()]);
    }

    private static void incrementCharArrayByOne(char[] array, int pos) {
        char c = array[pos];
        c++;

        if (c > '9') {
            c = '0';
            incrementCharArrayByOne(array, pos - 1);
        }
        array[pos] = c;
    }
}
