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
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A file enumerator that explicitly enables glob expansion in FileSource input paths.
 *
 * <p>Within a path segment, {@code *} matches zero or more characters, {@code ?} matches one
 * character, and character classes such as {@code [abc]}, {@code [a-z]}, and {@code [!a-z]} match
 * one character. A segment consisting of {@code **} matches zero or more directory levels. Matching
 * is case-sensitive and independent of the local operating system. Braces are literal; use {@code
 * [*]}, {@code [?]}, {@code [[]}, and {@code []]} for literal glob characters. Backslash escaping
 * is not supported because {@link Path} normalizes backslashes to separators.
 *
 * <p>This enumerator owns directory traversal, including recursive traversal of matched
 * directories. It passes unique, qualified <em>file</em> paths to the supplied delegate once per
 * enumeration, preserving the parallelism hint and all returned splits. The delegate controls file
 * filtering and splitting; it does not receive directories. Custom directory filters and
 * directory-specific subclass hooks on the delegate therefore do not govern glob traversal. Use the
 * separate directory filter to control traversal instead.
 *
 * <p>Enumeration starts at each input's longest prefix without glob syntax. Missing glob prefixes
 * and patterns with no matches yield no files. Missing non-glob inputs, listing failures, and
 * failures in the delegate are propagated. No discovery results are cached between invocations.
 * Broad patterns, especially {@code **}, may require listing large directory trees.
 *
 * <p>Install this enumerator explicitly through the file source builder's {@code setFileEnumerator}
 * method. Choose a delegate appropriate for the format, for example {@link
 * NonSplittingRecursiveEnumerator} for a non-splittable format. Existing file enumerators do not
 * expand globs.
 */
@PublicEvolving
public final class GlobFileEnumerator implements FileEnumerator {

    private final FileEnumerator delegate;
    private final Predicate<Path> directoryFilter;

    /**
     * Creates an enumerator that skips directories whose names start with '.' or '_'.
     *
     * @param delegate enumerator receiving concrete files for filtering and splitting
     */
    @PublicEvolving
    public GlobFileEnumerator(FileEnumerator delegate) {
        this(delegate, new DefaultFileFilter());
    }

    /**
     * Creates an enumerator with an explicit directory traversal policy.
     *
     * <p>The predicate is applied once per visited directory per invocation, starting at each
     * input's fixed prefix. Rejected directories are not listed. Ancestors above that prefix are
     * not visited. Hidden-file filtering remains the delegate's responsibility.
     *
     * @param delegate enumerator receiving concrete files, never directories
     * @param directoryFilter predicate accepting directories to traverse
     */
    @PublicEvolving
    public GlobFileEnumerator(FileEnumerator delegate, Predicate<Path> directoryFilter) {
        this.delegate = checkNotNull(delegate);
        this.directoryFilter = checkNotNull(directoryFilter);
    }

    @Override
    @PublicEvolving
    public Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits)
            throws IOException {
        // Validate all patterns before touching any filesystem.
        final List<GlobPattern> patterns = new ArrayList<>(paths.length);
        for (Path path : paths) {
            patterns.add(new GlobPattern(checkNotNull(path)));
        }

        final Discovery discovery = new Discovery();
        for (GlobPattern pattern : patterns) {
            final FileSystem fs = pattern.getRoot().getFileSystem();
            final Path root = pattern.getRoot().makeQualified(fs);
            final FileStatus status;
            try {
                status = fs.getFileStatus(root);
            } catch (FileNotFoundException e) {
                if (pattern.hasGlob()) {
                    continue;
                }
                throw e;
            }
            // Even ** cannot consume a path separator after a regular file.
            if (status.isDir() || !pattern.hasGlob()) {
                discovery.expand(fs, status, pattern, pattern.initialPositions());
            }
        }
        return delegate.enumerateSplits(discovery.files.toArray(new Path[0]), minDesiredSplits);
    }

    /** Per-invocation state; continuous discovery must also see files created in later calls. */
    private final class Discovery {

        private final Set<Path> files = new LinkedHashSet<>();
        private final Set<Path> collectedDirectories = new HashSet<>();
        private final Map<Path, Boolean> acceptedDirectories = new HashMap<>();

        private void expand(
                FileSystem fs, FileStatus status, GlobPattern pattern, Set<Integer> positions)
                throws IOException {
            final Path path = status.getPath().makeQualified(fs);
            if (status.isDir() && !acceptDirectory(path)) {
                return;
            }
            if (pattern.isComplete(positions)) {
                collectFiles(fs, status);
            } else if (status.isDir() && !collectedDirectories.contains(path)) {
                for (FileStatus child : fs.listStatus(path)) {
                    final Set<Integer> next = pattern.matchChild(child, positions);
                    if (!next.isEmpty()) {
                        expand(fs, child, pattern, next);
                    }
                }
            }
        }

        private void collectFiles(FileSystem fs, FileStatus status) throws IOException {
            final Path path = status.getPath().makeQualified(fs);
            if (!status.isDir()) {
                files.add(path);
            } else if (acceptDirectory(path) && collectedDirectories.add(path)) {
                for (FileStatus child : fs.listStatus(path)) {
                    collectFiles(fs, child);
                }
            }
        }

        private boolean acceptDirectory(Path path) {
            return acceptedDirectories.computeIfAbsent(path, directoryFilter::test);
        }
    }
}
