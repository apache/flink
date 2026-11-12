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

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.testutils.TestingFileSystem;
import org.apache.flink.connector.file.src.testutils.TestingFileSystem.TestBlockLocation;
import org.apache.flink.connector.file.src.testutils.TestingFileSystem.TestFileStatus;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

/** Tests for opt-in discovery and its boundary with existing file enumerators. */
class GlobFileEnumeratorTest {

    @TempDir private java.nio.file.Path tempDir;

    private TrackingFileSystem testFs;

    @AfterEach
    void unregisterTestFs() throws Exception {
        if (testFs != null) {
            testFs.unregister();
        }
    }

    @Test
    void testSegmentsAndMatchedDirectoryRecursion() throws Exception {
        final Path first = file("partition-1/file-a.csv");
        final Path second = file("partition-2/file-b.csv");
        final Path nested = file("partition-1/nested/file-c.csv");
        file("partition-10/file-d.csv");
        file("archive/partition-3/file-e.csv");
        file("partition-1/file-a.txt");

        assertThat(paths(enumerate("partition-[0-9]/file-?.csv")))
                .containsExactlyInAnyOrder(first, second);
        assertThat(paths(enumerate("partition-1")))
                .containsExactlyInAnyOrder(first, nested, path("partition-1/file-a.txt"));
        assertThat(paths(enumerate("partition-?")))
                .containsExactlyInAnyOrder(first, second, nested, path("partition-1/file-a.txt"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"**/*.csv", "**/**/*.csv", "**/**/**/file-*.csv"})
    void testRecursiveWildcardMatchesZeroAndMultipleDirectories(String pattern) throws Exception {
        final Path direct = file("file-direct.csv");
        final Path nested = file("one/file-one.csv");
        final Path deep = file("one/two/file-two.csv");
        file("one/no.txt");
        assertThat(paths(enumerate(pattern))).containsExactlyInAnyOrder(direct, nested, deep);
    }

    @Test
    void testOverlappingInputsAreDeduplicatedBeforeDelegation() throws Exception {
        final Path first = file("part-1/a.csv");
        final Path second = file("part-1/nested/b.csv");
        final AtomicInteger calls = new AtomicInteger();
        final FileEnumerator delegate =
                (paths, hint) -> {
                    assertThat(paths).containsExactlyInAnyOrder(first, second);
                    assertThat(hint).isEqualTo(17);
                    calls.incrementAndGet();
                    return new NonSplittingRecursiveEnumerator().enumerateSplits(paths, hint);
                };
        final GlobFileEnumerator enumerator = new GlobFileEnumerator(delegate);
        assertThat(
                        paths(
                                enumerator.enumerateSplits(
                                        new Path[] {
                                            path("part-*"),
                                            path("**/*.csv"),
                                            path("part-1"),
                                            first,
                                            path("**")
                                        },
                                        17)))
                .containsExactlyInAnyOrder(first, second);
        assertThat(calls).hasValue(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"non-splitting", "block-splitting", "all-dir", "block-all-dir"})
    void testFileFilterNeverSeesIntermediateDirectories(String delegateType) throws Exception {
        final Path csv = file("part-1/data.csv");
        file("part-1/data.txt");
        file("part-1/_hidden.csv");
        file(".hidden/data.csv");

        final Map<Path, Integer> evaluations = new HashMap<>();
        final Predicate<Path> predicate =
                path -> {
                    assertThat(path.getName()).contains(".");
                    evaluations.merge(path, 1, Integer::sum);
                    return path.getName().endsWith(".csv") && new DefaultFileFilter().test(path);
                };
        final FileEnumerator delegate;
        switch (delegateType) {
            case "non-splitting":
                delegate = new NonSplittingRecursiveEnumerator(predicate);
                break;
            case "block-splitting":
                delegate = new BlockSplittingRecursiveEnumerator(predicate, new String[0]);
                break;
            case "all-dir":
                delegate = new NonSplittingRecursiveAllDirEnumerator(predicate);
                break;
            case "block-all-dir":
                delegate = new BlockSplittingRecursiveAllDirEnumerator(predicate, new String[0]);
                break;
            default:
                throw new IllegalArgumentException(delegateType);
        }

        assertThat(
                        paths(
                                new GlobFileEnumerator(delegate)
                                        .enumerateSplits(new Path[] {path("*/*")}, 1)))
                .containsExactly(csv);
        assertThat(evaluations).hasSize(3);
        assertThat(evaluations.values()).containsOnly(1);
    }

    @Test
    void testAllDirRegexDelegates() throws Exception {
        final Path csv = file("part-1/data.csv");
        file("part-1/data.txt");
        for (FileEnumerator delegate :
                Arrays.asList(
                        new NonSplittingRecursiveAllDirEnumerator(".*\\.csv"),
                        new BlockSplittingRecursiveAllDirEnumerator(".*\\.csv"))) {
            assertThat(
                            paths(
                                    new GlobFileEnumerator(delegate)
                                            .enumerateSplits(new Path[] {path("part-*/*")}, 1)))
                    .containsExactly(csv);
        }
    }

    @Test
    void testDirectoryPolicyIsSeparateAndEvaluatedOnce() throws Exception {
        final Path selected = file("part-1/data.csv");
        file("rejected/data.csv");
        file(".hidden/data.csv");
        final Map<Path, Integer> evaluations = new HashMap<>();
        final GlobFileEnumerator enumerator =
                new GlobFileEnumerator(
                        new NonSplittingRecursiveEnumerator(),
                        path -> {
                            evaluations.merge(path, 1, Integer::sum);
                            return !path.getName().equals("rejected")
                                    && new DefaultFileFilter().test(path);
                        });
        assertThat(
                        paths(
                                enumerator.enumerateSplits(
                                        new Path[] {path("**/*.csv"), path("*"), path("part-1")},
                                        1)))
                .containsExactly(selected);
        assertThat(evaluations.values()).containsOnly(1);
    }

    @Test
    void testLiteralSpecialCharactersAndQualifiedUri() throws Exception {
        final Path bracket = file("{a,b}/report[2026].csv");
        final Path star = file("{a,b}/foo*bar");
        final Path question = file("{a,b}/foo?bar");
        final Path closing = file("{a,b}/close].csv");
        final Path percent = file("{a,b}/space % #+.csv");
        file("{a,b}/report2.csv");

        assertThat(paths(enumerate("{a,b}/report[[]2026].csv"))).containsExactly(bracket);
        assertThat(paths(enumerate("{a,b}/foo[*]bar"))).containsExactly(star);
        assertThat(paths(enumerate("{a,b}/foo[?]bar"))).containsExactly(question);
        assertThat(paths(enumerate("{a,b}/close[]].csv"))).containsExactly(closing);
        assertThat(paths(enumerate("{a,b}/space % #+.*"))).containsExactly(percent);
        assertThat(
                        paths(
                                new GlobFileEnumerator(new NonSplittingRecursiveEnumerator())
                                        .enumerateSplits(
                                                new Path[] {
                                                    new Path(path("{a,b}/space % #+.*").toUri())
                                                },
                                                1)))
                .containsExactly(percent);
    }

    @Test
    void testRelativePathsAndTrailingSeparators() throws Exception {
        final Path csv = file("part-1/data.csv");
        final java.nio.file.Path working =
                java.nio.file.Path.of(
                        FileSystem.getLocalFileSystem().getWorkingDirectory().toUri());
        final String relative = working.relativize(tempDir).toString() + "/part-*";
        final GlobFileEnumerator enumerator =
                new GlobFileEnumerator(new NonSplittingRecursiveEnumerator());
        assertThat(paths(enumerator.enumerateSplits(new Path[] {new Path(relative)}, 1)))
                .containsExactly(csv);
        assertThat(paths(enumerate("part-*/"))).containsExactly(csv);
        assertThat(
                        paths(
                                enumerator.enumerateSplits(
                                        new Path[] {
                                            new Path(URI.create(tempDir.toUri() + "part-*/"))
                                        },
                                        1)))
                .containsExactly(csv);
    }

    @ParameterizedTest
    @ValueSource(strings = {"data.csv/**", "*.csv/**", "*.csv/**/**", "data.csv/**/*"})
    void testRecursiveWildcardCannotDescendThroughAFile(String pattern) throws Exception {
        file("data.csv");
        assertThat(enumerate(pattern)).isEmpty();
    }

    @Test
    void testMissingPrefixNoMatchesAndNonGlobMissingInput() throws Exception {
        file("part-1/data.csv");
        assertThat(enumerate("missing/*.csv")).isEmpty();
        assertThat(enumerate("part-*/missing/*.csv")).isEmpty();
        assertThat(enumerate("part-1/*.txt")).isEmpty();
        assertThatThrownBy(() -> enumerate("missing.csv"))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testAllPatternsValidatedBeforeAccessingFilesystem() {
        assertThatThrownBy(
                        () ->
                                new GlobFileEnumerator(new NonSplittingRecursiveEnumerator())
                                        .enumerateSplits(
                                                new Path[] {
                                                    new Path("unregistered:///first/*"),
                                                    new Path("unregistered:///second/bad[")
                                                },
                                                1))
                .isInstanceOf(java.util.regex.PatternSyntaxException.class);
    }

    @Test
    void testPrunesUnrelatedSubtrees() throws Exception {
        final Path selected = new Path("testglob:///dir/part-1/a.csv");
        registerFileSystem(
                TestFileStatus.forFileWithDefaultBlock(selected, 1),
                TestFileStatus.forFileWithDefaultBlock(
                        new Path("testglob:///dir/archive/deep/b.csv"), 1));
        final Path unrelated = new Path("testglob:///dir/archive");
        testFs.listingFailures.put(unrelated, new AccessDeniedException(unrelated.toString()));

        assertThat(paths(enumerateTestFs("/dir/part-*/*.csv"))).containsExactly(selected);
        assertThat(testFs.listedPaths)
                .containsExactly(new Path("testglob:///dir"), new Path("testglob:///dir/part-1"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "permission", "transport"})
    void testTraversalFailuresPropagate(String failure) throws Exception {
        final Path selected = new Path("testglob:///dir/part-1/a.csv");
        registerFileSystem(TestFileStatus.forFileWithDefaultBlock(selected, 1));
        final IOException error =
                failure.equals("missing")
                        ? new FileNotFoundException("directory disappeared")
                        : failure.equals("permission")
                                ? new AccessDeniedException("denied")
                                : new IOException("transport failed");
        testFs.listingFailures.put(new Path("testglob:///dir/part-1"), error);
        assertThatThrownBy(() -> enumerateTestFs("/dir/part-*/*.csv")).isSameAs(error);
    }

    @Test
    void testBlockLookupFailureDoesNotReturnPartialSuccess() throws Exception {
        registerFileSystem(
                TestFileStatus.forFileWithDefaultBlock(new Path("testglob:///dir/a.csv"), 1),
                TestFileStatus.forFileWithDefaultBlock(new Path("testglob:///dir/b.csv"), 1),
                TestFileStatus.forFileWithDefaultBlock(new Path("testglob:///dir/c.csv"), 1));
        final FileNotFoundException error = new FileNotFoundException("b.csv disappeared");
        testFs.blockFailure = error;
        assertThatThrownBy(() -> enumerateTestFs("/dir/*.csv")).isSameAs(error);
        assertThat(testFs.blockLookups)
                .containsExactly(
                        new Path("testglob:///dir/a.csv"), new Path("testglob:///dir/b.csv"));
    }

    @Test
    void testDelegateFailureDoesNotBecomeAnEmptyGlob() throws Exception {
        file("part-1/a.csv");
        file("part-1/b.csv");
        final FileNotFoundException error = new FileNotFoundException("selected file disappeared");
        final FileEnumerator delegate =
                (paths, hint) -> {
                    assertThat(paths).hasSize(2);
                    throw error;
                };
        assertThatThrownBy(
                        () ->
                                new GlobFileEnumerator(delegate)
                                        .enumerateSplits(new Path[] {path("part-*/*.csv")}, 1))
                .isSameAs(error);
    }

    @Test
    void testAllBlocksCompressionAndEmptyFilesArePreserved() throws Exception {
        final Path csv = new Path("testglob:///dir/data.csv");
        final Path compressed = new Path("testglob:///dir/data.gz");
        final Path empty = new Path("testglob:///dir/empty.csv");
        registerFileSystem(
                TestFileStatus.forFileWithBlocks(
                        csv, 1000, new TestBlockLocation(0, 400), new TestBlockLocation(400, 600)),
                TestFileStatus.forFileWithBlocks(
                        compressed,
                        1000,
                        new TestBlockLocation(0, 400),
                        new TestBlockLocation(400, 600)),
                TestFileStatus.forFileWithBlocks(empty, 0));
        final Collection<FileSourceSplit> splits =
                new GlobFileEnumerator(new BlockSplittingRecursiveEnumerator())
                        .enumerateSplits(
                                new Path[] {
                                    new Path("testglob:///dir/*"), new Path("testglob:///dir")
                                },
                                10);
        assertThat(splits)
                .extracting(FileSourceSplit::path, FileSourceSplit::offset, FileSourceSplit::length)
                .containsExactlyInAnyOrder(
                        tuple(csv, 0L, 400L),
                        tuple(csv, 400L, 600L),
                        tuple(compressed, 0L, 1000L),
                        tuple(empty, 0L, 0L));
        assertThat(splits).extracting(FileSourceSplit::splitId).doesNotHaveDuplicates();
    }

    @Test
    void testRepeatedDiscoveryUsesSameDelegateAndDoesNotCacheMissingPrefixes() throws Exception {
        final GlobFileEnumerator enumerator =
                new GlobFileEnumerator(new NonSplittingRecursiveEnumerator());
        final Path[] patterns = {path("new/part-*/*.csv")};
        assertThat(enumerator.enumerateSplits(patterns, 1)).isEmpty();
        final Path first = file("new/part-1/a.csv");
        final Collection<FileSourceSplit> before = enumerator.enumerateSplits(patterns, 1);
        assertThat(paths(before)).containsExactly(first);
        final Path second = file("new/part-2/b.csv");
        final Collection<FileSourceSplit> after = enumerator.enumerateSplits(patterns, 1);
        assertThat(paths(after)).containsExactlyInAnyOrder(first, second);
        final List<FileSourceSplit> all = new ArrayList<>(before);
        all.addAll(after);
        assertThat(all).extracting(FileSourceSplit::splitId).doesNotHaveDuplicates();
    }

    private Collection<FileSourceSplit> enumerate(String pattern) throws IOException {
        return new GlobFileEnumerator(new NonSplittingRecursiveEnumerator())
                .enumerateSplits(new Path[] {path(pattern)}, 1);
    }

    private Collection<FileSourceSplit> enumerateTestFs(String pattern) throws IOException {
        return new GlobFileEnumerator(new NonSplittingRecursiveEnumerator())
                .enumerateSplits(new Path[] {new Path("testglob://" + pattern)}, 1);
    }

    private void registerFileSystem(TestFileStatus... statuses) throws Exception {
        testFs =
                new TrackingFileSystem(TestingFileSystem.createForFileStatus("testglob", statuses));
        testFs.register();
    }

    private Path file(String name) throws IOException {
        final java.nio.file.Path file = tempDir.resolve(name);
        Files.createDirectories(file.getParent());
        Files.writeString(file, name);
        return new Path(file.toUri());
    }

    private Path path(String name) {
        return new Path(tempDir.resolve(name).toUri());
    }

    private static List<Path> paths(Collection<FileSourceSplit> splits) {
        final List<Path> paths = new ArrayList<>();
        for (FileSourceSplit split : splits) {
            paths.add(split.path());
        }
        return paths;
    }

    private static final class TrackingFileSystem extends TestingFileSystem {

        private final List<Path> listedPaths = new ArrayList<>();
        private final List<Path> blockLookups = new ArrayList<>();
        private final Map<Path, IOException> listingFailures = new HashMap<>();
        private IOException blockFailure;

        private TrackingFileSystem(TestingFileSystem template) {
            super(template);
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            listedPaths.add(path);
            final IOException failure = listingFailures.get(path);
            if (failure != null) {
                throw failure;
            }
            return super.listStatus(path);
        }

        @Override
        public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
                throws IOException {
            blockLookups.add(file.getPath());
            if (blockFailure != null && file.getPath().getName().equals("b.csv")) {
                throw blockFailure;
            }
            return super.getFileBlockLocations(file, start, len);
        }
    }
}
