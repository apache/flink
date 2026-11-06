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

package org.apache.flink.fs.azurefs;

import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.PathProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link TestingDataLakeStorageOperations}.
 *
 * <p>Validates that the test double correctly constructs SDK types ({@link PathProperties}, {@link
 * PathItem}) whose constructors are fragile and could break on dependency upgrades.
 */
class TestingDataLakeStorageOperationsTest {

    private static final OffsetDateTime TEST_TIME =
            OffsetDateTime.of(2026, 1, 15, 10, 30, 0, 0, ZoneOffset.UTC);

    // --- PathProperties construction (26-param SDK constructor) ---

    static Stream<Arguments> pathPropertiesCases() {
        return Stream.of(
                // isDirectory, contentSize, lastModified, expectedSize
                Arguments.of(false, 42, TEST_TIME, 42L),
                Arguments.of(false, 10, TEST_TIME, 10L),
                Arguments.of(true, 0, TEST_TIME, 0L),
                Arguments.of(false, 5, null, 5L));
    }

    @ParameterizedTest
    @MethodSource("pathPropertiesCases")
    void shouldConstructPathPropertiesFromSdkConstructor(
            final boolean isDirectory,
            final int contentSize,
            @Nullable final OffsetDateTime lastModified,
            final long expectedSize) {
        final TestingDataLakeStorageOperations ops = new TestingDataLakeStorageOperations();
        if (isDirectory) {
            ops.addDirectory("path", lastModified);
        } else {
            ops.addFile("path", new byte[contentSize], lastModified);
        }

        final PathProperties props = ops.getFileClient("path").getProperties();

        assertThat(props.isDirectory()).isEqualTo(isDirectory);
        assertThat(props.getFileSize()).isEqualTo(expectedSize);
        assertThat(props.getLastModified()).isEqualTo(lastModified);
    }

    // --- PathItem construction ---

    static Stream<Arguments> pathItemCases() {
        return Stream.of(
                // isDirectory, contentSize, expectedSize
                Arguments.of(false, 100, 100L), Arguments.of(true, 0, 0L));
    }

    @ParameterizedTest
    @MethodSource("pathItemCases")
    void shouldConstructPathItemFromSdkConstructor(
            final boolean isDirectory, final int contentSize, final long expectedSize) {
        final TestingDataLakeStorageOperations ops = new TestingDataLakeStorageOperations();
        final String parent = "parent";
        final String child = parent + "/entry";
        if (isDirectory) {
            ops.addDirectory(child, TEST_TIME);
        } else {
            ops.addFile(child, new byte[contentSize], TEST_TIME);
        }

        final ListPathsOptions options = new ListPathsOptions().setPath(parent).setRecursive(false);
        final List<PathItem> items = toList(ops.listPaths(options, null));

        assertThat(items).hasSize(1);
        final PathItem item = items.get(0);
        assertThat(item.getName()).isEqualTo(child);
        assertThat(item.isDirectory()).isEqualTo(isDirectory);
        assertThat(item.getContentLength()).isEqualTo(expectedSize);
        assertThat(item.getLastModified()).isEqualTo(TEST_TIME);
    }

    // --- Error injection ---

    static Stream<Arguments> errorInjectionCases() {
        return Stream.of(
                // injector, trigger, expectedStatusCode
                Arguments.of(
                        (ErrorInjector) TestingDataLakeStorageOperations::injectError,
                        (ErrorTrigger) (ops, path) -> ops.getFileClient(path).getProperties(),
                        503),
                Arguments.of(
                        (ErrorInjector) TestingDataLakeStorageOperations::injectDeleteError,
                        (ErrorTrigger) (ops, path) -> ops.getFileClient(path).delete(),
                        403),
                Arguments.of(
                        (ErrorInjector) TestingDataLakeStorageOperations::injectDeleteError,
                        (ErrorTrigger) (ops, path) -> ops.getDirectoryClient(path).delete(),
                        403),
                Arguments.of(
                        (ErrorInjector) TestingDataLakeStorageOperations::injectDeleteError,
                        (ErrorTrigger)
                                (ops, path) -> ops.getDirectoryClient(path).deleteRecursively(),
                        403),
                Arguments.of(
                        (ErrorInjector) TestingDataLakeStorageOperations::injectListError,
                        (ErrorTrigger)
                                (ops, path) ->
                                        ops.listPaths(
                                                new ListPathsOptions()
                                                        .setPath(path)
                                                        .setRecursive(false),
                                                null),
                        500));
    }

    @ParameterizedTest
    @MethodSource("errorInjectionCases")
    void shouldThrowInjectedError(
            final ErrorInjector injector,
            final ErrorTrigger trigger,
            final int expectedStatusCode) {
        final TestingDataLakeStorageOperations ops = new TestingDataLakeStorageOperations();
        ops.addFile("target", new byte[10], TEST_TIME);
        injector.inject(ops, "target", expectedStatusCode);

        assertThatThrownBy(() -> trigger.execute(ops, "target"))
                .isInstanceOf(DataLakeStorageException.class)
                .satisfies(
                        e ->
                                assertThat(((DataLakeStorageException) e).getStatusCode())
                                        .isEqualTo(expectedStatusCode));
    }

    @FunctionalInterface
    private interface ErrorInjector {
        void inject(TestingDataLakeStorageOperations ops, String path, int statusCode);
    }

    @FunctionalInterface
    private interface ErrorTrigger {
        void execute(TestingDataLakeStorageOperations ops, String path)
                throws DataLakeStorageException;
    }

    // --- File operations ---

    static Stream<Arguments> fileClient404Cases() {
        return Stream.of(
                Arguments.of((ErrorTrigger) (ops, path) -> ops.getFileClient(path).getProperties()),
                Arguments.of((ErrorTrigger) (ops, path) -> ops.getFileClient(path).delete()),
                Arguments.of(
                        (ErrorTrigger) (ops, path) -> ops.getFileClient(path).rename("fs", "dst")));
    }

    @ParameterizedTest
    @MethodSource("fileClient404Cases")
    void shouldThrow404WhenFileNotFound(final ErrorTrigger trigger) {
        final TestingDataLakeStorageOperations ops = new TestingDataLakeStorageOperations();

        assertThatThrownBy(() -> trigger.execute(ops, "missing"))
                .isInstanceOf(DataLakeStorageException.class)
                .satisfies(
                        e ->
                                assertThat(((DataLakeStorageException) e).getStatusCode())
                                        .isEqualTo(404));
    }

    static Stream<Arguments> fileClientTypeMismatchCases() {
        return Stream.of(
                Arguments.of((ErrorTrigger) (ops, path) -> ops.getFileClient(path).delete()),
                Arguments.of(
                        (ErrorTrigger) (ops, path) -> ops.getFileClient(path).rename("fs", "dst")));
    }

    @ParameterizedTest
    @MethodSource("fileClientTypeMismatchCases")
    void shouldThrow409WhenFileClientOperatesOnDirectory(final ErrorTrigger trigger) {
        final TestingDataLakeStorageOperations ops = new TestingDataLakeStorageOperations();
        ops.addDirectory("target", TEST_TIME);

        assertThatThrownBy(() -> trigger.execute(ops, "target"))
                .isInstanceOf(DataLakeStorageException.class)
                .satisfies(
                        e ->
                                assertThat(((DataLakeStorageException) e).getStatusCode())
                                        .isEqualTo(409));
    }

    static Stream<Arguments> deleteCases() {
        return Stream.of(
                // isDirectory, deleter
                Arguments.of(false, (Deleter) (ops, path) -> ops.getFileClient(path).delete()),
                Arguments.of(true, (Deleter) (ops, path) -> ops.getDirectoryClient(path).delete()));
    }

    @ParameterizedTest
    @MethodSource("deleteCases")
    void shouldDeleteEntry(final boolean isDirectory, final Deleter deleter) {
        final TestingDataLakeStorageOperations ops = new TestingDataLakeStorageOperations();
        if (isDirectory) {
            ops.addDirectory("target", TEST_TIME);
        } else {
            ops.addFile("target", new byte[10], TEST_TIME);
        }

        deleter.delete(ops, "target");

        assertThat(ops.exists("target")).isFalse();
    }

    @FunctionalInterface
    private interface Deleter {
        void delete(TestingDataLakeStorageOperations ops, String path);
    }

    @Test
    void shouldRenameFile() {
        final TestingDataLakeStorageOperations ops = new TestingDataLakeStorageOperations();
        ops.addFile("old.txt", new byte[10], TEST_TIME);

        ops.getFileClient("old.txt").rename("container", "new.txt");

        assertThat(ops.exists("old.txt")).isFalse();
        assertThat(ops.exists("new.txt")).isTrue();
    }

    // --- Directory operations ---

    static Stream<Arguments> directoryClient404Cases() {
        return Stream.of(
                Arguments.of((ErrorTrigger) (ops, path) -> ops.getDirectoryClient(path).delete()),
                Arguments.of(
                        (ErrorTrigger)
                                (ops, path) -> ops.getDirectoryClient(path).deleteRecursively()),
                Arguments.of(
                        (ErrorTrigger)
                                (ops, path) -> ops.getDirectoryClient(path).rename("fs", "dst")));
    }

    @ParameterizedTest
    @MethodSource("directoryClient404Cases")
    void shouldThrow404WhenDirectoryNotFound(final ErrorTrigger trigger) {
        final TestingDataLakeStorageOperations ops = new TestingDataLakeStorageOperations();

        assertThatThrownBy(() -> trigger.execute(ops, "missing"))
                .isInstanceOf(DataLakeStorageException.class)
                .satisfies(
                        e ->
                                assertThat(((DataLakeStorageException) e).getStatusCode())
                                        .isEqualTo(404));
    }

    static Stream<Arguments> directoryClientTypeMismatchCases() {
        return Stream.of(
                Arguments.of((ErrorTrigger) (ops, path) -> ops.getDirectoryClient(path).delete()),
                Arguments.of(
                        (ErrorTrigger)
                                (ops, path) -> ops.getDirectoryClient(path).deleteRecursively()),
                Arguments.of(
                        (ErrorTrigger)
                                (ops, path) -> ops.getDirectoryClient(path).rename("fs", "dst")));
    }

    @ParameterizedTest
    @MethodSource("directoryClientTypeMismatchCases")
    void shouldThrow409WhenDirectoryClientOperatesOnFile(final ErrorTrigger trigger) {
        final TestingDataLakeStorageOperations ops = new TestingDataLakeStorageOperations();
        ops.addFile("target", new byte[10], TEST_TIME);

        assertThatThrownBy(() -> trigger.execute(ops, "target"))
                .isInstanceOf(DataLakeStorageException.class)
                .satisfies(
                        e ->
                                assertThat(((DataLakeStorageException) e).getStatusCode())
                                        .isEqualTo(409));
    }

    @Test
    void shouldThrow409WhenDeletingNonEmptyDirectory() {
        final TestingDataLakeStorageOperations ops = new TestingDataLakeStorageOperations();
        ops.addDirectory("dir", TEST_TIME);
        ops.addFile("dir/child.txt", new byte[5], TEST_TIME);

        assertThatThrownBy(() -> ops.getDirectoryClient("dir").delete())
                .isInstanceOf(DataLakeStorageException.class)
                .satisfies(
                        e ->
                                assertThat(((DataLakeStorageException) e).getStatusCode())
                                        .isEqualTo(409));
    }

    @Test
    void shouldDeleteRecursively() {
        final TestingDataLakeStorageOperations ops = new TestingDataLakeStorageOperations();
        ops.addDirectory("dir", TEST_TIME);
        ops.addFile("dir/a.txt", new byte[5], TEST_TIME);
        ops.addDirectory("dir/sub", TEST_TIME);
        ops.addFile("dir/sub/b.txt", new byte[5], TEST_TIME);

        ops.getDirectoryClient("dir").deleteRecursively();

        assertThat(ops.exists("dir")).isFalse();
        assertThat(ops.exists("dir/a.txt")).isFalse();
        assertThat(ops.exists("dir/sub")).isFalse();
        assertThat(ops.exists("dir/sub/b.txt")).isFalse();
    }

    @Test
    void shouldCreateDirectoryIfNotExists() {
        final TestingDataLakeStorageOperations ops = new TestingDataLakeStorageOperations();

        ops.getDirectoryClient("newdir").createIfNotExists();

        assertThat(ops.exists("newdir")).isTrue();
    }

    @Test
    void shouldNotOverwriteExistingDirectoryOnCreateIfNotExists() {
        final TestingDataLakeStorageOperations ops = new TestingDataLakeStorageOperations();
        ops.addDirectory("existing", TEST_TIME);

        ops.getDirectoryClient("existing").createIfNotExists();

        assertThat(ops.exists("existing")).isTrue();
    }

    @Test
    void shouldRenameDirectoryWithChildren() {
        final TestingDataLakeStorageOperations ops = new TestingDataLakeStorageOperations();
        ops.addDirectory("src", TEST_TIME);
        ops.addFile("src/file.txt", new byte[10], TEST_TIME);

        ops.getDirectoryClient("src").rename("container", "dst");

        assertThat(ops.exists("src")).isFalse();
        assertThat(ops.exists("src/file.txt")).isFalse();
        assertThat(ops.exists("dst")).isTrue();
        assertThat(ops.exists("dst/file.txt")).isTrue();
    }

    // --- listPaths ---

    static Stream<Arguments> listPathsRecursiveCases() {
        return Stream.of(
                // recursive, expectedNames
                Arguments.of(false, List.of("dir/a.txt", "dir/sub")),
                Arguments.of(true, List.of("dir/a.txt", "dir/sub", "dir/sub/deep.txt")));
    }

    @ParameterizedTest
    @MethodSource("listPathsRecursiveCases")
    void shouldListPathsRespectingRecursiveFlag(
            final boolean recursive, final List<String> expectedNames) {
        final TestingDataLakeStorageOperations ops = new TestingDataLakeStorageOperations();
        ops.addFile("dir/a.txt", new byte[1], TEST_TIME);
        ops.addDirectory("dir/sub", TEST_TIME);
        ops.addFile("dir/sub/deep.txt", new byte[1], TEST_TIME);

        final ListPathsOptions options =
                new ListPathsOptions().setPath("dir").setRecursive(recursive);
        final List<PathItem> items = toList(ops.listPaths(options, null));

        assertThat(items)
                .extracting(PathItem::getName)
                .containsExactlyInAnyOrderElementsOf(expectedNames);
    }

    static Stream<Arguments> listRootCases() {
        return Stream.of(
                // recursive, expectedNames
                Arguments.of(false, List.of("top.txt", "dir")),
                Arguments.of(true, List.of("top.txt", "dir", "dir/nested.txt")));
    }

    @ParameterizedTest
    @MethodSource("listRootCases")
    void shouldListRootPathsRespectingRecursiveFlag(
            final boolean recursive, final List<String> expectedNames) {
        final TestingDataLakeStorageOperations ops = new TestingDataLakeStorageOperations();
        ops.addFile("top.txt", new byte[1], TEST_TIME);
        ops.addDirectory("dir", TEST_TIME);
        ops.addFile("dir/nested.txt", new byte[1], TEST_TIME);

        final ListPathsOptions options = new ListPathsOptions().setRecursive(recursive);
        final List<PathItem> items = toList(ops.listPaths(options, null));

        assertThat(items)
                .extracting(PathItem::getName)
                .containsExactlyInAnyOrderElementsOf(expectedNames);
    }

    @Test
    void shouldReturnEmptyListForEmptyDirectory() {
        final TestingDataLakeStorageOperations ops = new TestingDataLakeStorageOperations();
        ops.addDirectory("empty", TEST_TIME);

        final ListPathsOptions options =
                new ListPathsOptions().setPath("empty").setRecursive(false);
        final List<PathItem> items = toList(ops.listPaths(options, null));

        assertThat(items).isEmpty();
    }

    // --- createStorageException ---

    @Test
    void shouldCreateStorageExceptionWithStatusCode() {
        final DataLakeStorageException ex =
                TestingDataLakeStorageOperations.createStorageException(429);

        assertThat(ex.getStatusCode()).isEqualTo(429);
    }

    @Test
    void shouldCreateStorageExceptionWithMessage() {
        final DataLakeStorageException ex =
                TestingDataLakeStorageOperations.createStorageException(503, "Service Unavailable");

        assertThat(ex.getStatusCode()).isEqualTo(503);
        assertThat(ex.getMessage()).contains("Service Unavailable");
    }

    private static <T> List<T> toList(final Iterable<T> iterable) {
        final List<T> list = new ArrayList<>();
        iterable.forEach(list::add);
        return list;
    }
}
