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

package org.apache.flink.core.fs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.util.WrappingProxy;
import org.apache.flink.util.WrappingProxyUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link FileSystem} base class. */
class FileSystemTest {

    @Test
    void testGet() throws URISyntaxException, IOException {
        String scheme = "file";

        assertThat(getFileSystemWithoutSafetyNet(scheme + ":///test/test"))
                .isInstanceOf(LocalFileSystem.class);

        try {
            getFileSystemWithoutSafetyNet(scheme + "://test/test");
        } catch (IOException ioe) {
            assertThat(ioe.getMessage()).startsWith("Found local file path with authority '");
        }

        assertThat(getFileSystemWithoutSafetyNet(scheme + ":/test/test"))
                .isInstanceOf(LocalFileSystem.class);

        assertThat(getFileSystemWithoutSafetyNet(scheme + ":test/test"))
                .isInstanceOf(LocalFileSystem.class);

        assertThat(getFileSystemWithoutSafetyNet("/test/test")).isInstanceOf(LocalFileSystem.class);

        assertThat(getFileSystemWithoutSafetyNet("test/test")).isInstanceOf(LocalFileSystem.class);
    }

    @Test
    void testUnsupportedFS() {
        /*
        exception should be:
        org.apache.flink.core.fs.UnsupportedFileSystemSchemeException: Could not find a file system implementation
        for scheme 'unknownfs'. The scheme is not directly supported by Flink and no Hadoop file system to support this
        scheme could be loaded. */
        assertThatThrownBy(() -> getFileSystemWithoutSafetyNet("unknownfs://authority/"))
                .isInstanceOf(UnsupportedFileSystemSchemeException.class)
                .hasMessageContaining("not directly supported")
                .hasMessageContaining("no Hadoop file system to support this scheme");
    }

    @Test
    void testKnownFSWithoutPlugins() {
        /*
        exception should be:
        org.apache.flink.core.fs.UnsupportedFileSystemSchemeException: Could not find a file
        system implementation for scheme 's3'. The scheme is directly supported by Flink through the following
        plugins: flink-s3-fs-hadoop, flink-s3-fs-presto. Please ensure that each plugin resides within its own
        subfolder within the plugins directory.
        See https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/plugins/ for more information. */
        assertThatThrownBy(() -> getFileSystemWithoutSafetyNet("s3://authority/"))
                .isInstanceOf(UnsupportedFileSystemSchemeException.class)
                .hasMessageContaining("is directly supported")
                .hasMessageContaining("flink-s3-fs-hadoop")
                .hasMessageContaining("flink-s3-fs-presto")
                .hasMessageNotContaining("no Hadoop file system to support this scheme");
    }

    @Test
    void testKnownFSWithoutPluginsAndException() {
        try {
            final Configuration config = new Configuration();
            config.set(CoreOptions.ALLOWED_FALLBACK_FILESYSTEMS, "s3;wasb");
            FileSystem.initialize(config, null);

            /*
            exception should be:
            org.apache.flink.core.fs.UnsupportedFileSystemSchemeException: Could not find a file
            system implementation for scheme 's3'. File system schemes are supported by Flink through the following
            plugin(s): flink-s3-fs-hadoop, flink-s3-fs-presto. No file system to support this scheme could be loaded.
            Please ensure that each plugin is configured properly and resides within its own subfolder in the plugins directory.
            See https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/plugins/ for more information. */
            assertThatThrownBy(() -> getFileSystemWithoutSafetyNet("s3://authority/"))
                    .isInstanceOf(UnsupportedFileSystemSchemeException.class)
                    .hasMessageContaining("File system schemes are supported")
                    .hasMessageContaining("flink-s3-fs-hadoop")
                    .hasMessageContaining("flink-s3-fs-presto")
                    .hasMessageContaining("Please ensure that each plugin is configured properly");
        } finally {
            FileSystem.initialize(new Configuration(), null);
        }
    }

    private static final String PRIORITY_TEST_SCHEME = "priority-test";

    private static final FileSystemFactory[] ORDER_AB = {new FactoryA(), new FactoryB()};
    private static final FileSystemFactory[] ORDER_BA = {new FactoryB(), new FactoryA()};

    static Stream<Arguments> prioritySelectionCases() {
        // Each case: (priorityA, priorityB, expectedFsClass, factories).
        // Higher priority always wins. Equal priority: last-loaded factory wins.
        return Stream.of(
                Arguments.of(0, 0, DummyFsB.class, ORDER_AB),
                Arguments.of(0, 100, DummyFsB.class, ORDER_AB),
                Arguments.of(100, 0, DummyFsA.class, ORDER_AB),
                Arguments.of(-10, 0, DummyFsB.class, ORDER_AB),
                Arguments.of(50, 50, DummyFsB.class, ORDER_AB),
                // Reversed loading order — equal-priority cases flip to A
                Arguments.of(0, 0, DummyFsA.class, ORDER_BA),
                Arguments.of(0, 100, DummyFsB.class, ORDER_BA),
                Arguments.of(100, 0, DummyFsA.class, ORDER_BA),
                Arguments.of(-10, 0, DummyFsB.class, ORDER_BA),
                Arguments.of(50, 50, DummyFsA.class, ORDER_BA));
    }

    @ParameterizedTest
    @MethodSource("prioritySelectionCases")
    void shouldSelectFactoryByPriority(
            final int priorityA,
            final int priorityB,
            final Class<? extends FileSystem> expectedFsClass,
            final FileSystemFactory[] factories)
            throws Exception {
        final Configuration config = new Configuration();
        config.set(
                CoreOptions.fileSystemFactoryPriority(
                        PRIORITY_TEST_SCHEME, FactoryA.class.getName()),
                priorityA);
        config.set(
                CoreOptions.fileSystemFactoryPriority(
                        PRIORITY_TEST_SCHEME, FactoryB.class.getName()),
                priorityB);

        try {
            FileSystem.initialize(config, pluginManager(factories));

            assertThat(createAndUnwrap(PRIORITY_TEST_SCHEME)).isInstanceOf(expectedFsClass);
        } finally {
            FileSystem.initialize(new Configuration(), null);
        }
    }

    @Test
    void shouldRegisterSingleFactory() throws Exception {
        try {
            FileSystem.initialize(new Configuration(), pluginManager(new FactoryA()));

            assertThat(createAndUnwrap(PRIORITY_TEST_SCHEME)).isInstanceOf(DummyFsA.class);
        } finally {
            FileSystem.initialize(new Configuration(), null);
        }
    }

    static Stream<Arguments> declaredPriorityCases() {
        // FactoryA declares priority -1, FactoryB has default priority 0.
        // FactoryB should win regardless of iteration order.
        return Stream.of(
                Arguments.of(ORDER_AB, DummyFsB.class), Arguments.of(ORDER_BA, DummyFsB.class));
    }

    @ParameterizedTest
    @MethodSource("declaredPriorityCases")
    void shouldUseFactoryDeclaredPriorityWhenNoConfigSet(
            final FileSystemFactory[] factories, final Class<? extends FileSystem> expectedFsClass)
            throws Exception {
        try {
            FileSystem.initialize(new Configuration(), pluginManager(factories));

            assertThat(createAndUnwrap(PRIORITY_TEST_SCHEME)).isInstanceOf(expectedFsClass);
        } finally {
            FileSystem.initialize(new Configuration(), null);
        }
    }

    private static FileSystem getFileSystemWithoutSafetyNet(final String uri)
            throws URISyntaxException, IOException {
        final FileSystem fileSystem = FileSystem.get(new URI(uri));

        if (fileSystem instanceof WrappingProxy) {
            //noinspection unchecked
            return WrappingProxyUtil.stripProxy((WrappingProxy<FileSystem>) fileSystem);
        }

        return fileSystem;
    }

    private static FileSystem createAndUnwrap(final String scheme)
            throws URISyntaxException, IOException {
        FileSystem fs =
                FileSystem.getRegisteredFileSystemFactories().stream()
                        .filter(f -> scheme.equals(f.getScheme()))
                        .findFirst()
                        .orElseThrow()
                        .create(new URI(scheme + "://test"));
        if (fs instanceof WrappingProxy) {
            //noinspection unchecked
            return WrappingProxyUtil.stripProxy((WrappingProxy<FileSystem>) fs);
        }
        return fs;
    }

    private static PluginManager pluginManager(final FileSystemFactory... factories) {
        final List<FileSystemFactory> list = Arrays.asList(factories);
        return new PluginManager() {
            @SuppressWarnings("unchecked")
            @Override
            public <P> Iterator<P> load(final Class<P> service) {
                if (service == FileSystemFactory.class) {
                    return (Iterator<P>) list.iterator();
                }
                return Collections.emptyIterator();
            }
        };
    }

    static final class DummyFsA extends LocalFileSystem {}

    static final class DummyFsB extends LocalFileSystem {}

    static final class FactoryA implements FileSystemFactory {
        @Override
        public String getScheme() {
            return PRIORITY_TEST_SCHEME;
        }

        @Override
        public int getPriority() {
            return -1;
        }

        @Override
        public FileSystem create(final URI fsUri) {
            return new DummyFsA();
        }
    }

    static final class FactoryB implements FileSystemFactory {
        @Override
        public String getScheme() {
            return PRIORITY_TEST_SCHEME;
        }

        @Override
        public FileSystem create(final URI fsUri) {
            return new DummyFsB();
        }
    }
}
