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
import org.apache.flink.util.WrappingProxy;
import org.apache.flink.util.WrappingProxyUtil;
import org.apache.flink.util.function.ThrowingRunnable;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the {@link FileSystem} base class. */
public class FileSystemTest {

    @Test
    public void testGet() throws URISyntaxException, IOException {
        String scheme = "file";

        assertTrue(
                getFileSystemWithoutSafetyNet(scheme + ":///test/test") instanceof LocalFileSystem);

        try {
            getFileSystemWithoutSafetyNet(scheme + "://test/test");
        } catch (IOException ioe) {
            assertTrue(ioe.getMessage().startsWith("Found local file path with authority '"));
        }

        assertTrue(
                getFileSystemWithoutSafetyNet(scheme + ":/test/test") instanceof LocalFileSystem);

        assertTrue(getFileSystemWithoutSafetyNet(scheme + ":test/test") instanceof LocalFileSystem);

        assertTrue(getFileSystemWithoutSafetyNet("/test/test") instanceof LocalFileSystem);

        assertTrue(getFileSystemWithoutSafetyNet("test/test") instanceof LocalFileSystem);
    }

    @Test
    public void testUnsupportedFS() throws Exception {
        Exception e = assertThatCode(() -> getFileSystemWithoutSafetyNet("unknownfs://authority/"));
        assertThat(e, Matchers.instanceOf(UnsupportedFileSystemSchemeException.class));
    }

    @Test
    public void testKnownFSWithoutPlugins() throws Exception {
        Exception e = assertThatCode(() -> getFileSystemWithoutSafetyNet("s3://authority/"));
        assertThat(e, Matchers.instanceOf(UnsupportedFileSystemSchemeException.class));
        /*
         exception should be:
         org.apache.flink.core.fs.UnsupportedFileSystemSchemeException: Could not find a file
        system implementation for scheme 's3'. The scheme is directly supported by Flink through the following
        plugins: flink-s3-fs-hadoop, flink-s3-fs-presto. Please ensure that each plugin resides within its own
        subfolder within the plugins directory. See https://ci.apache
        .org/projects/flink/flink-docs-master/ops/plugins.html for more information. */
        assertThat(e.getMessage(), not(containsString("not directly supported")));
        assertThat(e.getMessage(), containsString("flink-s3-fs-hadoop"));
        assertThat(e.getMessage(), containsString("flink-s3-fs-presto"));
    }

    @Test
    public void testKnownFSWithoutPluginsAndException() throws Exception {
        try {
            final Configuration config = new Configuration();
            config.set(CoreOptions.ALLOWED_FALLBACK_FILESYSTEMS, "s3;wasb");
            FileSystem.initialize(config);

            Exception e = assertThatCode(() -> getFileSystemWithoutSafetyNet("s3://authority/"));
            assertThat(e, Matchers.instanceOf(UnsupportedFileSystemSchemeException.class));
            /*
            exception should be:
            org.apache.flink.core.fs.UnsupportedFileSystemSchemeException: Could not find a file system implementation
            for scheme 's3'. The scheme is not directly supported by Flink and no Hadoop file system to support this
            scheme could be loaded. */
            assertThat(e.getMessage(), containsString("not directly supported"));
        } finally {
            FileSystem.initialize(new Configuration());
        }
    }

    private static <E extends Throwable> E assertThatCode(ThrowingRunnable<E> runnable) throws E {
        try {
            runnable.run();
            fail("No exception thrown");
            return null;
        } catch (Throwable e) {
            try {
                return (E) e;
            } catch (ClassCastException c) {
                throw e;
            }
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
}
