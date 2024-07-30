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

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link EntropyInjector}. */
class EntropyInjectorTest {

    @TempDir private static java.nio.file.Path tempFolder;

    @Test
    void testEmptyPath() throws Exception {
        EntropyInjectingFileSystem efs = new TestEntropyInjectingFs("test", "ignored");
        Path path = new Path("hdfs://localhost:12345");

        assertThat(EntropyInjector.resolveEntropy(path, efs, true)).isEqualTo(path);
        assertThat(EntropyInjector.resolveEntropy(path, efs, false)).isEqualTo(path);
    }

    @Test
    void testFullUriNonMatching() throws Exception {
        EntropyInjectingFileSystem efs = new TestEntropyInjectingFs("_entropy_key_", "ignored");
        Path path = new Path("s3://hugo@myawesomehost:55522/path/to/the/file");

        assertThat(EntropyInjector.resolveEntropy(path, efs, true)).isEqualTo(path);
        assertThat(EntropyInjector.resolveEntropy(path, efs, false)).isEqualTo(path);
    }

    @Test
    void testFullUriMatching() throws Exception {
        EntropyInjectingFileSystem efs = new TestEntropyInjectingFs("s0mek3y", "12345678");
        Path path = new Path("s3://hugo@myawesomehost:55522/path/s0mek3y/the/file");

        assertThat(EntropyInjector.resolveEntropy(path, efs, true))
                .isEqualTo(new Path("s3://hugo@myawesomehost:55522/path/12345678/the/file"));
        assertThat(EntropyInjector.resolveEntropy(path, efs, false))
                .isEqualTo(new Path("s3://hugo@myawesomehost:55522/path/the/file"));
    }

    @Test
    void testPathOnlyNonMatching() throws Exception {
        EntropyInjectingFileSystem efs = new TestEntropyInjectingFs("_entropy_key_", "ignored");
        Path path = new Path("/path/file");

        assertThat(EntropyInjector.resolveEntropy(path, efs, true)).isEqualTo(path);
        assertThat(EntropyInjector.resolveEntropy(path, efs, false)).isEqualTo(path);
    }

    @Test
    void testPathOnlyMatching() throws Exception {
        EntropyInjectingFileSystem efs = new TestEntropyInjectingFs("_entropy_key_", "xyzz");
        Path path = new Path("/path/_entropy_key_/file");

        assertThat(EntropyInjector.resolveEntropy(path, efs, true))
                .isEqualTo(new Path("/path/xyzz/file"));
        assertThat(EntropyInjector.resolveEntropy(path, efs, false))
                .isEqualTo(new Path("/path/file"));
    }

    @Test
    void testEntropyNotFullSegment() throws Exception {
        EntropyInjectingFileSystem efs = new TestEntropyInjectingFs("_entropy_key_", "pqr");
        Path path = new Path("s3://myhost:122/entropy-_entropy_key_-suffix/file");

        assertThat(EntropyInjector.resolveEntropy(path, efs, true))
                .isEqualTo(new Path("s3://myhost:122/entropy-pqr-suffix/file"));
        assertThat(EntropyInjector.resolveEntropy(path, efs, false))
                .isEqualTo(new Path("s3://myhost:122/entropy--suffix/file"));
    }

    @Test
    void testCreateEntropyAwarePlainFs() throws Exception {
        File folder = TempDirUtils.newFolder(tempFolder);
        Path path = new Path(Path.fromLocalFile(folder), "_entropy_/file");

        OutputStreamAndPath out =
                EntropyInjector.createEntropyAware(
                        LocalFileSystem.getSharedInstance(), path, WriteMode.NO_OVERWRITE);

        out.stream().close();

        assertThat(out.path()).isEqualTo(path);
        assertThat(new File(new File(folder, "_entropy_"), "file")).exists();
    }

    @Test
    void testCreateEntropyAwareEntropyFs() throws Exception {
        File folder = TempDirUtils.newFolder(tempFolder);
        Path path = new Path(Path.fromLocalFile(folder), "_entropy_/file");
        Path pathWithEntropy = new Path(Path.fromLocalFile(folder), "test-entropy/file");

        FileSystem fs = new TestEntropyInjectingFs("_entropy_", "test-entropy");

        OutputStreamAndPath out =
                EntropyInjector.createEntropyAware(fs, path, WriteMode.NO_OVERWRITE);

        out.stream().close();

        assertThat(out.path()).isEqualTo(new Path(Path.fromLocalFile(folder), "test-entropy/file"));
        assertThat(new File(new File(folder, "test-entropy"), "file")).exists();
    }

    @Test
    void testWithSafetyNet() throws Exception {
        final String entropyKey = "__ekey__";
        final String entropyValue = "abc";

        final File folder = TempDirUtils.newFolder(tempFolder);

        final Path path = new Path(Path.fromLocalFile(folder), entropyKey + "/path/");
        final Path pathWithEntropy = new Path(Path.fromLocalFile(folder), entropyValue + "/path/");

        TestEntropyInjectingFs efs = new TestEntropyInjectingFs(entropyKey, entropyValue);

        FSDataOutputStream out;

        FileSystemSafetyNet.initializeSafetyNetForThread();
        FileSystem fs = FileSystemSafetyNet.wrapWithSafetyNetWhenActivated(efs);
        try {
            OutputStreamAndPath streamAndPath =
                    EntropyInjector.createEntropyAware(fs, path, WriteMode.NO_OVERWRITE);

            out = streamAndPath.stream();

            assertThat(streamAndPath.path()).isEqualTo(pathWithEntropy);
        } finally {
            FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
        }

        // check that the safety net closed the stream
        assertThatThrownBy(
                        () -> {
                            out.write(42);
                            out.flush();
                        })
                .isInstanceOf(IOException.class);
    }

    @Test
    void testClassLoaderFixingFsWithSafeyNet() throws Exception {
        final String entropyKey = "__ekey__";
        final String entropyValue = "abc";

        final File folder = TempDirUtils.newFolder(tempFolder);

        final Path path = new Path(Path.fromLocalFile(folder), entropyKey + "/path/");
        final Path pathWithEntropy = new Path(Path.fromLocalFile(folder), entropyValue + "/path/");

        PluginFileSystemFactory pluginFsFactory =
                PluginFileSystemFactory.of(new TestFileSystemFactory(entropyKey, entropyValue));
        FileSystem testFs = pluginFsFactory.create(URI.create("test"));

        FileSystemSafetyNet.initializeSafetyNetForThread();
        FileSystem fs = FileSystemSafetyNet.wrapWithSafetyNetWhenActivated(testFs);
        try {
            OutputStreamAndPath streamAndPath =
                    EntropyInjector.createEntropyAware(fs, path, WriteMode.NO_OVERWRITE);

            assertThat(streamAndPath.path()).isEqualTo(pathWithEntropy);
        } finally {
            FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
        }
    }

    @Test
    void testClassLoaderFixingFsWithoutSafeyNet() throws Exception {
        final String entropyKey = "__ekey__";
        final String entropyValue = "abc";

        final File folder = TempDirUtils.newFolder(tempFolder);

        final Path path = new Path(Path.fromLocalFile(folder), entropyKey + "/path/");
        final Path pathWithEntropy = new Path(Path.fromLocalFile(folder), entropyValue + "/path/");

        PluginFileSystemFactory pluginFsFactory =
                PluginFileSystemFactory.of(new TestFileSystemFactory(entropyKey, entropyValue));
        FileSystem testFs = pluginFsFactory.create(URI.create("test"));

        OutputStreamAndPath streamAndPath =
                EntropyInjector.createEntropyAware(testFs, path, WriteMode.NO_OVERWRITE);

        assertThat(streamAndPath.path()).isEqualTo(pathWithEntropy);
    }

    @Test
    void testIsEntropyFs() throws Exception {
        final String entropyKey = "_test_";
        final FileSystem efs = new TestEntropyInjectingFs(entropyKey, "ignored");
        final File folder = TempDirUtils.newFolder(tempFolder);
        final Path path = new Path(Path.fromLocalFile(folder), entropyKey + "/path/");
        assertThat(EntropyInjector.isEntropyInjecting(efs, path)).isTrue();
    }

    @Test
    void testIsEntropyFsWithNullEntropyKey() throws Exception {
        final FileSystem efs = new TestEntropyInjectingFs(null, "ignored");

        final File folder = TempDirUtils.newFolder(tempFolder);
        assertThat(EntropyInjector.isEntropyInjecting(efs, Path.fromLocalFile(folder))).isFalse();
    }

    @Test
    void testIsEntropyFsPathDoesNotIncludeEntropyKey() throws Exception {
        final String entropyKey = "_test_";
        final FileSystem efs = new TestEntropyInjectingFs(entropyKey, "ignored");
        final File folder = TempDirUtils.newFolder(tempFolder);
        final Path path = new Path(Path.fromLocalFile(folder), "path"); // no entropy key
        assertThat(EntropyInjector.isEntropyInjecting(efs, path)).isFalse();
    }

    // ------------------------------------------------------------------------

    private static final class TestEntropyInjectingFs extends LocalFileSystem
            implements EntropyInjectingFileSystem {

        private final String key;

        private final String entropy;

        TestEntropyInjectingFs(String key, String entropy) {
            this.key = key;
            this.entropy = entropy;
        }

        @Override
        public String getEntropyInjectionKey() {
            return key;
        }

        @Override
        public String generateEntropy() {
            return entropy;
        }

        private static File newFolder(File root, String... subDirs) throws IOException {
            String subFolder = String.join("/", subDirs);
            File result = new File(root, subFolder);
            if (!result.mkdirs()) {
                throw new IOException("Couldn't create folders " + root);
            }
            return result;
        }
    }

    private static class TestFileSystemFactory implements FileSystemFactory {

        private final String key;
        private final String entropy;

        TestFileSystemFactory(String key, String entropy) {
            this.key = key;
            this.entropy = entropy;
        }

        @Override
        public String getScheme() {
            return null;
        }

        @Override
        public FileSystem create(URI fsUri) {
            return new TestEntropyInjectingFs(key, entropy);
        }

        private static File newFolder(File root, String... subDirs) throws IOException {
            String subFolder = String.join("/", subDirs);
            File result = new File(root, subFolder);
            if (!result.mkdirs()) {
                throw new IOException("Couldn't create folders " + root);
            }
            return result;
        }
    }

    private static File newFolder(File root, String... subDirs) throws IOException {
        String subFolder = String.join("/", subDirs);
        File result = new File(root, subFolder);
        if (!result.mkdirs()) {
            throw new IOException("Couldn't create folders " + root);
        }
        return result;
    }
}
