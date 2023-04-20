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

package org.apache.flink.util;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Tests for the {@link FileUtils}. */
@ExtendWith(TestLoggerExtension.class)
public class FileUtilsTest {

    @TempDir private java.nio.file.Path temporaryFolder;

    // ------------------------------------------------------------------------
    //  Tests
    // ------------------------------------------------------------------------

    @Test
    void testReadAllBytes() throws Exception {

        File tempFile =
                TempDirUtils.newFolder(Paths.get(this.getClass().getResource("/").getPath()));

        final int fileSize = 1024;
        final String testFilePath =
                tempFile.toPath()
                        .resolve(this.getClass().getSimpleName() + "_" + fileSize + ".txt")
                        .toString();

        {
            String expectedMD5 = generateTestFile(testFilePath, 1024);
            final byte[] data = FileUtils.readAllBytes((new File(testFilePath)).toPath());
            assertThat(md5Hex(data)).isEqualTo(expectedMD5);
        }

        {
            String expectedMD5 = generateTestFile(testFilePath, 4096);
            final byte[] data = FileUtils.readAllBytes((new File(testFilePath)).toPath());
            assertThat(md5Hex(data)).isEqualTo(expectedMD5);
        }

        {
            String expectedMD5 = generateTestFile(testFilePath, 5120);
            final byte[] data = FileUtils.readAllBytes((new File(testFilePath)).toPath());
            assertThat(md5Hex(data)).isEqualTo(expectedMD5);
        }
    }

    @Test
    void testDeleteQuietly() throws Exception {
        // should ignore the call
        FileUtils.deleteDirectoryQuietly(null);
        File doesNotExist = TempDirUtils.newFolder(temporaryFolder, "abc");
        FileUtils.deleteDirectoryQuietly(doesNotExist);

        File cannotDeleteParent = TempDirUtils.newFolder(temporaryFolder);
        File cannotDeleteChild = new File(cannotDeleteParent, "child");

        try {
            assertThat(cannotDeleteChild.createNewFile()).isTrue();
            assertThat(cannotDeleteParent.setWritable(false)).isTrue();
            assertThat(cannotDeleteChild.setWritable(false)).isTrue();

            FileUtils.deleteDirectoryQuietly(cannotDeleteParent);
        } finally {
            //noinspection ResultOfMethodCallIgnored
            cannotDeleteParent.setWritable(true);
            //noinspection ResultOfMethodCallIgnored
            cannotDeleteChild.setWritable(true);
        }
    }

    @Test
    void testDeleteDirectory() throws Exception {

        // deleting a non-existent file should not cause an error

        File doesNotExist = TempDirUtils.newFolder(temporaryFolder, "abc");
        FileUtils.deleteDirectory(doesNotExist);

        // deleting a write protected file should throw an error

        File cannotDeleteParent = TempDirUtils.newFolder(temporaryFolder);
        File cannotDeleteChild = new File(cannotDeleteParent, "child");

        try {
            assumeThat(cannotDeleteChild.createNewFile()).isTrue();
            assumeThat(cannotDeleteParent.setWritable(false)).isTrue();
            assumeThat(cannotDeleteChild.setWritable(false)).isTrue();
            assertThatThrownBy(() -> FileUtils.deleteDirectory(cannotDeleteParent))
                    .isInstanceOf(AccessDeniedException.class);
        } finally {
            //noinspection ResultOfMethodCallIgnored
            cannotDeleteParent.setWritable(true);
            //noinspection ResultOfMethodCallIgnored
            cannotDeleteChild.setWritable(true);
        }
    }

    @Test
    void testDeleteDirectoryWhichIsAFile() throws Exception {

        // deleting a directory that is actually a file should fails

        File file = TempDirUtils.newFile(temporaryFolder);
        assertThatThrownBy(() -> FileUtils.deleteDirectory(file))
                .withFailMessage("this should fail with an exception")
                .isInstanceOf(IOException.class);
    }

    /** Deleting a symbolic link directory should not delete the files in it. */
    @Test
    void testDeleteSymbolicLinkDirectory() throws Exception {
        // creating a directory to which the test creates a symbolic link
        File linkedDirectory = TempDirUtils.newFolder(temporaryFolder);
        File fileInLinkedDirectory = new File(linkedDirectory, "child");
        assertThat(fileInLinkedDirectory.createNewFile()).isTrue();

        File symbolicLink = new File(temporaryFolder.toString(), "symLink");
        try {
            Files.createSymbolicLink(symbolicLink.toPath(), linkedDirectory.toPath());
        } catch (FileSystemException e) {
            // this operation can fail under Windows due to: "A required privilege is not held by
            // the client."
            assumeThat(OperatingSystem.isWindows())
                    .withFailMessage("This test does not work properly under Windows")
                    .isFalse();
            throw e;
        }

        FileUtils.deleteDirectory(symbolicLink);
        assertThat(fileInLinkedDirectory.exists()).isTrue();
    }

    @Test
    void testDeleteDirectoryConcurrently() throws Exception {
        final File parent = TempDirUtils.newFolder(temporaryFolder);

        generateRandomDirs(parent, 20, 5, 3);

        // start three concurrent threads that delete the contents
        CheckedThread t1 = new Deleter(parent);
        CheckedThread t2 = new Deleter(parent);
        CheckedThread t3 = new Deleter(parent);
        t1.start();
        t2.start();
        t3.start();
        t1.sync();
        t2.sync();
        t3.sync();

        // assert is empty
        assertThat(parent.exists()).isFalse();
    }

    @Test
    void testCompressionOnAbsolutePath() throws IOException {
        final java.nio.file.Path testDir =
                TempDirUtils.newFolder(temporaryFolder, "compressDir").toPath();
        verifyDirectoryCompression(testDir, testDir);
    }

    @Test
    void testCompressionOnRelativePath() throws IOException {
        final java.nio.file.Path testDir =
                TempDirUtils.newFolder(temporaryFolder, "compressDir").toPath();
        final java.nio.file.Path relativeCompressDir =
                Paths.get(new File("").getAbsolutePath()).relativize(testDir);

        verifyDirectoryCompression(testDir, relativeCompressDir);
    }

    @Test
    void testListFilesInPathWithoutAnyFileReturnEmptyList() throws IOException {
        final java.nio.file.Path testDir =
                TempDirUtils.newFolder(temporaryFolder, "_test_0").toPath();

        assertThat(FileUtils.listFilesInDirectory(testDir, FileUtils::isJarFile)).isEmpty();
    }

    @Test
    void testListFilesInPath() throws IOException {
        final java.nio.file.Path testDir =
                TempDirUtils.newFolder(temporaryFolder, "_test_1").toPath();
        final Collection<java.nio.file.Path> testFiles = prepareTestFiles(testDir);

        final Collection<java.nio.file.Path> filesInDirectory =
                FileUtils.listFilesInDirectory(testDir, FileUtils::isJarFile);
        assertThat(filesInDirectory).containsExactlyInAnyOrderElementsOf(testFiles);
    }

    @Test
    void testRelativizeOfAbsolutePath() throws IOException {
        final java.nio.file.Path absolutePath =
                TempDirUtils.newFolder(temporaryFolder).toPath().toAbsolutePath();

        final java.nio.file.Path rootPath = temporaryFolder.getRoot();
        final java.nio.file.Path relativePath = FileUtils.relativizePath(rootPath, absolutePath);
        assertThat(relativePath.isAbsolute()).isFalse();
        assertThat(absolutePath).isEqualTo(rootPath.resolve(relativePath));
    }

    @Test
    void testRelativizeOfRelativePath() {
        final java.nio.file.Path path = Paths.get("foobar");
        assertThat(path.isAbsolute()).isFalse();

        final java.nio.file.Path relativePath =
                FileUtils.relativizePath(temporaryFolder.getRoot(), path);
        assertThat(path).isEqualTo(relativePath);
    }

    @Test
    void testAbsolutePathToURL() throws MalformedURLException {
        final java.nio.file.Path absolutePath = temporaryFolder.getRoot().toAbsolutePath();
        final URL absoluteURL = FileUtils.toURL(absolutePath);

        final java.nio.file.Path transformedURL = Paths.get(absoluteURL.getPath());
        assertThat(transformedURL).isEqualTo(absolutePath);
    }

    @Test
    void testRelativePathToURL() throws MalformedURLException {
        final java.nio.file.Path relativePath = Paths.get("foobar");
        assertThat(relativePath.isAbsolute()).isFalse();

        final URL relativeURL = FileUtils.toURL(relativePath);
        final java.nio.file.Path transformedPath = Paths.get(relativeURL.getPath());

        assertThat(transformedPath).isEqualTo(relativePath);
    }

    @Test
    void testListDirFailsIfDirectoryDoesNotExist() {
        final String fileName = "_does_not_exists_file";
        assertThatThrownBy(
                        () ->
                                FileUtils.listFilesInDirectory(
                                        temporaryFolder.getRoot().resolve(fileName),
                                        FileUtils::isJarFile))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testListAFileFailsBecauseDirectoryIsExpected() throws IOException {
        final String fileName = "a.jar";
        final File file = TempDirUtils.newFile(temporaryFolder, fileName);
        assertThatThrownBy(
                        () -> FileUtils.listFilesInDirectory(file.toPath(), FileUtils::isJarFile))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testFollowSymbolicDirectoryLink() throws IOException {
        final File directory = TempDirUtils.newFolder(temporaryFolder, "a");
        final File file = new File(directory, "a.jar");
        assertThat(file.createNewFile()).isTrue();

        final File otherDirectory = TempDirUtils.newFolder(temporaryFolder);
        java.nio.file.Path linkPath = Paths.get(otherDirectory.getPath(), "a.lnk");
        Files.createSymbolicLink(linkPath, directory.toPath());

        Collection<java.nio.file.Path> paths =
                FileUtils.listFilesInDirectory(linkPath, FileUtils::isJarFile);

        assertThat(paths).containsExactlyInAnyOrder(linkPath.resolve(file.getName()));
    }

    @Test
    void testGetTargetPathNotContainsSymbolicPath() throws IOException {
        java.nio.file.Path testPath = Paths.get("parent", "child");
        java.nio.file.Path targetPath = FileUtils.getTargetPathIfContainsSymbolicPath(testPath);
        assertThat(targetPath).isEqualTo(testPath);
    }

    @Test
    void testGetTargetPathContainsSymbolicPath() throws IOException {
        File linkedDir = TempDirUtils.newFolder(temporaryFolder, "linked");
        java.nio.file.Path symlink = Paths.get(temporaryFolder.toString(), "symlink");
        java.nio.file.Path dirInLinked =
                TempDirUtils.newFolder(linkedDir.toPath(), "one", "two").toPath().toRealPath();
        Files.createSymbolicLink(symlink, linkedDir.toPath());

        java.nio.file.Path targetPath =
                FileUtils.getTargetPathIfContainsSymbolicPath(
                        symlink.resolve("one").resolve("two"));
        assertThat(targetPath).isEqualTo(dirInLinked);
    }

    @Test
    void testGetTargetPathContainsMultipleSymbolicPath() throws IOException {
        File linked1Dir = TempDirUtils.newFolder(temporaryFolder, "linked1");
        java.nio.file.Path symlink1 = Paths.get(temporaryFolder.toString(), "symlink1");
        Files.createSymbolicLink(symlink1, linked1Dir.toPath());

        java.nio.file.Path symlink2 = Paths.get(symlink1.toString(), "symlink2");
        File linked2Dir = TempDirUtils.newFolder(temporaryFolder, "linked2");
        Files.createSymbolicLink(symlink2, linked2Dir.toPath());
        java.nio.file.Path dirInLinked2 =
                TempDirUtils.newFolder(linked2Dir.toPath(), "one").toPath().toRealPath();

        // symlink3 point to another symbolic link: symlink2
        java.nio.file.Path symlink3 = Paths.get(symlink1.toString(), "symlink3");
        Files.createSymbolicLink(symlink3, symlink2);

        java.nio.file.Path targetPath =
                FileUtils.getTargetPathIfContainsSymbolicPath(
                        // path contains multiple symlink : xxx/symlink1/symlink3/one
                        symlink3.resolve("one"));
        assertThat(targetPath).isEqualTo(dirInLinked2);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private static void assertDirEquals(java.nio.file.Path expected, java.nio.file.Path actual)
            throws IOException {
        assertThat(Files.isDirectory(actual)).isEqualTo(Files.isDirectory(expected));
        assertThat(actual.getFileName()).isEqualTo(expected.getFileName());

        if (Files.isDirectory(expected)) {
            List<java.nio.file.Path> expectedContents;
            try (Stream<java.nio.file.Path> files = Files.list(expected)) {
                expectedContents =
                        files.sorted(Comparator.comparing(java.nio.file.Path::toString))
                                .collect(Collectors.toList());
            }
            List<java.nio.file.Path> actualContents;
            try (Stream<java.nio.file.Path> files = Files.list(actual)) {
                actualContents =
                        files.sorted(Comparator.comparing(java.nio.file.Path::toString))
                                .collect(Collectors.toList());
            }

            assertThat(actualContents).hasSameSizeAs(expectedContents);

            for (int x = 0; x < expectedContents.size(); x++) {
                assertDirEquals(expectedContents.get(x), actualContents.get(x));
            }
        } else {
            byte[] expectedBytes = Files.readAllBytes(expected);
            byte[] actualBytes = Files.readAllBytes(actual);
            assertThat(actualBytes).isEqualTo(expectedBytes);
        }
    }

    private static void generateRandomDirs(File dir, int numFiles, int numDirs, int depth)
            throws IOException {
        // generate the random files
        for (int i = 0; i < numFiles; i++) {
            File file = new File(dir, new AbstractID().toString());
            try (FileOutputStream out = new FileOutputStream(file)) {
                out.write(1);
            }
        }

        if (depth > 0) {
            // generate the directories
            for (int i = 0; i < numDirs; i++) {
                File subdir = new File(dir, new AbstractID().toString());
                assertThat(subdir.mkdir()).isTrue();
                generateRandomDirs(subdir, numFiles, numDirs, depth - 1);
            }
        }
    }

    /**
     * Generate some files in the directory {@code dir}.
     *
     * @param dir the directory where the files are generated
     * @return The list of generated files
     * @throws IOException if I/O error occurs while generating the files
     */
    public static Collection<java.nio.file.Path> prepareTestFiles(final java.nio.file.Path dir)
            throws IOException {
        final java.nio.file.Path jobSubDir1 = Files.createDirectory(dir.resolve("_sub_dir1"));
        final java.nio.file.Path jobSubDir2 = Files.createDirectory(dir.resolve("_sub_dir2"));
        final java.nio.file.Path jarFile1 = Files.createFile(dir.resolve("file1.jar"));
        final java.nio.file.Path jarFile2 = Files.createFile(dir.resolve("file2.jar"));
        final java.nio.file.Path jarFile3 = Files.createFile(jobSubDir1.resolve("file3.jar"));
        final java.nio.file.Path jarFile4 = Files.createFile(jobSubDir2.resolve("file4.jar"));
        final Collection<java.nio.file.Path> jarFiles = new ArrayList<>();

        Files.createFile(dir.resolve("file1.txt"));
        Files.createFile(jobSubDir2.resolve("file2.txt"));

        jarFiles.add(jarFile1);
        jarFiles.add(jarFile2);
        jarFiles.add(jarFile3);
        jarFiles.add(jarFile4);
        return jarFiles;
    }

    /**
     * Generate some directories in a original directory based on the {@code testDir}.
     *
     * @param testDir the path of the directory where the test directories are generated
     * @param compressDir the path of directory to be verified
     * @throws IOException if I/O error occurs while generating the directories
     */
    private void verifyDirectoryCompression(
            final java.nio.file.Path testDir, final java.nio.file.Path compressDir)
            throws IOException {
        final String testFileContent =
                "Goethe - Faust: Der Tragoedie erster Teil\n"
                        + "Prolog im Himmel.\n"
                        + "Der Herr. Die himmlischen Heerscharen. Nachher Mephistopheles. Die drei\n"
                        + "Erzengel treten vor.\n"
                        + "RAPHAEL: Die Sonne toent, nach alter Weise, In Brudersphaeren Wettgesang,\n"
                        + "Und ihre vorgeschriebne Reise Vollendet sie mit Donnergang. Ihr Anblick\n"
                        + "gibt den Engeln Staerke, Wenn keiner Sie ergruenden mag; die unbegreiflich\n"
                        + "hohen Werke Sind herrlich wie am ersten Tag.\n"
                        + "GABRIEL: Und schnell und unbegreiflich schnelle Dreht sich umher der Erde\n"
                        + "Pracht; Es wechselt Paradieseshelle Mit tiefer, schauervoller Nacht. Es\n"
                        + "schaeumt das Meer in breiten Fluessen Am tiefen Grund der Felsen auf, Und\n"
                        + "Fels und Meer wird fortgerissen Im ewig schnellem Sphaerenlauf.\n"
                        + "MICHAEL: Und Stuerme brausen um die Wette Vom Meer aufs Land, vom Land\n"
                        + "aufs Meer, und bilden wuetend eine Kette Der tiefsten Wirkung rings umher.\n"
                        + "Da flammt ein blitzendes Verheeren Dem Pfade vor des Donnerschlags. Doch\n"
                        + "deine Boten, Herr, verehren Das sanfte Wandeln deines Tags.";

        final java.nio.file.Path extractDir =
                TempDirUtils.newFolder(temporaryFolder, "extractDir").toPath();

        final java.nio.file.Path originalDir = Paths.get("rootDir");
        final java.nio.file.Path emptySubDir = originalDir.resolve("emptyDir");
        final java.nio.file.Path fullSubDir = originalDir.resolve("fullDir");
        final java.nio.file.Path file1 = originalDir.resolve("file1");
        final java.nio.file.Path file2 = originalDir.resolve("file2");
        final java.nio.file.Path file3 = fullSubDir.resolve("file3");

        Files.createDirectory(testDir.resolve(originalDir));
        Files.createDirectory(testDir.resolve(emptySubDir));
        Files.createDirectory(testDir.resolve(fullSubDir));
        Files.copy(
                new ByteArrayInputStream(testFileContent.getBytes(StandardCharsets.UTF_8)),
                testDir.resolve(file1));
        Files.createFile(testDir.resolve(file2));
        Files.copy(
                new ByteArrayInputStream(testFileContent.getBytes(StandardCharsets.UTF_8)),
                testDir.resolve(file3));

        final Path zip =
                FileUtils.compressDirectory(
                        new Path(compressDir.resolve(originalDir).toString()),
                        new Path(compressDir.resolve(originalDir) + ".zip"));

        FileUtils.expandDirectory(zip, new Path(extractDir.toAbsolutePath().toString()));

        assertDirEquals(compressDir.resolve(originalDir), extractDir.resolve(originalDir));
    }

    /**
     * Generates a random content file.
     *
     * @param outputFile the path of the output file
     * @param length the size of content to generate
     * @return MD5 of the output file
     * @throws IOException
     * @throws NoSuchAlgorithmException
     */
    private static String generateTestFile(String outputFile, int length)
            throws IOException, NoSuchAlgorithmException {
        Path outputFilePath = new Path(outputFile);

        final FileSystem fileSystem = outputFilePath.getFileSystem();
        try (final FSDataOutputStream fsDataOutputStream =
                fileSystem.create(outputFilePath, FileSystem.WriteMode.OVERWRITE)) {
            return writeRandomContent(fsDataOutputStream, length);
        }
    }

    private static String writeRandomContent(OutputStream out, int length)
            throws IOException, NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("MD5");

        Random random = new Random();
        char startChar = 32, endChar = 127;
        for (int i = 0; i < length; i++) {
            int rnd = random.nextInt(endChar - startChar);
            byte b = (byte) (startChar + rnd);

            out.write(b);
            messageDigest.update(b);
        }

        byte[] b = messageDigest.digest();
        return org.apache.flink.util.StringUtils.byteToHexString(b);
    }

    private static String md5Hex(byte[] data) throws NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        messageDigest.update(data);

        byte[] b = messageDigest.digest();
        return org.apache.flink.util.StringUtils.byteToHexString(b);
    }

    // ------------------------------------------------------------------------

    private static class Deleter extends CheckedThread {

        private final File target;

        Deleter(File target) {
            this.target = target;
        }

        @Override
        public void go() throws Exception {
            FileUtils.deleteDirectory(target);
        }
    }
}
