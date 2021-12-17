/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.python.env.beam;

import org.apache.flink.api.common.JobID;
import org.apache.flink.python.env.PythonDependencyInfo;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.OperatingSystem;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.flink.python.env.beam.ProcessPythonEnvironmentManager.PYFLINK_GATEWAY_DISABLED;
import static org.apache.flink.python.env.beam.ProcessPythonEnvironmentManager.PYTHON_ARCHIVES_DIR;
import static org.apache.flink.python.env.beam.ProcessPythonEnvironmentManager.PYTHON_FILES_DIR;
import static org.apache.flink.python.env.beam.ProcessPythonEnvironmentManager.PYTHON_REQUIREMENTS_CACHE;
import static org.apache.flink.python.env.beam.ProcessPythonEnvironmentManager.PYTHON_REQUIREMENTS_DIR;
import static org.apache.flink.python.env.beam.ProcessPythonEnvironmentManager.PYTHON_REQUIREMENTS_FILE;
import static org.apache.flink.python.env.beam.ProcessPythonEnvironmentManager.PYTHON_REQUIREMENTS_INSTALL_DIR;
import static org.apache.flink.python.env.beam.ProcessPythonEnvironmentManager.PYTHON_WORKING_DIR;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link ProcessPythonEnvironmentManager}. */
public class ProcessPythonEnvironmentManagerTest {

    private static String tmpDir;
    private static boolean isUnix;

    @BeforeClass
    public static void prepareTempDirectory() throws IOException {
        File tmpFile = File.createTempFile("process_environment_manager_test", "");
        if (tmpFile.delete() && tmpFile.mkdirs()) {
            tmpDir = tmpFile.getAbsolutePath();
        } else {
            throw new IOException(
                    "Create temp directory: " + tmpFile.getAbsolutePath() + " failed!");
        }

        for (int i = 0; i < 6; i++) {
            File distributedFile = new File(tmpDir, "file" + i);
            try (FileOutputStream out = new FileOutputStream(distributedFile)) {
                out.write(i);
            }
        }

        for (int i = 0; i < 2; i++) {
            File distributedDirectory = new File(tmpDir, "dir" + i);
            if (distributedDirectory.mkdirs()) {
                for (int j = 0; j < 2; j++) {
                    File fileInDirs = new File(tmpDir, "dir" + i + File.separator + "file" + j);
                    try (FileOutputStream out = new FileOutputStream(fileInDirs)) {
                        out.write(i);
                        out.write(j);
                    }
                }
            } else {
                throw new IOException(
                        "Create temp dir: " + distributedDirectory.getAbsolutePath() + " failed!");
            }
        }

        isUnix =
                OperatingSystem.isFreeBSD()
                        || OperatingSystem.isLinux()
                        || OperatingSystem.isMac()
                        || OperatingSystem.isSolaris();
        for (int i = 0; i < 2; i++) {
            File zipFile = new File(tmpDir, "zip" + i);
            try (ZipArchiveOutputStream zipOut =
                    new ZipArchiveOutputStream(new FileOutputStream(zipFile))) {
                ZipArchiveEntry zipfile0 = new ZipArchiveEntry("zipDir" + i + "/zipfile0");
                zipfile0.setUnixMode(0711);
                zipOut.putArchiveEntry(zipfile0);
                zipOut.write(new byte[] {1, 1, 1, 1, 1});
                zipOut.closeArchiveEntry();
                ZipArchiveEntry zipfile1 = new ZipArchiveEntry("zipDir" + i + "/zipfile1");
                zipfile1.setUnixMode(0644);
                zipOut.putArchiveEntry(zipfile1);
                zipOut.write(new byte[] {2, 2, 2, 2, 2});
                zipOut.closeArchiveEntry();
            }
            File zipExpected =
                    new File(String.join(File.separator, tmpDir, "zipExpected" + i, "zipDir" + i));
            if (!zipExpected.mkdirs()) {
                throw new IOException(
                        "Create temp dir: " + zipExpected.getAbsolutePath() + " failed!");
            }
            File zipfile0 = new File(zipExpected, "zipfile0");
            try (FileOutputStream out = new FileOutputStream(zipfile0)) {
                out.write(new byte[] {1, 1, 1, 1, 1});
            }
            File zipfile1 = new File(zipExpected, "zipfile1");
            try (FileOutputStream out = new FileOutputStream(zipfile1)) {
                out.write(new byte[] {2, 2, 2, 2, 2});
            }

            if (isUnix) {
                if (!(zipfile0.setReadable(true, true)
                        && zipfile0.setWritable(true, true)
                        && zipfile0.setExecutable(true))) {
                    throw new IOException(
                            "Set unixmode 711 to temp file: "
                                    + zipfile0.getAbsolutePath()
                                    + "failed!");
                }
                if (!(zipfile1.setReadable(true)
                        && zipfile1.setWritable(true, true)
                        && zipfile1.setExecutable(false))) {
                    throw new IOException(
                            "Set unixmode 644 to temp file: "
                                    + zipfile1.getAbsolutePath()
                                    + "failed!");
                }
            }
        }
    }

    @AfterClass
    public static void cleanTempDirectory() {
        if (tmpDir != null) {
            FileUtils.deleteDirectoryQuietly(new File(tmpDir));
            tmpDir = null;
        }
    }

    @Test
    public void testPythonFiles() throws Exception {
        // use LinkedHashMap to preserve the path order in environment variable
        Map<String, String> pythonFiles = new LinkedHashMap<>();
        pythonFiles.put(String.join(File.separator, tmpDir, "zip0"), "test_zip.zip");
        pythonFiles.put(String.join(File.separator, tmpDir, "file1"), "test_file1.py");
        pythonFiles.put(String.join(File.separator, tmpDir, "file2"), "test_file2.egg");
        pythonFiles.put(String.join(File.separator, tmpDir, "dir0"), "test_dir");
        PythonDependencyInfo dependencyInfo =
                new PythonDependencyInfo(pythonFiles, null, null, new HashMap<>(), "python");

        try (ProcessPythonEnvironmentManager environmentManager =
                createBasicPythonEnvironmentManager(dependencyInfo)) {
            environmentManager.open();
            String baseDir = environmentManager.getBaseDirectory();
            Map<String, String> environmentVariable = environmentManager.getPythonEnv();

            String[] expectedUserPythonPaths =
                    new String[] {
                        String.join(File.separator, baseDir, PYTHON_FILES_DIR, "zip0", "test_zip"),
                        String.join(File.separator, baseDir, PYTHON_FILES_DIR, "file1"),
                        String.join(
                                File.separator,
                                baseDir,
                                PYTHON_FILES_DIR,
                                "file2",
                                "test_file2.egg"),
                        String.join(File.separator, baseDir, PYTHON_FILES_DIR, "dir0", "test_dir")
                    };
            String expectedPythonPath = String.join(File.pathSeparator, expectedUserPythonPaths);

            assertEquals(expectedPythonPath, environmentVariable.get("PYTHONPATH"));
            assertFileEquals(
                    new File(String.join(File.separator, tmpDir, "file1")),
                    new File(
                            String.join(
                                    File.separator,
                                    baseDir,
                                    PYTHON_FILES_DIR,
                                    "file1",
                                    "test_file1.py")));
            assertFileEquals(
                    new File(String.join(File.separator, tmpDir, "zipExpected0")),
                    new File(
                            String.join(
                                    File.separator,
                                    baseDir,
                                    PYTHON_FILES_DIR,
                                    "zip0",
                                    "test_zip")));
            assertFileEquals(
                    new File(String.join(File.separator, tmpDir, "file2")),
                    new File(
                            String.join(
                                    File.separator,
                                    baseDir,
                                    PYTHON_FILES_DIR,
                                    "file2",
                                    "test_file2.egg")));
            assertFileEquals(
                    new File(String.join(File.separator, tmpDir, "dir0")),
                    new File(
                            String.join(
                                    File.separator,
                                    baseDir,
                                    PYTHON_FILES_DIR,
                                    "dir0",
                                    "test_dir")));
        }
    }

    @Test
    public void testRequirements() throws Exception {
        PythonDependencyInfo dependencyInfo =
                new PythonDependencyInfo(
                        new HashMap<>(),
                        String.join(File.separator, tmpDir, "file0"),
                        String.join(File.separator, tmpDir, "dir0"),
                        new HashMap<>(),
                        "python");

        try (ProcessPythonEnvironmentManager environmentManager =
                createBasicPythonEnvironmentManager(dependencyInfo)) {
            File baseDirectory = new File(tmpDir, "python-dist-" + UUID.randomUUID().toString());
            if (!baseDirectory.mkdirs()) {
                throw new IOException(
                        "Could not find a unique directory name in '"
                                + tmpDir
                                + "' for storing the generated files of python dependency.");
            }
            String tmpBase = baseDirectory.getAbsolutePath();
            Map<String, String> environmentVariable =
                    environmentManager.constructEnvironmentVariables(tmpBase);

            Map<String, String> expected = new HashMap<>();
            expected.put("python", "python");
            expected.put("BOOT_LOG_DIR", tmpBase);
            expected.put(PYFLINK_GATEWAY_DISABLED, "true");
            expected.put(PYTHON_REQUIREMENTS_FILE, String.join(File.separator, tmpDir, "file0"));
            expected.put(PYTHON_REQUIREMENTS_CACHE, String.join(File.separator, tmpDir, "dir0"));
            expected.put(
                    PYTHON_REQUIREMENTS_INSTALL_DIR,
                    String.join(File.separator, tmpBase, PYTHON_REQUIREMENTS_DIR));
            assertEquals(expected, environmentVariable);
        }
    }

    @Test
    public void testArchives() throws Exception {
        // use LinkedHashMap to preserve the file order in python working directory
        Map<String, String> archives = new LinkedHashMap<>();
        archives.put(String.join(File.separator, tmpDir, "zip0"), "py27.zip");
        archives.put(String.join(File.separator, tmpDir, "zip1"), "py37");
        PythonDependencyInfo dependencyInfo =
                new PythonDependencyInfo(new HashMap<>(), null, null, archives, "python");

        try (ProcessPythonEnvironmentManager environmentManager =
                createBasicPythonEnvironmentManager(dependencyInfo)) {
            environmentManager.open();
            String tmpBase = environmentManager.getBaseDirectory();
            Map<String, String> environmentVariable = environmentManager.getPythonEnv();

            Map<String, String> expected = getBasicExpectedEnv(environmentManager);
            expected.put(
                    PYTHON_WORKING_DIR, String.join(File.separator, tmpBase, PYTHON_ARCHIVES_DIR));
            assertEquals(expected, environmentVariable);
            assertFileEquals(
                    new File(String.join(File.separator, tmpDir, "zipExpected0")),
                    new File(String.join(File.separator, tmpBase, PYTHON_ARCHIVES_DIR, "py27.zip")),
                    true);
            assertFileEquals(
                    new File(String.join(File.separator, tmpDir, "zipExpected1")),
                    new File(String.join(File.separator, tmpBase, PYTHON_ARCHIVES_DIR, "py37")),
                    true);
        }
    }

    @Test
    public void testPythonExecutable() throws Exception {
        PythonDependencyInfo dependencyInfo =
                new PythonDependencyInfo(
                        new HashMap<>(), null, null, new HashMap<>(), "/usr/local/bin/python");

        try (ProcessPythonEnvironmentManager environmentManager =
                createBasicPythonEnvironmentManager(dependencyInfo)) {
            environmentManager.open();
            Map<String, String> environmentVariable = environmentManager.getPythonEnv();

            Map<String, String> expected = getBasicExpectedEnv(environmentManager);
            expected.put("python", "/usr/local/bin/python");
            assertEquals(expected, environmentVariable);
        }
    }

    @Test
    public void testCreateRetrievalToken() throws Exception {
        PythonDependencyInfo dependencyInfo =
                new PythonDependencyInfo(new HashMap<>(), null, null, new HashMap<>(), "python");
        Map<String, String> sysEnv = new HashMap<>();
        sysEnv.put("FLINK_HOME", "/flink");

        try (ProcessPythonEnvironmentManager environmentManager =
                new ProcessPythonEnvironmentManager(
                        dependencyInfo, new String[] {tmpDir}, sysEnv, new JobID())) {
            environmentManager.open();
            String retrievalToken = environmentManager.createRetrievalToken();

            File retrievalTokenFile = new File(retrievalToken);
            byte[] content = new byte[(int) retrievalTokenFile.length()];
            try (DataInputStream input = new DataInputStream(new FileInputStream(retrievalToken))) {
                input.readFully(content);
            }
            assertEquals("{\"manifest\": {}}", new String(content));
        }
    }

    @Test
    public void testSetLogDirectory() throws Exception {
        PythonDependencyInfo dependencyInfo =
                new PythonDependencyInfo(new HashMap<>(), null, null, new HashMap<>(), "python");

        try (ProcessPythonEnvironmentManager environmentManager =
                new ProcessPythonEnvironmentManager(
                        dependencyInfo, new String[] {tmpDir}, new HashMap<>(), new JobID())) {
            environmentManager.open();
            Map<String, String> env =
                    environmentManager.constructEnvironmentVariables(
                            environmentManager.getBaseDirectory());
            Map<String, String> expected = getBasicExpectedEnv(environmentManager);
            expected.put("BOOT_LOG_DIR", environmentManager.getBaseDirectory());
            assertEquals(expected, env);
        }
    }

    @Test
    public void testOpenClose() throws Exception {
        PythonDependencyInfo dependencyInfo =
                new PythonDependencyInfo(new HashMap<>(), null, null, new HashMap<>(), "python");

        try (ProcessPythonEnvironmentManager environmentManager =
                createBasicPythonEnvironmentManager(dependencyInfo)) {
            environmentManager.open();
            environmentManager.createRetrievalToken();

            String tmpBase = environmentManager.getBaseDirectory();
            assertTrue(new File(tmpBase).isDirectory());
            environmentManager.close();
            assertFalse(new File(tmpBase).exists());
        }
    }

    private static void assertFileEquals(File expectedFile, File actualFile)
            throws IOException, NoSuchAlgorithmException {
        assertFileEquals(expectedFile, actualFile, false);
    }

    private static void assertFileEquals(File expectedFile, File actualFile, boolean checkUnixMode)
            throws IOException, NoSuchAlgorithmException {
        assertTrue(actualFile.exists());
        assertTrue(expectedFile.exists());
        if (expectedFile.getAbsolutePath().equals(actualFile.getAbsolutePath())) {
            return;
        }

        if (isUnix && checkUnixMode) {
            Set<PosixFilePermission> expectedPerm =
                    Files.getPosixFilePermissions(Paths.get(expectedFile.toURI()));
            Set<PosixFilePermission> actualPerm =
                    Files.getPosixFilePermissions(Paths.get(actualFile.toURI()));
            assertEquals(expectedPerm, actualPerm);
        }

        if (expectedFile.isDirectory()) {
            assertTrue(actualFile.isDirectory());
            String[] expectedSubFiles = expectedFile.list();
            assertArrayEquals(expectedSubFiles, actualFile.list());
            if (expectedSubFiles != null) {
                for (String fileName : expectedSubFiles) {
                    assertFileEquals(
                            new File(expectedFile.getAbsolutePath(), fileName),
                            new File(actualFile.getAbsolutePath(), fileName));
                }
            }
        } else {
            assertEquals(expectedFile.length(), actualFile.length());
            if (expectedFile.length() > 0) {
                assertTrue(org.apache.commons.io.FileUtils.contentEquals(expectedFile, actualFile));
            }
        }
    }

    private static Map<String, String> getBasicExpectedEnv(
            ProcessPythonEnvironmentManager environmentManager) {
        Map<String, String> map = new HashMap<>();
        String tmpBase = environmentManager.getBaseDirectory();
        map.put("python", "python");
        map.put("BOOT_LOG_DIR", tmpBase);
        map.put(PYFLINK_GATEWAY_DISABLED, "true");
        return map;
    }

    private static ProcessPythonEnvironmentManager createBasicPythonEnvironmentManager(
            PythonDependencyInfo dependencyInfo) {
        return new ProcessPythonEnvironmentManager(
                dependencyInfo, new String[] {tmpDir}, new HashMap<>(), new JobID());
    }
}
