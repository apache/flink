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

package org.apache.flink.table.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;

/** Tests for {@link org.apache.flink.table.utils.PartitionPathUtils}. */
class PartitionPathUtilsTest {

    private FileSystem mockFileSystem;

    @BeforeEach
    void setUp() {
        mockFileSystem = mock(FileSystem.class);
    }

    /** Create a mock FileStatus for testing. */
    private FileStatus createMockFileStatus(Path path, boolean isDirectory) {
        FileStatus fileStatus = mock(FileStatus.class);
        when(fileStatus.getPath()).thenReturn(path);
        when(fileStatus.isDir()).thenReturn(isDirectory);
        return fileStatus;
    }

    /** Build a path from components. */
    private Path buildPath(String... components) {
        StringBuilder pathBuilder = new StringBuilder();
        for (int i = 0; i < components.length; i++) {
            if (i > 0) {
                pathBuilder.append(Path.SEPARATOR);
            }
            pathBuilder.append(components[i]);
        }
        return new Path(pathBuilder.toString());
    }

    /** Helper method to create a partition directory structure for testing. */
    private void setupPartitionDirectories(Path basePath, String... partitions) throws IOException {
        FileStatus baseStatus = createMockFileStatus(basePath, true);
        when(mockFileSystem.getFileStatus(basePath)).thenReturn(baseStatus);

        FileStatus[] partitionStatuses = new FileStatus[partitions.length];
        for (int i = 0; i < partitions.length; i++) {
            Path partitionPath = buildPath(basePath.toString(), partitions[i]);
            partitionStatuses[i] = createMockFileStatus(partitionPath, true);
            when(mockFileSystem.getFileStatus(partitionPath)).thenReturn(partitionStatuses[i]);
        }
        when(mockFileSystem.listStatus(basePath)).thenReturn(partitionStatuses);
    }

    /** Helper method to setup files in directory. */
    private void setupFilesInDirectory(Path basePath, String... files) throws IOException {
        FileStatus baseStatus = createMockFileStatus(basePath, true);
        when(mockFileSystem.getFileStatus(basePath)).thenReturn(baseStatus);

        FileStatus[] fileStatuses = new FileStatus[files.length];
        for (int i = 0; i < files.length; i++) {
            Path filePath = buildPath(basePath.toString(), files[i]);
            fileStatuses[i] = createMockFileStatus(filePath, false);
            when(mockFileSystem.getFileStatus(filePath)).thenReturn(fileStatuses[i]);
        }
        when(mockFileSystem.listStatus(basePath)).thenReturn(fileStatuses);
    }

    /** Helper method to setup mixed files and directories. */
    private void setupMixedFilesAndDirectories(Path basePath, String[] directories, String[] files)
            throws IOException {
        FileStatus baseStatus = createMockFileStatus(basePath, true);
        when(mockFileSystem.getFileStatus(basePath)).thenReturn(baseStatus);

        int totalItems = directories.length + files.length;
        FileStatus[] allStatuses = new FileStatus[totalItems];

        int index = 0;
        for (String dir : directories) {
            Path dirPath = buildPath(basePath.toString(), dir);
            allStatuses[index++] = createMockFileStatus(dirPath, true);
            when(mockFileSystem.getFileStatus(dirPath)).thenReturn(allStatuses[index - 1]);
        }

        for (String file : files) {
            Path filePath = buildPath(basePath.toString(), file);
            allStatuses[index++] = createMockFileStatus(filePath, false);
            when(mockFileSystem.getFileStatus(filePath)).thenReturn(allStatuses[index - 1]);
        }

        when(mockFileSystem.listStatus(basePath)).thenReturn(allStatuses);
    }

    @Test
    void testEscapeChar() {
        for (char c = 0; c <= 128; c++) {
            String expected = "%" + String.format("%1$02X", (int) c);
            String actual = PartitionPathUtils.escapeChar(c, new StringBuilder()).toString();
            assertThat(actual).isEqualTo(expected);
        }
    }

    @Test
    void testEscapePathNameWithHeadControl() {
        String origin = "[00";
        String expected = "%5B00";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertThat(actual).isEqualTo(expected);
        assertThat(PartitionPathUtils.unescapePathName(actual)).isEqualTo(origin);
    }

    @Test
    void testEscapePathNameWithTailControl() {
        String origin = "00]";
        String expected = "00%5D";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertThat(actual).isEqualTo(expected);
        assertThat(PartitionPathUtils.unescapePathName(actual)).isEqualTo(origin);
    }

    @Test
    void testEscapePathNameWithMidControl() {
        String origin = "00:00";
        String expected = "00%3A00";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertThat(actual).isEqualTo(expected);
        assertThat(PartitionPathUtils.unescapePathName(actual)).isEqualTo(origin);
    }

    @Test
    void testEscapePathName() {
        String origin = "[00:00]";
        String expected = "%5B00%3A00%5D";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertThat(actual).isEqualTo(expected);
        assertThat(PartitionPathUtils.unescapePathName(actual)).isEqualTo(origin);
    }

    @Test
    void testEscapePathNameWithoutControl() {
        String origin = "0000";
        String expected = "0000";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertThat(actual).isEqualTo(expected);
        assertThat(PartitionPathUtils.unescapePathName(actual)).isEqualTo(origin);
    }

    @Test
    void testEscapePathNameWithCurlyBraces() {
        String origin = "{partName}";
        String expected = "%7BpartName%7D";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertThat(actual).isEqualTo(expected);
        assertThat(PartitionPathUtils.unescapePathName(actual)).isEqualTo(origin);
    }

    @Test
    void testSearchPartSpecAndPathsBasicPartitionStructures() throws Exception {
        // Test single level partition: base/dt=2023/
        Path basePath1 = buildPath("base");
        setupPartitionDirectories(basePath1, "dt=2023");

        List<Tuple2<LinkedHashMap<String, String>, Path>> result1 =
                PartitionPathUtils.searchPartSpecAndPaths(mockFileSystem, basePath1, 1);

        assertThat(result1).hasSize(1);
        assertThat(result1.get(0).f0).containsEntry("dt", "2023");
        assertThat(result1.get(0).f1).isEqualTo(buildPath("base", "dt=2023"));

        // Reset mock for next test
        mockFileSystem = mock(FileSystem.class);

        // Test multiple partitions: base/dt=2023/ and base/dt=2024/
        Path basePath2 = buildPath("base");
        setupPartitionDirectories(basePath2, "dt=2023", "dt=2024");

        List<Tuple2<LinkedHashMap<String, String>, Path>> result2 =
                PartitionPathUtils.searchPartSpecAndPaths(mockFileSystem, basePath2, 1);

        assertThat(result2).hasSize(2);
        assertThat(result2.get(0).f0).containsEntry("dt", "2023");
        assertThat(result2.get(0).f1).isEqualTo(buildPath("base", "dt=2023"));
        assertThat(result2.get(1).f0).containsEntry("dt", "2024");
        assertThat(result2.get(1).f1).isEqualTo(buildPath("base", "dt=2024"));
    }

    @Test
    void testSearchPartSpecAndPathsWithMultiLevelPartition() throws Exception {
        // Test multi level partition: base/dt=2023/hr=10/
        Path basePath = buildPath("base");
        Path dtPath = buildPath("base", "dt=2023");
        Path hrPath = buildPath("base", "dt=2023", "hr=10");

        // Create mocks first
        FileStatus baseStatus = createMockFileStatus(basePath, true);
        FileStatus dtStatus = createMockFileStatus(dtPath, true);
        FileStatus hrStatus = createMockFileStatus(hrPath, true);

        // Mock file system structure
        when(mockFileSystem.getFileStatus(basePath)).thenReturn(baseStatus);
        when(mockFileSystem.getFileStatus(dtPath)).thenReturn(dtStatus);
        when(mockFileSystem.getFileStatus(hrPath)).thenReturn(hrStatus);
        when(mockFileSystem.listStatus(basePath)).thenReturn(new FileStatus[] {dtStatus});
        when(mockFileSystem.listStatus(dtPath)).thenReturn(new FileStatus[] {hrStatus});

        List<Tuple2<LinkedHashMap<String, String>, Path>> result =
                PartitionPathUtils.searchPartSpecAndPaths(mockFileSystem, basePath, 2);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).f0).containsEntry("dt", "2023");
        assertThat(result.get(0).f0).containsEntry("hr", "10");
        assertThat(result.get(0).f1).isEqualTo(hrPath);
    }

    @Test
    void testSearchPartSpecAndPathsFiltersHiddenFiles() throws Exception {
        // Test filtering hidden files: base/dt=2023/, base/dt=2024/, base/_hidden/, base/.hidden/
        Path basePath = buildPath("base");
        Path partitionPath = buildPath("base", "dt=2023");
        Path partitionPath2 = buildPath("base", "dt=2024");
        Path partitionPathWithFile = buildPath("base", "dt=2024", "data1.parquet");
        Path hiddenPath1 = buildPath("base", "_hidden");
        Path hiddenPath2 = buildPath("base", ".hidden");
        Path hiddenPath3 = buildPath("base", "_hidden", "data1.parquet");
        Path hiddenPath4 = buildPath("base", ".hidden", "data1.parquet");

        // Create mocks first
        FileStatus baseStatus = createMockFileStatus(basePath, true);
        FileStatus partitionStatus = createMockFileStatus(partitionPath, true);
        FileStatus partitionStatus2 = createMockFileStatus(partitionPath2, true);
        FileStatus partitionStatusWithFile = createMockFileStatus(partitionPathWithFile, false);
        FileStatus hiddenStatus1 = createMockFileStatus(hiddenPath1, true);
        FileStatus hiddenStatus2 = createMockFileStatus(hiddenPath2, true);
        FileStatus hiddenStatus3 = createMockFileStatus(hiddenPath3, false);
        FileStatus hiddenStatus4 = createMockFileStatus(hiddenPath4, false);

        // Mock file system structure
        when(mockFileSystem.getFileStatus(basePath)).thenReturn(baseStatus);
        when(mockFileSystem.getFileStatus(partitionPath)).thenReturn(partitionStatus);
        when(mockFileSystem.getFileStatus(partitionPath2)).thenReturn(partitionStatus2);
        when(mockFileSystem.getFileStatus(partitionPathWithFile))
                .thenReturn(partitionStatusWithFile);
        when(mockFileSystem.getFileStatus(hiddenPath1)).thenReturn(hiddenStatus1);
        when(mockFileSystem.getFileStatus(hiddenPath2)).thenReturn(hiddenStatus2);
        when(mockFileSystem.getFileStatus(hiddenPath3)).thenReturn(hiddenStatus3);
        when(mockFileSystem.getFileStatus(hiddenPath4)).thenReturn(hiddenStatus4);
        when(mockFileSystem.listStatus(basePath))
                .thenReturn(
                        new FileStatus[] {
                            partitionStatus, partitionStatus2, hiddenStatus1, hiddenStatus2
                        });
        when(mockFileSystem.listStatus(partitionPath2))
                .thenReturn(new FileStatus[] {partitionStatusWithFile});
        when(mockFileSystem.listStatus(hiddenPath1)).thenReturn(new FileStatus[] {hiddenStatus3});
        when(mockFileSystem.listStatus(hiddenPath2)).thenReturn(new FileStatus[] {hiddenStatus4});

        List<Tuple2<LinkedHashMap<String, String>, Path>> result =
                PartitionPathUtils.searchPartSpecAndPaths(mockFileSystem, basePath, 1);

        // Both non-hidden partitions should be returned
        assertThat(result).hasSize(2);
        assertThat(result.get(0).f0).containsEntry("dt", "2023");
        assertThat(result.get(0).f1).isEqualTo(partitionPath);
        assertThat(result.get(1).f0).containsEntry("dt", "2024");
        assertThat(result.get(1).f1).isEqualTo(partitionPath2);
    }

    @Test
    void testSearchPartSpecAndPathsWithEmptyDirectory() throws Exception {
        // Test with empty directory
        Path basePath = buildPath("base");

        // Create mock first
        FileStatus baseStatus = createMockFileStatus(basePath, true);

        // Mock file system structure with empty directory
        when(mockFileSystem.getFileStatus(basePath)).thenReturn(baseStatus);
        when(mockFileSystem.listStatus(basePath)).thenReturn(new FileStatus[0]);

        List<Tuple2<LinkedHashMap<String, String>, Path>> result =
                PartitionPathUtils.searchPartSpecAndPaths(mockFileSystem, basePath, 1);

        assertThat(result).isEmpty();
    }

    @Test
    void testSearchPartSpecAndPathsWithNonExistentPath() throws Exception {
        // Test with non-existent path
        Path nonExistentPath = buildPath("non", "existent");

        // Mock file system to throw IOException for non-existent path
        doThrow(new IOException("Path does not exist"))
                .when(mockFileSystem)
                .getFileStatus(nonExistentPath);

        List<Tuple2<LinkedHashMap<String, String>, Path>> result =
                PartitionPathUtils.searchPartSpecAndPaths(mockFileSystem, nonExistentPath, 1);

        // Should return empty list for non-existent path
        assertThat(result).isEmpty();
    }

    @Test
    void testSearchPartSpecAndPathsWithFilesAndDirectories() throws Exception {
        // Test directory containing both files and partition directories
        Path basePath = buildPath("base");
        String[] directories = {"dt=2023"};
        String[] files = {"data1.parquet", "data2.csv"};

        setupMixedFilesAndDirectories(basePath, directories, files);

        List<Tuple2<LinkedHashMap<String, String>, Path>> result =
                PartitionPathUtils.searchPartSpecAndPaths(mockFileSystem, basePath, 1);

        // Should find the partition directory and files (current behavior includes all items at
        // level)
        assertThat(result).hasSize(3);

        // Check partition directory
        assertThat(result.get(0).f0).containsEntry("dt", "2023");
        assertThat(result.get(0).f1).isEqualTo(buildPath("base", "dt=2023"));

        // Check files (empty partition specs)
        assertThat(result.get(1).f0).isEmpty();
        assertThat(result.get(1).f1).isEqualTo(buildPath("base", "data1.parquet"));
        assertThat(result.get(2).f0).isEmpty();
        assertThat(result.get(2).f1).isEqualTo(buildPath("base", "data2.csv"));
    }

    @Test
    void testSearchPartSpecAndPathsWithFilesOnly() throws Exception {
        // Test directory containing only files, no partition directories
        Path basePath = buildPath("base");
        setupFilesInDirectory(basePath, "data1.parquet", "data2.csv");

        List<Tuple2<LinkedHashMap<String, String>, Path>> result =
                PartitionPathUtils.searchPartSpecAndPaths(mockFileSystem, basePath, 1);

        // Should find only files with empty partition specs
        assertThat(result).hasSize(2);
        assertThat(result.get(0).f0).isEmpty();
        assertThat(result.get(0).f1).isEqualTo(buildPath("base", "data1.parquet"));
        assertThat(result.get(1).f0).isEmpty();
        assertThat(result.get(1).f1).isEqualTo(buildPath("base", "data2.csv"));
    }

    @Test
    void testSearchPartSpecAndPathsWithFilesInPartitionDirectory() throws Exception {
        // Test partition directory containing files
        Path basePath = buildPath("base");
        Path partitionPath = buildPath("base", "dt=2023");
        Path fileInPartition = buildPath("base", "dt=2023", "data.parquet");

        // Create mocks
        FileStatus baseStatus = createMockFileStatus(basePath, true);
        FileStatus partitionStatus = createMockFileStatus(partitionPath, true);
        FileStatus fileStatus = createMockFileStatus(fileInPartition, false);

        // Mock file system structure - partition directory contains files
        when(mockFileSystem.getFileStatus(basePath)).thenReturn(baseStatus);
        when(mockFileSystem.getFileStatus(partitionPath)).thenReturn(partitionStatus);
        when(mockFileSystem.getFileStatus(fileInPartition)).thenReturn(fileStatus);
        when(mockFileSystem.listStatus(basePath)).thenReturn(new FileStatus[] {partitionStatus});
        when(mockFileSystem.listStatus(partitionPath)).thenReturn(new FileStatus[] {fileStatus});

        List<Tuple2<LinkedHashMap<String, String>, Path>> result =
                PartitionPathUtils.searchPartSpecAndPaths(mockFileSystem, basePath, 1);

        // Should find the partition directory (files inside partition don't matter for this search
        // level)
        assertThat(result).hasSize(1);
        assertThat(result.get(0).f0).containsEntry("dt", "2023");
        assertThat(result.get(0).f1).isEqualTo(partitionPath);
    }

    @Test
    void testSearchPartSpecAndPathsWithDeepDirectoryNoPartitions() throws Exception {
        // Test deep directory structure with no partitions
        Path basePath = buildPath("base");
        Path level1Path = buildPath("base", "level1");
        Path level2Path = buildPath("base", "level1", "level2");
        Path filePath = buildPath("base", "level1", "level2", "data.txt");

        // Create mocks
        FileStatus baseStatus = createMockFileStatus(basePath, true);
        FileStatus level1Status = createMockFileStatus(level1Path, true);
        FileStatus level2Status = createMockFileStatus(level2Path, true);
        FileStatus fileStatus = createMockFileStatus(filePath, false);

        // Mock file system structure - deep directories but no partitions
        when(mockFileSystem.getFileStatus(basePath)).thenReturn(baseStatus);
        when(mockFileSystem.getFileStatus(level1Path)).thenReturn(level1Status);
        when(mockFileSystem.getFileStatus(level2Path)).thenReturn(level2Status);
        when(mockFileSystem.getFileStatus(filePath)).thenReturn(fileStatus);
        when(mockFileSystem.listStatus(basePath)).thenReturn(new FileStatus[] {level1Status});
        when(mockFileSystem.listStatus(level1Path)).thenReturn(new FileStatus[] {level2Status});
        when(mockFileSystem.listStatus(level2Path)).thenReturn(new FileStatus[] {fileStatus});

        List<Tuple2<LinkedHashMap<String, String>, Path>> result =
                PartitionPathUtils.searchPartSpecAndPaths(mockFileSystem, basePath, 3);

        // Should find the file at the deep level (no partitions in this structure)
        assertThat(result).hasSize(1);
        assertThat(result.get(0).f0).isEmpty(); // No partition spec
        assertThat(result.get(0).f1).isEqualTo(filePath);
    }


  @Test
    void testSearchPartSpecAndPathsWithComplexHiddenFileStructure() throws Exception {
        // Test complex structure: base/dt=2023/hr=01/data1.csv, base/.hidden/.data2.csv, base/.hidden/data1.csv
        Path basePath = buildPath("base");
        Path partitionPath1 = buildPath("base", "dt=2023");
        Path partitionPath2 = buildPath("base", "dt=2023", "hr=01");
        Path validFile = buildPath("base", "dt=2023", "hr=01", "data1.csv");
        Path hiddenDir1 = buildPath("base", ".hidden");
        Path hiddenDir2 = buildPath("base", ".hidden", ".data2.csv");
        Path hiddenDir3 = buildPath("base", ".hidden", "data1.csv");

        // Create mocks
        FileStatus baseStatus = createMockFileStatus(basePath, true);
        FileStatus partitionStatus1 = createMockFileStatus(partitionPath1, true);
        FileStatus partitionStatus2 = createMockFileStatus(partitionPath2, true);
        FileStatus validFileStatus = createMockFileStatus(validFile, false);
        FileStatus hiddenDirStatus1 = createMockFileStatus(hiddenDir1, true);
        FileStatus hiddenDirStatus2 = createMockFileStatus(hiddenDir2, false);
        FileStatus hiddenDirStatus3 = createMockFileStatus(hiddenDir3, false);

        // Mock file system structure
        when(mockFileSystem.getFileStatus(basePath)).thenReturn(baseStatus);
        when(mockFileSystem.getFileStatus(partitionPath1)).thenReturn(partitionStatus1);
        when(mockFileSystem.getFileStatus(partitionPath2)).thenReturn(partitionStatus2);
        when(mockFileSystem.getFileStatus(validFile)).thenReturn(validFileStatus);
        when(mockFileSystem.getFileStatus(hiddenDir1)).thenReturn(hiddenDirStatus1);
        when(mockFileSystem.getFileStatus(hiddenDir2)).thenReturn(hiddenDirStatus2);
        when(mockFileSystem.getFileStatus(hiddenDir3)).thenReturn(hiddenDirStatus3);

        when(mockFileSystem.listStatus(basePath))
                .thenReturn(new FileStatus[] {partitionStatus1, hiddenDirStatus1});
        when(mockFileSystem.listStatus(partitionPath1))
                .thenReturn(new FileStatus[] {partitionStatus2});
        when(mockFileSystem.listStatus(partitionPath2))
                .thenReturn(new FileStatus[] {validFileStatus});
        when(mockFileSystem.listStatus(hiddenDir1))
                .thenReturn(new FileStatus[] {hiddenDirStatus2, hiddenDirStatus3});

        List<Tuple2<LinkedHashMap<String, String>, Path>> result =
                PartitionPathUtils.searchPartSpecAndPaths(mockFileSystem, basePath, 2);

        // Should only find the valid partition path, hidden files and directories should be filtered out
        assertThat(result).hasSize(1);
        assertThat(result.get(0).f0).containsEntry("dt", "2023");
        assertThat(result.get(0).f0).containsEntry("hr", "01");
        assertThat(result.get(0).f1).isEqualTo(partitionPath2);
    }
}
