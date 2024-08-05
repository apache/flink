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

package org.apache.flink.connector.file.src.assigners;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.Path;

import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/** Unit tests for the {@link FixedFileSplitAssigner}. */
public class FixedFileSplitAssignerTest {
    private static final Path TEST_PATH =
            Path.fromLocalFile(new File(System.getProperty("java.io.tmpdir")));

    @Test
    public void testRandomAssignment() {
        Random random = new Random();
        int splitCount = random.nextInt(20) + 1;
        int taskParallelism = random.nextInt(20) + 1;
        Collection<FileSourceSplit> sourceSplits = genFileSourceSplits(splitCount);
        FixedFileSplitAssigner fixedFileSplitAssigner = new FixedFileSplitAssigner(sourceSplits);
        Set<String> expectIdSet =
                sourceSplits.stream().map(FileSourceSplit::splitId).collect(Collectors.toSet());
        fixedFileSplitAssigner.setTaskParallelism(taskParallelism);
        List<List<FileSourceSplit>> allSplits =
                collectAllSplits(taskParallelism, fixedFileSplitAssigner);
        assertEquals(splitCount, allSplits.stream().mapToInt(List::size).sum());
        assertEquals(expectIdSet, getAllSplitsIdFromListList(allSplits));
    }

    @Test
    public void testReturnSplits() {
        int splitCount = 4;
        int taskParallelism = 2;
        Collection<FileSourceSplit> sourceSplits = genFileSourceSplits(splitCount);
        FixedFileSplitAssigner fixedFileSplitAssigner = new FixedFileSplitAssigner(sourceSplits);
        fixedFileSplitAssigner.setTaskParallelism(taskParallelism);
        List<List<FileSourceSplit>> allSplits =
                collectAllSplits(taskParallelism, fixedFileSplitAssigner);
        assertEquals(2, allSplits.size());
        assertEquals(2, allSplits.get(0).size());
        assertEquals(2, allSplits.get(1).size());

        // Add all back for one task
        List<FileSourceSplit> splitsBack = allSplits.get(0);
        fixedFileSplitAssigner.addSplits(splitsBack);
        List<List<FileSourceSplit>> newAllSplits =
                collectAllSplits(taskParallelism, fixedFileSplitAssigner);
        assertEquals(2, newAllSplits.size());
        assertEquals(2, newAllSplits.get(0).size());
        assertEquals(0, newAllSplits.get(1).size());
        assertEquals(getAllSplitsIdFromList(splitsBack), getAllSplitsIdFromList(allSplits.get(0)));

        // Add some back for one task
        splitsBack = Arrays.asList(allSplits.get(1).get(0));
        fixedFileSplitAssigner.addSplits(splitsBack);
        newAllSplits = collectAllSplits(taskParallelism, fixedFileSplitAssigner);
        assertEquals(1, newAllSplits.get(1).size());
        assertEquals(
                getAllSplitsIdFromList(splitsBack), getAllSplitsIdFromList(newAllSplits.get(1)));
    }

    @Test
    public void testAssignmentWithDifferentSize() {
        int taskParallelism = 4;
        long MB_BYTES = 1024 * 1024;
        int fileIndex = 0;
        List<FileSourceSplit> sourceSplits = new ArrayList<>();
        sourceSplits.add(createFileSourceSplit(fileIndex++, 100 * MB_BYTES));
        sourceSplits.add(createFileSourceSplit(fileIndex++, MB_BYTES));
        sourceSplits.add(createFileSourceSplit(fileIndex++, MB_BYTES));
        sourceSplits.add(createFileSourceSplit(fileIndex++, MB_BYTES));
        sourceSplits.add(createFileSourceSplit(fileIndex++, MB_BYTES));
        sourceSplits.add(createFileSourceSplit(fileIndex++, MB_BYTES * 20));
        sourceSplits.add(createFileSourceSplit(fileIndex, MB_BYTES * 7));

        FixedFileSplitAssigner fixedFileSplitAssigner = new FixedFileSplitAssigner(sourceSplits);
        fixedFileSplitAssigner.setTaskParallelism(taskParallelism);
        List<List<FileSourceSplit>> allSplits =
                collectAllSplits(taskParallelism, fixedFileSplitAssigner);
        assertEquals(1, allSplits.get(0).size());
        assertEquals(1, allSplits.get(1).size());
        assertEquals(2, allSplits.get(2).size());
        assertEquals(3, allSplits.get(3).size());
        assertEquals(100 * MB_BYTES, allSplits.get(0).get(0).length());
        assertEquals(20 * MB_BYTES, allSplits.get(1).get(0).length());
        assertEquals(
                8 * MB_BYTES, allSplits.get(2).stream().mapToLong(FileSourceSplit::length).sum());
    }

    private Set<String> getAllSplitsIdFromListList(List<List<FileSourceSplit>> allSplits) {
        return allSplits.stream()
                .flatMap(List::stream)
                .map(FileSourceSplit::splitId)
                .collect(Collectors.toSet());
    }

    private Set<String> getAllSplitsIdFromList(List<FileSourceSplit> allSplits) {
        return allSplits.stream().map(FileSourceSplit::splitId).collect(Collectors.toSet());
    }

    private List<List<FileSourceSplit>> collectAllSplits(
            int taskParallelism, FixedFileSplitAssigner fixedFileSplitAssigner) {
        List<List<FileSourceSplit>> tasksSplits = new ArrayList<>();
        for (int i = 0; i < taskParallelism; i++) {
            List<FileSourceSplit> taskSplits = new ArrayList<>();

            while (true) {
                Optional<FileSourceSplit> fileSourceSplit = fixedFileSplitAssigner.getNext(null, i);
                if (fileSourceSplit.isPresent()) {
                    taskSplits.add(fileSourceSplit.get());
                } else {
                    break;
                }
            }
            tasksSplits.add(taskSplits);
        }
        return tasksSplits;
    }

    private FileSourceSplit createFileSourceSplit(int splitId, long length) {
        return new FileSourceSplit(String.valueOf(splitId), TEST_PATH, 0, length, 0, length);
    }

    private Collection<FileSourceSplit> genFileSourceSplits(int count) {
        List<FileSourceSplit> fileSourceSplitList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            fileSourceSplitList.add(createFileSourceSplit(i, 1024));
        }
        return fileSourceSplitList;
    }
}
