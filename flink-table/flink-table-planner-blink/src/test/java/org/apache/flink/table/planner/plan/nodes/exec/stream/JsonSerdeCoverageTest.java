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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil;
import org.apache.flink.table.planner.plan.utils.ReflectionsUtil;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

/**
 * Test to check whether all {@link StreamExecNode}s have been implemented json
 * serialization/deserialization.
 */
public class JsonSerdeCoverageTest {

    private static final List<String> UNSUPPORTED_JSON_SERDE_CLASSES =
            Arrays.asList(
                    "StreamExecDataStreamScan",
                    "StreamExecLegacyTableSourceScan",
                    "StreamExecLegacySink",
                    "StreamExecLookupJoin",
                    "StreamExecPythonGroupAggregate",
                    "StreamExecWindowTableFunction",
                    "StreamExecPythonGroupWindowAggregate",
                    "StreamExecGroupTableAggregate",
                    "StreamExecPythonGroupTableAggregate",
                    "StreamExecPythonOverAggregate",
                    "StreamExecPythonCorrelate",
                    "StreamExecPythonCalc",
                    "StreamExecSort",
                    "StreamExecMultipleInput",
                    "StreamExecValues");

    @SuppressWarnings({"rawtypes"})
    @Test
    public void testStreamExecNodeJsonSerdeCoverage() {
        Set<Class<? extends StreamExecNode>> subClasses =
                ReflectionsUtil.scanSubClasses("org.apache.flink", StreamExecNode.class);

        List<String> classes = new ArrayList<>();
        List<String> classesWithoutJsonCreator = new ArrayList<>();
        List<String> classesWithJsonCreatorInUnsupportedList = new ArrayList<>();
        for (Class<? extends StreamExecNode> clazz : subClasses) {
            String className = clazz.getSimpleName();
            classes.add(className);
            boolean hasJsonCreator = JsonSerdeUtil.hasJsonCreatorAnnotation(clazz);
            if (hasJsonCreator && UNSUPPORTED_JSON_SERDE_CLASSES.contains(className)) {
                classesWithJsonCreatorInUnsupportedList.add(className);
            }
            if (!hasJsonCreator && !UNSUPPORTED_JSON_SERDE_CLASSES.contains(className)) {
                classesWithoutJsonCreator.add(className);
            }
        }
        assertTrue(
                String.format(
                        "%s do not support json serialization/deserialization, "
                                + "please refer the implementation of the other StreamExecNodes.",
                        String.join(",", classesWithoutJsonCreator)),
                classesWithoutJsonCreator.isEmpty());
        assertTrue(
                String.format(
                        "%s have support json serialization/deserialization, "
                                + "but still in UNSUPPORTED_JSON_SERDE_CLASSES list. "
                                + "please move them from UNSUPPORTED_JSON_SERDE_CLASSES.",
                        String.join(",", classesWithJsonCreatorInUnsupportedList)),
                classesWithJsonCreatorInUnsupportedList.isEmpty());
        List<String> notExistingClasses =
                UNSUPPORTED_JSON_SERDE_CLASSES.stream()
                        .filter(c -> !classes.contains(c))
                        .collect(Collectors.toList());
        assertTrue(
                String.format(
                        "%s do not exist any more, please remove them from UNSUPPORTED_JSON_SERDE_CLASSES.",
                        String.join(",", notExistingClasses)),
                notExistingClasses.isEmpty());
    }
}
