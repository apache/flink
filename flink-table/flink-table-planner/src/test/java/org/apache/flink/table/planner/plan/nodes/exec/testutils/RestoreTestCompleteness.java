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

package org.apache.flink.table.planner.plan.nodes.exec.testutils;

import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecInputAdapter;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecMatch;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecMultipleInput;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNestedLoopJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecOverAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecPythonCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecPythonGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecPythonGroupWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecPythonOverAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecRank;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecScriptTransform;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortLimit;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortMergeJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecWindowTableFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonGroupTableAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonGroupWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonOverAggregate;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil.ExecNodeNameVersion;

import org.apache.flink.shaded.guava32.com.google.common.reflect.ClassPath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Validate restore tests exists for Exec Nodes. */
public class RestoreTestCompleteness {

    private static final Set<Class<? extends ExecNode<?>>> SKIP_EXEC_NODES =
            new HashSet<Class<? extends ExecNode<?>>>() {
                {
                    /** Ignoring python based exec nodes temporarily. */
                    add(StreamExecPythonCalc.class);
                    add(StreamExecPythonCorrelate.class);
                    add(StreamExecPythonOverAggregate.class);
                    add(StreamExecPythonGroupAggregate.class);
                    add(StreamExecPythonGroupTableAggregate.class);
                    add(StreamExecPythonGroupWindowAggregate.class);

                    add(BatchExecPythonCorrelate.class);
                    add(BatchExecPythonGroupAggregate.class);
                    add(BatchExecPythonGroupWindowAggregate.class);
                    add(BatchExecPythonOverAggregate.class);

                    add(BatchExecHashAggregate.class);
                    add(BatchExecSortAggregate.class);
                    add(BatchExecScriptTransform.class);
                    add(BatchExecMultipleInput.class);

                    add(BatchExecNestedLoopJoin.class);
                    add(BatchExecMatch.class);
                    add(BatchExecHashWindowAggregate.class);
                    add(BatchExecInputAdapter.class);
                    add(BatchExecOverAggregate.class);

                    add(BatchExecRank.class);
                    add(BatchExecSortLimit.class);
                    add(BatchExecSortMergeJoin.class);
                    add(BatchExecSortWindowAggregate.class);
                    add(BatchExecWindowTableFunction.class);
                }
            };

    private Class<? extends ExecNode<?>> getExecNode(Class<?> restoreTest)
            throws NoSuchMethodException, InvocationTargetException, InstantiationException,
                    IllegalAccessException {
        Method getExecNodeMethod = restoreTest.getMethod("getExecNode");
        Class<? extends ExecNode<?>> execNode =
                (Class<? extends ExecNode<?>>)
                        getExecNodeMethod.invoke(
                                restoreTest.getDeclaredConstructor().newInstance());
        return execNode;
    }

    private List<Class<? extends ExecNode<?>>> getChildExecNodes(Class<?> restoreTest)
            throws NoSuchMethodException, InvocationTargetException, InstantiationException,
                    IllegalAccessException {
        Method getChildExecNodesMethod = restoreTest.getMethod("getChildExecNodes");
        List<Class<? extends ExecNode<?>>> childExecNodes =
                (List<Class<? extends ExecNode<?>>>)
                        getChildExecNodesMethod.invoke(
                                restoreTest.getDeclaredConstructor().newInstance());
        return childExecNodes;
    }

    @Test
    public void testMissingRestoreTest()
            throws IOException, NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException {
        Map<ExecNodeNameVersion, Class<? extends ExecNode<?>>> versionedExecNodes =
                ExecNodeMetadataUtil.getVersionedExecNodes();

        Set<ClassPath.ClassInfo> classesInPackage =
                ClassPath.from(this.getClass().getClassLoader())
                        .getTopLevelClassesRecursive(
                                "org.apache.flink.table.planner.plan.nodes.exec.stream")
                        .stream()
                        .filter(x -> RestoreTestBase.class.isAssignableFrom(x.load()))
                        .collect(Collectors.toSet());

        Set<Class<? extends ExecNode<?>>> execNodesWithRestoreTests = new HashSet<>();

        for (ClassPath.ClassInfo classInfo : classesInPackage) {
            Class<?> restoreTest = classInfo.load();

            Class<? extends ExecNode<?>> execNode = getExecNode(restoreTest);
            execNodesWithRestoreTests.add(execNode);

            List<Class<? extends ExecNode<?>>> childExecNodes = getChildExecNodes(restoreTest);
            for (Class<? extends ExecNode<?>> childExecNode : childExecNodes) {
                execNodesWithRestoreTests.add(childExecNode);
            }
        }

        Set<ClassPath.ClassInfo> batchClassesInPackage =
                ClassPath.from(this.getClass().getClassLoader())
                        .getTopLevelClassesRecursive(
                                "org.apache.flink.table.planner.plan.nodes.exec.batch")
                        .stream()
                        .filter(x -> BatchCompiledPlanTestBase.class.isAssignableFrom(x.load()))
                        .collect(Collectors.toSet());

        //        Set<Class<? extends ExecNode<?>>> execNodesWithRestoreTests = new HashSet<>();

        for (ClassPath.ClassInfo classInfo : batchClassesInPackage) {
            Class<?> restoreTest = classInfo.load();

            Class<? extends ExecNode<?>> execNode = getExecNode(restoreTest);
            execNodesWithRestoreTests.add(execNode);

            List<Class<? extends ExecNode<?>>> childExecNodes = getChildExecNodes(restoreTest);
            for (Class<? extends ExecNode<?>> childExecNode : childExecNodes) {
                execNodesWithRestoreTests.add(childExecNode);
            }
        }

        System.out.println(
                execNodesWithRestoreTests.stream()
                        .map(Objects::toString)
                        .collect(Collectors.joining(", ")));
        for (Map.Entry<ExecNodeNameVersion, Class<? extends ExecNode<?>>> entry :
                versionedExecNodes.entrySet()) {
            ExecNodeNameVersion execNodeNameVersion = entry.getKey();
            System.out.println(execNodeNameVersion);
        }

        for (Map.Entry<ExecNodeNameVersion, Class<? extends ExecNode<?>>> entry :
                versionedExecNodes.entrySet()) {
            ExecNodeNameVersion execNodeNameVersion = entry.getKey();
            Class<? extends ExecNode<?>> execNode = entry.getValue();
            if (!SKIP_EXEC_NODES.contains(execNode)) {
                final String msg =
                        "Missing restore test for "
                                + execNodeNameVersion
                                + "\nPlease add a restore test for "
                                + execNode.toString();
                Assertions.assertTrue(execNodesWithRestoreTests.contains(execNode), msg);
            }
        }
    }
}
