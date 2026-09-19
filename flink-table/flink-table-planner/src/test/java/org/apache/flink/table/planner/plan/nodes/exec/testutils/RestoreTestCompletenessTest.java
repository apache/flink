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
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNestedLoopJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonAsyncCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonCorrelate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonGroupTableAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonGroupWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecPythonOverAggregate;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil.ExecNodeNameVersion;

import org.apache.flink.shaded.guava33.com.google.common.reflect.ClassPath;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.fail;

/** Validate restore tests exists for Exec Nodes. */
class RestoreTestCompletenessTest {

    private static final Set<Class<? extends ExecNode<?>>> SKIP_EXEC_NODES =
            Set.of(
                    /* Ignoring python based exec nodes temporarily. */
                    StreamExecPythonCalc.class,
                    StreamExecPythonCorrelate.class,
                    StreamExecPythonOverAggregate.class,
                    StreamExecPythonGroupAggregate.class,
                    StreamExecPythonGroupTableAggregate.class,
                    StreamExecPythonGroupWindowAggregate.class,
                    StreamExecPythonAsyncCalc.class,

                    // Covered by tests in WindowAggregateEventTimeRestoreTest
                    StreamExecLocalWindowAggregate.class,
                    StreamExecGlobalWindowAggregate.class,

                    // There is jira for these 2 batch tests
                    // https://issues.apache.org/jira/browse/FLINK-40306
                    BatchExecHashAggregate.class,
                    BatchExecNestedLoopJoin.class);

    private Class<? extends ExecNode<?>> getExecNode(Class<?> restoreTest)
            throws NoSuchMethodException,
                    InvocationTargetException,
                    InstantiationException,
                    IllegalAccessException {
        Method getExecNodeMethod = restoreTest.getMethod("getExecNode");
        Class<? extends ExecNode<?>> execNode =
                (Class<? extends ExecNode<?>>)
                        getExecNodeMethod.invoke(
                                restoreTest.getDeclaredConstructor().newInstance());
        return execNode;
    }

    private List<Class<? extends ExecNode<?>>> getChildExecNodes(Class<?> restoreTest)
            throws NoSuchMethodException,
                    InvocationTargetException,
                    InstantiationException,
                    IllegalAccessException {
        Method getChildExecNodesMethod = restoreTest.getMethod("getChildExecNodes");
        List<Class<? extends ExecNode<?>>> childExecNodes =
                (List<Class<? extends ExecNode<?>>>)
                        getChildExecNodesMethod.invoke(
                                restoreTest.getDeclaredConstructor().newInstance());
        return childExecNodes;
    }

    @Test
    void testMissingRestoreTest()
            throws IOException,
                    NoSuchMethodException,
                    InstantiationException,
                    IllegalAccessException,
                    InvocationTargetException {
        Map<ExecNodeNameVersion, Class<? extends ExecNode<?>>> versionedExecNodes =
                ExecNodeMetadataUtil.getVersionedExecNodes();

        Set<ClassPath.ClassInfo> classesInPackage =
                new HashSet<>(
                        gatherClasses(
                                RestoreTestBase.class,
                                "org.apache.flink.table.planner.plan.nodes.exec.stream"));
        classesInPackage.addAll(
                gatherClasses(
                        BatchRestoreTestBase.class,
                        "org.apache.flink.table.planner.plan.nodes.exec.batch"));

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

        Set<Class<? extends ExecNode<?>>> productionExecNodes = ExecNodeMetadataUtil.execNodes();
        for (Map.Entry<ExecNodeNameVersion, Class<? extends ExecNode<?>>> entry :
                versionedExecNodes.entrySet()) {
            ExecNodeNameVersion execNodeNameVersion = entry.getKey();
            Class<? extends ExecNode<?>> execNode = entry.getValue();
            // Ignore test-only nodes that other tests leak into the shared LOOKUP_MAP via
            // addTestNode().
            if (!productionExecNodes.contains(execNode)) {
                continue;
            }
            if (!SKIP_EXEC_NODES.contains(execNode)
                    && !execNodesWithRestoreTests.contains(execNode)) {
                fail(
                        "Missing restore test for "
                                + execNodeNameVersion
                                + "\nPlease add a restore test for "
                                + execNode.toString());
            }
        }
    }

    private Set<ClassPath.ClassInfo> gatherClasses(Class<?> clazz, String packageName)
            throws IOException {
        return ClassPath.from(this.getClass().getClassLoader())
                .getTopLevelClassesRecursive(packageName)
                .stream()
                .filter(x -> clazz.isAssignableFrom(x.load()))
                .collect(Collectors.toSet());
    }
}
