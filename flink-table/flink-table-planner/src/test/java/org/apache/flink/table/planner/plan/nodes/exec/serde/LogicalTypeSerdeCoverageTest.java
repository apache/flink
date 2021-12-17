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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.table.planner.plan.utils.ReflectionsUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.UnresolvedUserDefinedType;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

/**
 * Test to check whether all {@link LogicalType}s have been tested in {@link LogicalTypeSerdeTest}.
 */
public class LogicalTypeSerdeCoverageTest {

    @Test
    public void testLogicalTypeJsonSerdeCoverage() {
        Set<Class<? extends LogicalType>> allSubClasses =
                ReflectionsUtil.scanSubClasses("org.apache.flink", LogicalType.class);

        Set<Class<? extends LogicalType>> remainingSubClasses = new HashSet<>(allSubClasses);
        // An unresolved user-defined type needs to be resolved into a proper user-defined type,
        // so we ignore it here
        remainingSubClasses.remove(UnresolvedUserDefinedType.class);
        Set<Class<? extends LogicalType>> testedSubClasses = new HashSet<>();
        for (LogicalType logicalType : LogicalTypeSerdeTest.testData()) {
            testedSubClasses.add(logicalType.getClass());
        }
        remainingSubClasses.removeAll(testedSubClasses);
        assertTrue(
                String.format(
                        "[%s] are not tested in LogicalTypeSerdeTest.",
                        remainingSubClasses.stream()
                                .map(Class::getSimpleName)
                                .collect(Collectors.joining(","))),
                remainingSubClasses.isEmpty());
    }
}
