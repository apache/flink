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

import org.apache.flink.table.planner.plan.nodes.exec.testutils.SemanticTestBase;
import org.apache.flink.table.runtime.operators.sink.constraint.ConstraintEnforcer;
import org.apache.flink.table.test.program.TableTestProgram;

import java.util.List;

/** Semantic tests for {@link ConstraintEnforcer}. */
public class ConstraintEnforcerSemanticTests extends SemanticTestBase {

    @Override
    public List<TableTestProgram> programs() {
        return List.of(
                ConstraintEnforcerTestPrograms.NOT_NULL_DROP_NESTED_ROWS,
                ConstraintEnforcerTestPrograms.NOT_NULL_ERROR_NESTED_ROWS,
                ConstraintEnforcerTestPrograms.LENGTH_TRIM_PAD_NESTED_ROWS,
                ConstraintEnforcerTestPrograms.LENGTH_ERROR_NESTED_ROWS,
                ConstraintEnforcerTestPrograms.NOT_NULL_DROP_NESTED_ARRAYS,
                ConstraintEnforcerTestPrograms.NOT_NULL_ERROR_NESTED_ARRAYS,
                ConstraintEnforcerTestPrograms.NOT_NULL_DROP_NESTED_MAPS,
                ConstraintEnforcerTestPrograms.NOT_NULL_ERROR_NESTED_MAPS,
                ConstraintEnforcerTestPrograms.LENGTH_TRIM_PAD_WITH_NESTED_COLLECTIONS,
                ConstraintEnforcerTestPrograms.LENGTH_ERROR_WITH_NESTED_ARRAYS,
                ConstraintEnforcerTestPrograms.LENGTH_ERROR_WITH_NESTED_MAPS);
    }
}
