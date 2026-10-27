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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.planner.plan.nodes.exec.UuidTestPrograms;
import org.apache.flink.table.planner.plan.nodes.exec.testutils.BatchSemanticTestBase;
import org.apache.flink.table.test.program.TableTestProgram;

import java.util.List;

/**
 * Batch semantic tests for the {@link DataTypes#UUID()} type as an ordering, grouping and join key.
 */
public class UuidSemanticTest extends BatchSemanticTestBase {

    @Override
    public List<TableTestProgram> programs() {
        return List.of(
                UuidTestPrograms.UUID_EQUALITY,
                UuidTestPrograms.UUID_COMPARISON,
                UuidTestPrograms.UUID_LITERAL_FILTER,
                UuidTestPrograms.UUID_ORDER_BY,
                UuidTestPrograms.UUID_GROUP_BY,
                UuidTestPrograms.UUID_JOIN);
    }
}
