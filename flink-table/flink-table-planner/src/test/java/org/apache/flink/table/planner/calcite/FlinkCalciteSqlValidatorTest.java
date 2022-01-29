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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.utils.PlannerMocks;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlinkCalciteSqlValidator}. */
public class FlinkCalciteSqlValidatorTest {

    private final PlannerMocks plannerMocks =
            PlannerMocks.create()
                    .registerTemporaryTable(
                            "t1", Schema.newBuilder().column("a", DataTypes.INT()).build());

    @Test
    public void testUpsertInto() {
        assertThatThrownBy(() -> plannerMocks.getParser().parse("UPSERT INTO t1 VALUES(1)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "UPSERT INTO statement is not supported. Please use INSERT INTO instead.");
    }

    @Test
    public void testExplainUpsertInto() {
        assertThatThrownBy(() -> plannerMocks.getParser().parse("EXPLAIN UPSERT INTO t1 VALUES(1)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "UPSERT INTO statement is not supported. Please use INSERT INTO instead.");
    }
}
