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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunctionKind;
import org.apache.flink.table.functions.python.PythonScalarFunction;
import org.apache.flink.table.planner.utils.JavaTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

/** Plans compositions of scalar UDFs with different Python argument representations. */
class PythonCalcSplitFunctionKindRuleTest extends TableTestBase {

    @Test
    void testStreamingComposition() {
        verifyComposition(javaStreamTestUtil());
    }

    @Test
    void testBatchComposition() {
        verifyComposition(javaBatchTestUtil());
    }

    private void verifyComposition(JavaTableTestUtil util) {
        util.addTableSource("T", Schema.newBuilder().column("a", DataTypes.INT()).build());
        for (PythonFunctionKind kind : PythonFunctionKind.values()) {
            util.tableEnv()
                    .createTemporarySystemFunction(
                            kind.name().toLowerCase() + "_udf",
                            new PythonScalarFunction(
                                    kind.name(),
                                    new byte[0],
                                    new DataType[] {DataTypes.INT()},
                                    DataTypes.INT(),
                                    kind,
                                    true,
                                    false,
                                    new PythonEnv(PythonEnv.ExecType.PROCESS)));
        }
        util.verifyExecPlan(
                "SELECT arrow_udf(a), pandas_udf(a), general_udf(a), "
                        + "arrow_udf(arrow_udf(a)), arrow_udf(pandas_udf(a)), "
                        + "pandas_udf(arrow_udf(a)), arrow_udf(general_udf(a)), "
                        + "general_udf(arrow_udf(a)) FROM T");
    }
}
