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

package org.apache.flink.optimizer.java;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.util.CompilerTestBase;

import org.junit.Test;

import static org.junit.Assert.fail;

@SuppressWarnings({"serial", "unchecked"})
public class DeltaIterationDependenciesTest extends CompilerTestBase {

    @Test
    public void testExceptionWhenNewWorksetNotDependentOnWorkset() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Tuple2<Long, Long>> input = env.fromElements(new Tuple2<Long, Long>(0L, 0L));

            DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> deltaIteration =
                    input.iterateDelta(input, 10, 0);

            DataSet<Tuple2<Long, Long>> delta =
                    deltaIteration
                            .getSolutionSet()
                            .join(deltaIteration.getWorkset())
                            .where(0)
                            .equalTo(0)
                            .projectFirst(1)
                            .projectSecond(1);

            DataSet<Tuple2<Long, Long>> nextWorkset =
                    deltaIteration
                            .getSolutionSet()
                            .join(input)
                            .where(0)
                            .equalTo(0)
                            .projectFirst(1)
                            .projectSecond(1);

            DataSet<Tuple2<Long, Long>> result = deltaIteration.closeWith(delta, nextWorkset);

            result.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

            Plan p = env.createProgramPlan();
            try {
                compileNoStats(p);
                fail(
                        "Should not be able to compile, since the next workset does not depend on the workset");
            } catch (CompilerException e) {
                // good
            } catch (Exception e) {
                fail("wrong exception type");
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
