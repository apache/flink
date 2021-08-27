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

package org.apache.flink.test.iterative;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.test.util.CoordVector;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.test.util.PointFormatter;
import org.apache.flink.test.util.PointInFormat;
import org.apache.flink.util.Collector;

/** Test iteration with operator chaining. */
public class IterationWithChainingITCase extends JavaProgramTestBase {

    private static final String DATA_POINTS =
            "0|50.90|16.20|72.08|\n" + "1|73.65|61.76|62.89|\n" + "2|61.73|49.95|92.74|\n";

    private String dataPath;
    private String resultPath;

    @Override
    protected void preSubmit() throws Exception {
        dataPath = createTempFile("data_points.txt", DATA_POINTS);
        resultPath = getTempFilePath("result");
    }

    @Override
    protected void testProgram() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataSet<Tuple2<Integer, CoordVector>> initialInput =
                env.readFile(new PointInFormat(), dataPath).setParallelism(1).name("Input");

        IterativeDataSet<Tuple2<Integer, CoordVector>> iteration =
                initialInput.iterate(2).name("Loop");

        DataSet<Tuple2<Integer, CoordVector>> identity =
                iteration
                        .groupBy(0)
                        .reduceGroup(
                                new GroupReduceFunction<
                                        Tuple2<Integer, CoordVector>,
                                        Tuple2<Integer, CoordVector>>() {
                                    @Override
                                    public void reduce(
                                            Iterable<Tuple2<Integer, CoordVector>> values,
                                            Collector<Tuple2<Integer, CoordVector>> out)
                                            throws Exception {
                                        for (Tuple2<Integer, CoordVector> value : values) {
                                            out.collect(value);
                                        }
                                    }
                                })
                        .map(
                                new MapFunction<
                                        Tuple2<Integer, CoordVector>,
                                        Tuple2<Integer, CoordVector>>() {
                                    @Override
                                    public Tuple2<Integer, CoordVector> map(
                                            Tuple2<Integer, CoordVector> value) throws Exception {
                                        return value;
                                    }
                                });

        iteration.closeWith(identity).writeAsFormattedText(resultPath, new PointFormatter());

        env.execute("Iteration with chained map test");

        compareResultsByLinesInMemory(DATA_POINTS, resultPath);
    }
}
