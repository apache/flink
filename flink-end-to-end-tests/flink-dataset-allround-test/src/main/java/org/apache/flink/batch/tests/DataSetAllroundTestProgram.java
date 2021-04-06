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

package org.apache.flink.batch.tests;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

/**
 * Program to test a large chunk of DataSet API operators and primitives:
 *
 * <ul>
 *   <li>Map, FlatMap, Filter
 *   <li>GroupReduce, Reduce
 *   <li>Join
 *   <li>CoGroup
 *   <li>BulkIteration
 *   <li>Different key definitions (position, name, KeySelector)
 * </ul>
 *
 * <p>Program parameters:
 *
 * <ul>
 *   <li>loadFactor (int): controls generated data volume. Does not affect result.
 *   <li>outputPath (String): path to write the result
 *   <li>infinite (Boolean): if set to true one of the sources will be infinite. The job will never
 *       end. (default: false(
 * </ul>
 */
public class DataSetAllroundTestProgram {

    @SuppressWarnings("Convert2Lambda")
    public static void main(String[] args) throws Exception {

        // get parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        int loadFactor = Integer.parseInt(params.getRequired("loadFactor"));
        String outputPath = params.getRequired("outputPath");
        boolean infinite = params.getBoolean("infinite", false);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        int numKeys = loadFactor * 128 * 1024;
        DataSet<Tuple2<String, Integer>> x1Keys;
        DataSet<Tuple2<String, Integer>> x2Keys =
                env.createInput(Generator.generate(numKeys * 32, 2)).setParallelism(4);
        DataSet<Tuple2<String, Integer>> x8Keys =
                env.createInput(Generator.generate(numKeys, 8)).setParallelism(4);

        if (infinite) {
            x1Keys = env.createInput(Generator.generateInfinitely(numKeys)).setParallelism(4);
        } else {
            x1Keys = env.createInput(Generator.generate(numKeys, 1)).setParallelism(4);
        }

        DataSet<Tuple2<String, Integer>> joined =
                x2Keys
                        // shift keys (check for correct handling of key positions)
                        .map(x -> Tuple4.of("0-0", 0L, 1, x.f0))
                        .returns(Types.TUPLE(Types.STRING, Types.LONG, Types.INT, Types.STRING))
                        // join datasets on non-unique fields (m-n join)
                        // Result: (key, 1) 16 * #keys records, all keys are preserved
                        .join(x8Keys)
                        .where(3)
                        .equalTo(0)
                        .with((l, r) -> Tuple2.of(l.f3, 1))
                        .returns(Types.TUPLE(Types.STRING, Types.INT))
                        // key definition with key selector function
                        .groupBy(
                                new KeySelector<Tuple2<String, Integer>, String>() {
                                    @Override
                                    public String getKey(Tuple2<String, Integer> value) {
                                        return value.f0;
                                    }
                                })
                        // reduce
                        // Result: (key, cnt), #keys records with unique keys, cnt = 16
                        .reduce((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1));

        // co-group two datasets on their primary keys.
        // we filter both inputs such that only 6.25% of the keys overlap.
        // result: (key, cnt), #keys records with unique keys, cnt = (6.25%: 2, 93.75%: 1)
        DataSet<Tuple2<String, Integer>> coGrouped =
                x1Keys.filter(x -> x.f1 > 59)
                        .coGroup(x1Keys.filter(x -> x.f1 < 68))
                        .where("f0")
                        .equalTo("f0")
                        .with(
                                (CoGroupFunction<
                                                Tuple2<String, Integer>,
                                                Tuple2<String, Integer>,
                                                Tuple2<String, Integer>>)
                                        (l, r, out) -> {
                                            int cnt = 0;
                                            String key = "";
                                            for (Tuple2<String, Integer> t : l) {
                                                cnt++;
                                                key = t.f0;
                                            }
                                            for (Tuple2<String, Integer> t : r) {
                                                cnt++;
                                                key = t.f0;
                                            }
                                            out.collect(Tuple2.of(key, cnt));
                                        })
                        .returns(Types.TUPLE(Types.STRING, Types.INT));

        // join datasets on keys (1-1 join) and replicate by 16 (previously computed count)
        // result: (key, cnt), 16 * #keys records, all keys preserved, cnt = (6.25%: 2, 93.75%: 1)
        DataSet<Tuple2<String, Integer>> joined2 =
                joined.join(coGrouped, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE)
                        .where(0)
                        .equalTo("f0")
                        .flatMap(
                                (FlatMapFunction<
                                                Tuple2<
                                                        Tuple2<String, Integer>,
                                                        Tuple2<String, Integer>>,
                                                Tuple2<String, Integer>>)
                                        (p, out) -> {
                                            for (int i = 0; i < p.f0.f1; i++) {
                                                out.collect(Tuple2.of(p.f0.f0, p.f1.f1));
                                            }
                                        })
                        .returns(Types.TUPLE(Types.STRING, Types.INT));

        // iteration. double the count field until all counts are at 32 or more
        // result: (key, cnt), 16 * #keys records, all keys preserved, cnt = (6.25%: 64, 93.75%: 32)
        IterativeDataSet<Tuple2<String, Integer>> initial = joined2.iterate(16);
        DataSet<Tuple2<String, Integer>> iteration =
                initial.map(x -> Tuple2.of(x.f0, x.f1 * 2))
                        .returns(Types.TUPLE(Types.STRING, Types.INT));
        DataSet<Boolean> termination =
                iteration
                        // stop iteration if all values are larger/equal 32
                        .flatMap(
                                (FlatMapFunction<Tuple2<String, Integer>, Boolean>)
                                        (x, out) -> {
                                            if (x.f1 < 32) {
                                                out.collect(false);
                                            }
                                        })
                        .returns(Types.BOOLEAN);
        DataSet<Tuple2<Integer, Integer>> result =
                initial.closeWith(iteration, termination)
                        // group on the count field and count records
                        // result: two records: (32, cnt1) and (64, cnt2) where cnt1 = x * 15/16,
                        // cnt2 = x * 1/16
                        .groupBy(1)
                        .reduceGroup(
                                (GroupReduceFunction<
                                                Tuple2<String, Integer>, Tuple2<Integer, Integer>>)
                                        (g, out) -> {
                                            int key = 0;
                                            int cnt = 0;
                                            for (Tuple2<String, Integer> r : g) {
                                                key = r.f1;
                                                cnt++;
                                            }
                                            out.collect(Tuple2.of(key, cnt));
                                        })
                        .returns(Types.TUPLE(Types.INT, Types.INT))
                        // normalize result by load factor
                        // result: two records: (32: 15360) and (64, 1024). (x = 16384)
                        .map(x -> Tuple2.of(x.f0, x.f1 / (loadFactor * 128)))
                        .returns(Types.TUPLE(Types.INT, Types.INT));

        // sort and emit result
        result.sortPartition(0, Order.ASCENDING)
                .setParallelism(1)
                .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute();
    }
}
