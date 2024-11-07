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

package org.apache.flink.test.recovery;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/** Test the recovery of a simple batch program in the case of TaskManager process failure. */
@ExtendWith(NoOpTestExtension.class)
public class TaskManagerProcessFailureBatchRecoveryITCase
        extends AbstractTaskManagerProcessFailureRecoveryTest {
    // --------------------------------------------------------------------------------------------
    //  Test the program
    // --------------------------------------------------------------------------------------------

    @Override
    public void testTaskManagerFailure(Configuration configuration, final File coordinateDir)
            throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createRemoteEnvironment(
                        "localhost", 1337, configuration);
        env.setParallelism(PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        final long numElements = 100000L;
        final DataStream<Long> result =
                env.fromSequence(1, numElements)
                        // make sure every mapper is involved (no one is skipped because of lazy
                        // split assignment)
                        .rebalance()
                        // the majority of the behavior is in the MapFunction
                        .map(
                                new RichMapFunction<Long, Long>() {

                                    private final File proceedFile =
                                            new File(coordinateDir, PROCEED_MARKER_FILE);

                                    private boolean markerCreated = false;
                                    private boolean checkForProceedFile = true;

                                    @Override
                                    public Long map(Long value) throws Exception {
                                        if (!markerCreated) {
                                            int taskIndex =
                                                    getRuntimeContext()
                                                            .getTaskInfo()
                                                            .getIndexOfThisSubtask();
                                            touchFile(
                                                    new File(
                                                            coordinateDir,
                                                            READY_MARKER_FILE_PREFIX + taskIndex));
                                            markerCreated = true;
                                        }

                                        // check if the proceed file exists
                                        if (checkForProceedFile) {
                                            if (proceedFile.exists()) {
                                                checkForProceedFile = false;
                                            } else {
                                                // otherwise wait so that we make slow progress
                                                Thread.sleep(100);
                                            }
                                        }
                                        return value;
                                    }
                                })
                        .setParallelism(PARALLELISM)
                        .windowAll(GlobalWindows.createWithEndOfStreamTrigger())
                        .reduce(
                                new ReduceFunction<Long>() {
                                    @Override
                                    public Long reduce(Long value1, Long value2) {
                                        return value1 + value2;
                                    }
                                });

        long sum =
                CollectionUtil.iteratorToList(result.executeAndCollect()).stream()
                        .mapToLong(x -> x)
                        .sum();
        assertThat(numElements * (numElements + 1L) / 2L).isEqualTo(sum);
    }
}
