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

package org.apache.flink.streaming.util.asyncprocessing;

import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AsyncKeyOrderedProcessingOperator;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A test harness for testing a {@link AsyncKeyOrderedProcessingOperator} .
 *
 * <p>This harness has no difference with {@link AsyncOneInputStreamOperatorTestHarness} but check
 * environment state.
 */
public class AsyncOneInputStreamOperatorTestHarnessWithoutEnvCheck<IN, OUT>
        extends AsyncOneInputStreamOperatorTestHarness<IN, OUT> {

    public static <IN, OUT> AsyncOneInputStreamOperatorTestHarnessWithoutEnvCheck<IN, OUT> create(
            OneInputStreamOperator<IN, OUT> operator) throws Exception {

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        CompletableFuture<AsyncOneInputStreamOperatorTestHarnessWithoutEnvCheck<IN, OUT>> future =
                new CompletableFuture<>();
        executorService.execute(
                () -> {
                    try {
                        future.complete(
                                new AsyncOneInputStreamOperatorTestHarnessWithoutEnvCheck<>(
                                        executorService,
                                        SimpleOperatorFactory.of(operator),
                                        1,
                                        1,
                                        0));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        return future.get();
    }

    protected AsyncOneInputStreamOperatorTestHarnessWithoutEnvCheck(
            ExecutorService executor,
            StreamOperatorFactory<OUT> operatorFactory,
            int maxParallelism,
            int parallelism,
            int subtaskIndex)
            throws Exception {
        super(executor, operatorFactory, maxParallelism, parallelism, subtaskIndex);
    }

    @Override
    protected void checkEnvState() {
        // do not check
    }
}
