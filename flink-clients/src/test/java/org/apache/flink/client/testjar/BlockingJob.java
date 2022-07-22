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

package org.apache.flink.client.testjar;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.client.cli.CliFrontendTestUtils;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.util.FlinkException;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

/**
 * A testing job which blocks processing until it gets manually {@link #unblock(String) unblocked}.
 */
public class BlockingJob {

    private static final ConcurrentMap<String, CountDownLatch> RUNNING = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, CountDownLatch> BLOCKED = new ConcurrentHashMap<>();

    public static PackagedProgram getProgram(String blockId) throws FlinkException {
        try {
            return PackagedProgram.newBuilder()
                    .setUserClassPaths(
                            Collections.singletonList(
                                    new File(CliFrontendTestUtils.getTestJarPath())
                                            .toURI()
                                            .toURL()))
                    .setEntryPointClassName(BlockingJob.class.getName())
                    .setArguments(blockId)
                    .build();
        } catch (ProgramInvocationException | FileNotFoundException | MalformedURLException e) {
            throw new FlinkException("Could not load the provided entrypoint class.", e);
        }
    }

    public static void cleanUp(String blockId) {
        RUNNING.remove(blockId);
        BLOCKED.remove(blockId);
    }

    public static void awaitRunning(String blockId) throws InterruptedException {
        RUNNING.computeIfAbsent(blockId, ignored -> new CountDownLatch(1)).await();
    }

    public static void unblock(String blockId) {
        BLOCKED.computeIfAbsent(blockId, ignored -> new CountDownLatch(1)).countDown();
    }

    public static void main(String[] args) throws Exception {
        final String blockId = args[0];
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.fromCollection(Arrays.asList(1, 2, 3))
                .map(element -> element + 1)
                .map(
                        element -> {
                            RUNNING.computeIfAbsent(blockId, ignored -> new CountDownLatch(1))
                                    .countDown();
                            BLOCKED.computeIfAbsent(blockId, ignored -> new CountDownLatch(1))
                                    .await();
                            return element;
                        })
                .output(new DiscardingOutputFormat<>());
        env.execute();
    }
}
