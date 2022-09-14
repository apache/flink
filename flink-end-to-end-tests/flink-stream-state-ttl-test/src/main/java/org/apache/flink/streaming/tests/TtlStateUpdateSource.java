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

package org.apache.flink.streaming.tests;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.tests.verify.TtlStateVerifier;

import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * Source of randomly generated keyed state updates.
 *
 * <p>Internal loop generates {@code sleepAfterElements} state updates for each verifier from {@link
 * TtlStateVerifier#VERIFIERS} using {@link TtlStateVerifier#generateRandomUpdate} and waits for
 * {@code sleepTime} to continue generation.
 */
class TtlStateUpdateSource extends RichParallelSourceFunction<TtlStateUpdate> {

    private static final long serialVersionUID = 1L;

    private final int maxKey;
    private final long sleepAfterElements;
    private final long sleepTime;

    /** Flag that determines if this source is running, i.e. generating events. */
    private volatile boolean running = true;

    TtlStateUpdateSource(int maxKey, long sleepAfterElements, long sleepTime) {
        this.maxKey = maxKey;
        this.sleepAfterElements = sleepAfterElements;
        this.sleepTime = sleepTime;
    }

    @Override
    public void run(SourceContext<TtlStateUpdate> ctx) throws Exception {
        Random random = new Random();
        long elementsBeforeSleep = sleepAfterElements;
        while (running) {
            for (int i = 0; i < sleepAfterElements; i++) {
                synchronized (ctx.getCheckpointLock()) {
                    Map<String, Object> updates =
                            TtlStateVerifier.VERIFIERS.stream()
                                    .collect(
                                            Collectors.toMap(
                                                    TtlStateVerifier::getId,
                                                    TtlStateVerifier::generateRandomUpdate));
                    ctx.collect(new TtlStateUpdate(random.nextInt(maxKey), updates));
                }
            }

            if (sleepTime > 0) {
                if (elementsBeforeSleep == 1) {
                    elementsBeforeSleep = sleepAfterElements;
                    long rnd = sleepTime < Integer.MAX_VALUE ? random.nextInt((int) sleepTime) : 0L;
                    Thread.sleep(rnd + sleepTime);
                } else if (elementsBeforeSleep > 1) {
                    --elementsBeforeSleep;
                }
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
