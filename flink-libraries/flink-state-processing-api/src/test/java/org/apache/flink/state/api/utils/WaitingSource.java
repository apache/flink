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

package org.apache.flink.state.api.utils;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A wrapper class that allows threads to block until the inner source completes. It's run method
 * does not return until explicitly canceled so external processes can perform operations such as
 * taking savepoints.
 *
 * @param <T> The output type of the inner source.
 */
@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class WaitingSource<T> extends RichSourceFunction<T>
        implements ResultTypeQueryable<T>, WaitingFunction {

    private static final Map<String, OneShotLatch> guards = new HashMap<>();

    private final SourceFunction<T> source;

    private final TypeInformation<T> returnType;

    private final String guardId;

    private volatile boolean running;

    public WaitingSource(SourceFunction<T> source, TypeInformation<T> returnType) {
        this.source = source;
        this.returnType = returnType;
        this.guardId = UUID.randomUUID().toString();

        guards.put(guardId, new OneShotLatch());
        this.running = true;
    }

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        if (source instanceof RichSourceFunction) {
            ((RichSourceFunction<T>) source).setRuntimeContext(t);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (source instanceof RichSourceFunction) {
            ((RichSourceFunction<T>) source).open(parameters);
        }
    }

    @Override
    public void close() throws Exception {
        if (source instanceof RichSourceFunction) {
            ((RichSourceFunction<T>) source).close();
        }
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        OneShotLatch latch = guards.get(guardId);
        try {
            source.run(ctx);
        } finally {
            latch.trigger();
        }

        while (running) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    @Override
    public void cancel() {
        source.cancel();
        running = false;
    }

    /** This method blocks until the inner source has completed. */
    @Override
    public void await() throws RuntimeException {
        try {
            guards.get(guardId).await();
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to initialize source");
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return returnType;
    }
}
