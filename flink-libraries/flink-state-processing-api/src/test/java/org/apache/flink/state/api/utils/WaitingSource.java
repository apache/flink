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
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * A wrapper class that does not return until explicitly canceled so external processes can perform
 * operations such as taking savepoints.
 *
 * @param <T> The output type of the inner source.
 */
public class WaitingSource<T> extends RichSourceFunction<T> implements ResultTypeQueryable<T> {

    private final SourceFunction<T> source;

    private final TypeInformation<T> returnType;

    private volatile boolean running;

    public WaitingSource(SourceFunction<T> source, TypeInformation<T> returnType) {
        this.source = source;
        this.returnType = returnType;

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
        source.run(ctx);

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

    @Override
    public TypeInformation<T> getProducedType() {
        return returnType;
    }
}
