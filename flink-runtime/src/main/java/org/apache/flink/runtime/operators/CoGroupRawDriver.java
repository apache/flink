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
package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class CoGroupRawDriver<IT1, IT2, OT> implements Driver<CoGroupFunction<IT1, IT2, OT>, OT> {

    private static final Logger LOG = LoggerFactory.getLogger(CoGroupRawDriver.class);

    private TaskContext<CoGroupFunction<IT1, IT2, OT>, OT> taskContext;

    private SimpleIterable<IT1> coGroupIterator1;
    private SimpleIterable<IT2> coGroupIterator2;

    @Override
    public void setup(TaskContext<CoGroupFunction<IT1, IT2, OT>, OT> context) {
        this.taskContext = context;
    }

    @Override
    public int getNumberOfInputs() {
        return 2;
    }

    @Override
    public int getNumberOfDriverComparators() {
        return 0;
    }

    @Override
    public Class<CoGroupFunction<IT1, IT2, OT>> getStubType() {
        @SuppressWarnings("unchecked")
        final Class<CoGroupFunction<IT1, IT2, OT>> clazz =
                (Class<CoGroupFunction<IT1, IT2, OT>>) (Class<?>) CoGroupFunction.class;
        return clazz;
    }

    @Override
    public void prepare() throws Exception {
        final TaskConfig config = this.taskContext.getTaskConfig();
        if (config.getDriverStrategy() != DriverStrategy.CO_GROUP_RAW) {
            throw new Exception(
                    "Unrecognized driver strategy for CoGoup Python driver: "
                            + config.getDriverStrategy().name());
        }

        final MutableObjectIterator<IT1> in1 = this.taskContext.getInput(0);
        final MutableObjectIterator<IT2> in2 = this.taskContext.getInput(1);

        IT1 reuse1 = this.taskContext.<IT1>getInputSerializer(0).getSerializer().createInstance();
        IT2 reuse2 = this.taskContext.<IT2>getInputSerializer(1).getSerializer().createInstance();

        this.coGroupIterator1 = new SimpleIterable<IT1>(reuse1, in1);
        this.coGroupIterator2 = new SimpleIterable<IT2>(reuse2, in2);

        if (LOG.isDebugEnabled()) {
            LOG.debug(this.taskContext.formatLogString("CoGroup task iterator ready."));
        }
    }

    @Override
    public void run() throws Exception {
        final CoGroupFunction<IT1, IT2, OT> coGroupStub = this.taskContext.getStub();
        final Collector<OT> collector = this.taskContext.getOutputCollector();
        final SimpleIterable<IT1> i1 = this.coGroupIterator1;
        final SimpleIterable<IT2> i2 = this.coGroupIterator2;

        coGroupStub.coGroup(i1, i2, collector);
    }

    @Override
    public void cleanup() throws Exception {}

    @Override
    public void cancel() throws Exception {
        cleanup();
    }

    public static class SimpleIterable<IN> implements Iterable<IN> {
        private IN reuse;
        private final MutableObjectIterator<IN> iterator;

        public SimpleIterable(IN reuse, MutableObjectIterator<IN> iterator) throws IOException {
            this.iterator = iterator;
            this.reuse = reuse;
        }

        @Override
        public Iterator<IN> iterator() {
            return new SimpleIterator<IN>(reuse, iterator);
        }

        protected class SimpleIterator<IN> implements Iterator<IN> {
            private IN reuse;
            private final MutableObjectIterator<IN> iterator;
            private boolean consumed = true;

            public SimpleIterator(IN reuse, MutableObjectIterator<IN> iterator) {
                this.iterator = iterator;
                this.reuse = reuse;
            }

            @Override
            public boolean hasNext() {
                try {
                    if (!consumed) {
                        return true;
                    }
                    IN result = iterator.next(reuse);
                    consumed = result == null;
                    return !consumed;
                } catch (IOException ioex) {
                    throw new RuntimeException(
                            "An error occurred while reading the next record: " + ioex.getMessage(),
                            ioex);
                }
            }

            @Override
            public IN next() {
                consumed = true;
                return reuse;
            }

            @Override
            public void remove() { // unused
            }
        }
    }
}
