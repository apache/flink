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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.util.NonReusingMutableToRegularIteratorWrapper;
import org.apache.flink.runtime.util.ReusingMutableToRegularIteratorWrapper;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GroupReduceDriver task which is executed by a Task Manager. The task has a single input and one
 * or multiple outputs. It is provided with a GroupReduceFunction implementation or a
 * RichGroupReduceFunction. This Driver performs multiple tasks depending on the DriverStrategy. In
 * case of a ALL_GROUP_REDUCE_COMBINE it uses the combine function of the supplied user function. In
 * case of the ALL_GROUP_REDUCE, it uses the reduce function of the supplied user function to
 * process all elements. In either case, the function is executed on all elements.
 *
 * <p>The AllGroupReduceDriver creates an iterator over all records from its input. The iterator is
 * handed to the <code>reduce()</code> method of the GroupReduceFunction.
 *
 * @see org.apache.flink.api.common.functions.GroupReduceFunction
 */
public class AllGroupReduceDriver<IT, OT> implements Driver<GroupReduceFunction<IT, OT>, OT> {

    private static final Logger LOG = LoggerFactory.getLogger(AllGroupReduceDriver.class);

    private TaskContext<GroupReduceFunction<IT, OT>, OT> taskContext;

    private MutableObjectIterator<IT> input;

    private TypeSerializer<IT> serializer;

    private DriverStrategy strategy;

    private boolean objectReuseEnabled = false;

    // ------------------------------------------------------------------------

    @Override
    public void setup(TaskContext<GroupReduceFunction<IT, OT>, OT> context) {
        this.taskContext = context;
    }

    @Override
    public int getNumberOfInputs() {
        return 1;
    }

    @Override
    public Class<GroupReduceFunction<IT, OT>> getStubType() {
        @SuppressWarnings("unchecked")
        final Class<GroupReduceFunction<IT, OT>> clazz =
                (Class<GroupReduceFunction<IT, OT>>) (Class<?>) GroupReduceFunction.class;
        return clazz;
    }

    @Override
    public int getNumberOfDriverComparators() {
        return 0;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public void prepare() throws Exception {
        final TaskConfig config = this.taskContext.getTaskConfig();
        this.strategy = config.getDriverStrategy();

        switch (this.strategy) {
            case ALL_GROUP_REDUCE_COMBINE:
                if (!(this.taskContext.getStub() instanceof GroupCombineFunction)) {
                    throw new Exception(
                            "Using combiner on a UDF that does not implement the combiner interface "
                                    + GroupCombineFunction.class.getName());
                }
            case ALL_GROUP_REDUCE:
            case ALL_GROUP_COMBINE:
                break;
            default:
                throw new Exception(
                        "Unrecognized driver strategy for AllGroupReduce driver: "
                                + this.strategy.name());
        }

        this.serializer = this.taskContext.<IT>getInputSerializer(0).getSerializer();
        this.input = this.taskContext.getInput(0);

        ExecutionConfig executionConfig = taskContext.getExecutionConfig();
        this.objectReuseEnabled = executionConfig.isObjectReuseEnabled();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "AllGroupReduceDriver object reuse: "
                            + (this.objectReuseEnabled ? "ENABLED" : "DISABLED")
                            + ".");
        }
    }

    @Override
    public void run() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    this.taskContext.formatLogString(
                            "AllGroupReduceDriver preprocessing done. Running Reducer code."));
        }

        if (objectReuseEnabled) {
            final ReusingMutableToRegularIteratorWrapper<IT> inIter =
                    new ReusingMutableToRegularIteratorWrapper<IT>(this.input, this.serializer);

            // single UDF call with the single group
            if (inIter.hasNext()) {
                if (strategy == DriverStrategy.ALL_GROUP_REDUCE) {
                    final GroupReduceFunction<IT, OT> reducer = this.taskContext.getStub();
                    final Collector<OT> output = this.taskContext.getOutputCollector();
                    reducer.reduce(inIter, output);
                } else if (strategy == DriverStrategy.ALL_GROUP_REDUCE_COMBINE
                        || strategy == DriverStrategy.ALL_GROUP_COMBINE) {
                    @SuppressWarnings("unchecked")
                    final GroupCombineFunction<IT, OT> combiner =
                            (GroupCombineFunction<IT, OT>) this.taskContext.getStub();
                    final Collector<OT> output = this.taskContext.getOutputCollector();
                    combiner.combine(inIter, output);
                } else {
                    throw new Exception("The strategy " + strategy + " is unknown to this driver.");
                }
            }

        } else {
            final NonReusingMutableToRegularIteratorWrapper<IT> inIter =
                    new NonReusingMutableToRegularIteratorWrapper<IT>(this.input, this.serializer);

            // single UDF call with the single group
            if (inIter.hasNext()) {
                if (strategy == DriverStrategy.ALL_GROUP_REDUCE) {
                    final GroupReduceFunction<IT, OT> reducer = this.taskContext.getStub();
                    final Collector<OT> output = this.taskContext.getOutputCollector();
                    reducer.reduce(inIter, output);
                } else if (strategy == DriverStrategy.ALL_GROUP_REDUCE_COMBINE
                        || strategy == DriverStrategy.ALL_GROUP_COMBINE) {
                    @SuppressWarnings("unchecked")
                    final GroupCombineFunction<IT, OT> combiner =
                            (GroupCombineFunction<IT, OT>) this.taskContext.getStub();
                    final Collector<OT> output = this.taskContext.getOutputCollector();
                    combiner.combine(inIter, output);
                } else {
                    throw new Exception("The strategy " + strategy + " is unknown to this driver.");
                }
            }
        }
    }

    @Override
    public void cleanup() {}

    @Override
    public void cancel() {}
}
