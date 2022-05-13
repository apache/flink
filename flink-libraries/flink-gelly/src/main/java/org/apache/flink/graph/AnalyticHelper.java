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

package org.apache.flink.graph;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;

/**
 * A {@link GraphAnalytic} computes over a DataSet and returns the results via Flink accumulators.
 * This computation is cheaply performed in a terminating {@link RichOutputFormat}.
 *
 * <p>This class simplifies the creation of analytic helpers by providing pass-through methods for
 * adding and getting accumulators. Each accumulator name is prefixed with a random string since
 * Flink accumulators share a per-job global namespace. This class also provides empty
 * implementations of {@link RichOutputFormat#open} and {@link RichOutputFormat#close}.
 *
 * @param <T> element type
 */
public abstract class AnalyticHelper<T> extends RichOutputFormat<T> {

    private static final String SEPARATOR = "-";

    private String id = new AbstractID().toString();

    @Override
    public void configure(Configuration parameters) {}

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {}

    /**
     * Adds an accumulator by prepending the given name with a random string.
     *
     * @param name The name of the accumulator
     * @param accumulator The accumulator
     * @param <V> Type of values that are added to the accumulator
     * @param <A> Type of the accumulator result as it will be reported to the client
     */
    public <V, A extends Serializable> void addAccumulator(
            String name, Accumulator<V, A> accumulator) {
        getRuntimeContext().addAccumulator(id + SEPARATOR + name, accumulator);
    }

    /**
     * Gets the accumulator with the given name. Returns {@code null}, if no accumulator with that
     * name was produced.
     *
     * @param accumulatorName The name of the accumulator
     * @param <A> The generic type of the accumulator value
     * @return The value of the accumulator with the given name
     */
    public <A> A getAccumulator(ExecutionEnvironment env, String accumulatorName) {
        JobExecutionResult result = env.getLastJobExecutionResult();

        Preconditions.checkNotNull(
                result, "No result found for job, was execute() called before getting the result?");

        return result.getAccumulatorResult(id + SEPARATOR + accumulatorName);
    }
}
