/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

/**
 * Implementation of the SinkFunction writing every tuple to the standard output or standard error
 * stream.
 *
 * <p>Four possible format options: {@code sinkIdentifier}:taskId> output <- {@code sinkIdentifier}
 * provided, parallelism > 1 {@code sinkIdentifier}> output <- {@code sinkIdentifier} provided,
 * parallelism == 1 taskId> output <- no {@code sinkIdentifier} provided, parallelism > 1 output <-
 * no {@code sinkIdentifier} provided, parallelism == 1
 *
 * @param <IN> Input record type
 */
@PublicEvolving
public class PrintSinkFunction<IN> extends RichSinkFunction<IN> {

    private static final long serialVersionUID = 1L;

    private final PrintSinkOutputWriter<IN> writer;

    /** Instantiates a print sink function that prints to standard out. */
    public PrintSinkFunction() {
        writer = new PrintSinkOutputWriter<>(false);
    }

    /**
     * Instantiates a print sink function that prints to standard out.
     *
     * @param stdErr True, if the format should print to standard error instead of standard out.
     */
    public PrintSinkFunction(final boolean stdErr) {
        writer = new PrintSinkOutputWriter<>(stdErr);
    }

    /**
     * Instantiates a print sink function that prints to standard out and gives a sink identifier.
     *
     * @param stdErr True, if the format should print to standard error instead of standard out.
     * @param sinkIdentifier Message that identify sink and is prefixed to the output of the value
     */
    public PrintSinkFunction(final String sinkIdentifier, final boolean stdErr) {
        writer = new PrintSinkOutputWriter<>(sinkIdentifier, stdErr);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
        writer.open(context.getIndexOfThisSubtask(), context.getNumberOfParallelSubtasks());
    }

    @Override
    public void invoke(IN record) {
        writer.write(record);
    }

    @Override
    public String toString() {
        return writer.toString();
    }
}
