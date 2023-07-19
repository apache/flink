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

package org.apache.flink.api.java.io;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

/**
 * Output format that prints results into either stdout or stderr.
 *
 * <p>Four possible format options: {@code sinkIdentifier}:taskId> output <- {@code sinkIdentifier}
 * provided, parallelism > 1 {@code sinkIdentifier}> output <- {@code sinkIdentifier} provided,
 * parallelism == 1 taskId> output <- no {@code sinkIdentifier} provided, parallelism > 1 output <-
 * no {@code sinkIdentifier} provided, parallelism == 1
 *
 * @param <T> Input record type
 * @deprecated All Flink DataSet APIs are deprecated since Flink 1.18 and will be removed in a
 *     future Flink major version. You can still build your application in DataSet, but you should
 *     move to either the DataStream and/or Table API.
 * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741">
 *     FLIP-131: Consolidate the user-facing Dataflow SDKs/APIs (and deprecate the DataSet API</a>
 */
@Deprecated
@PublicEvolving
public class PrintingOutputFormat<T> extends RichOutputFormat<T> {

    private static final long serialVersionUID = 1L;

    private final PrintSinkOutputWriter<T> writer;

    // --------------------------------------------------------------------------------------------

    /** Instantiates a printing output format that prints to standard out. */
    public PrintingOutputFormat() {
        writer = new PrintSinkOutputWriter<>(false);
    }

    /**
     * Instantiates a printing output format that prints to standard out.
     *
     * @param stdErr True, if the format should print to standard error instead of standard out.
     */
    public PrintingOutputFormat(final boolean stdErr) {
        writer = new PrintSinkOutputWriter<>(stdErr);
    }

    /**
     * Instantiates a printing output format that prints to standard out with a prefixed message.
     *
     * @param sinkIdentifier Message that is prefixed to the output of the value.
     * @param stdErr True, if the format should print to standard error instead of standard out.
     */
    public PrintingOutputFormat(final String sinkIdentifier, final boolean stdErr) {
        writer = new PrintSinkOutputWriter<>(sinkIdentifier, stdErr);
    }

    @Override
    public void configure(Configuration parameters) {}

    @Override
    public void open(int taskNumber, int numTasks) {
        writer.open(taskNumber, numTasks);
    }

    @Override
    public void writeRecord(T record) {
        writer.write(record);
    }

    @Override
    public void close() {}

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return writer.toString();
    }
}
