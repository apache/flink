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

package org.apache.flink.runtime.testtasks;

import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.types.IntValue;

/** A simple task that emits int value until the result partition is unavailable for output. */
public class OutputBlockedInvokable extends AbstractInvokable {

    public OutputBlockedInvokable(Environment environment) {
        super(environment);
    }

    @Override
    public void invoke() throws Exception {
        final IntValue value = new IntValue(1234);
        final ResultPartitionWriter resultPartitionWriter = getEnvironment().getWriter(0);
        final RecordWriter<IntValue> writer =
                new RecordWriterBuilder<IntValue>().build(resultPartitionWriter);

        while (true) {
            writer.emit(value);
        }
    }
}
