/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** A bounded one-input stream operator for test. */
public class TestBoundedOneInputStreamOperator extends AbstractStreamOperator<String>
        implements OneInputStreamOperator<String, String>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final String name;

    public TestBoundedOneInputStreamOperator(String name) {
        this.name = name;
    }

    @Override
    public void processElement(StreamRecord<String> element) {
        output.collect(element);
    }

    @Override
    public void endInput() {
        output("[" + name + "]: End of input");
    }

    @Override
    public void close() throws Exception {
        ProcessingTimeService timeService = getProcessingTimeService();
        timeService.registerTimer(
                timeService.getCurrentProcessingTime(),
                t -> output("[" + name + "]: Timer registered in close"));

        output("[" + name + "]: Bye");
        super.close();
    }

    private void output(String record) {
        output.collect(new StreamRecord<>(record));
    }
}
