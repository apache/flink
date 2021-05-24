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

package org.apache.flink.streaming.util;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** A test operator class for any mode reading. */
public class TestAnyModeReadingStreamOperator extends AbstractStreamOperator<String>
        implements TwoInputStreamOperator<String, Integer, String>, InputSelectable {

    private final String name;

    public TestAnyModeReadingStreamOperator(String name) {
        super();

        this.name = name;
    }

    @Override
    public InputSelection nextSelection() {
        return InputSelection.ALL;
    }

    @Override
    public void processElement1(StreamRecord<String> element) {
        output.collect(element.replace("[" + name + "-1]: " + element.getValue()));
    }

    @Override
    public void processElement2(StreamRecord<Integer> element) {
        output.collect(element.replace("[" + name + "-2]: " + element.getValue()));
    }
}
