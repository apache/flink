/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayDeque;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;

/** Tests {@link BatchGroupedReduceOperator}. */
public class BatchGroupedReduceOperatorTest extends TestLogger {

    @Rule public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void noIncrementalResults() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, String, String> testHarness =
                createTestHarness();

        testHarness.processElement(new StreamRecord<>("hello"));
        testHarness.processElement(new StreamRecord<>("hello"));
        testHarness.processElement(new StreamRecord<>("ciao"));
        testHarness.processElement(new StreamRecord<>("ciao"));

        assertThat(testHarness.getOutput(), empty());
    }

    @Test
    public void resultsOnMaxWatermark() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, String, String> testHarness =
                createTestHarness();

        testHarness.processElement(new StreamRecord<>("hello"));
        testHarness.processElement(new StreamRecord<>("hello"));
        testHarness.processElement(new StreamRecord<>("ciao"));
        testHarness.processElement(new StreamRecord<>("ciao"));
        testHarness.processElement(new StreamRecord<>("ciao"));

        testHarness.processWatermark(Long.MAX_VALUE);

        ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
        expectedOutput.add(new StreamRecord<>("hellohello", Long.MAX_VALUE));
        expectedOutput.add(new StreamRecord<>("ciaociaociao", Long.MAX_VALUE));
        expectedOutput.add(new Watermark(Long.MAX_VALUE));

        assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));
    }

    @Test
    public void resultForSingleInput() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, String, String> testHarness =
                createTestHarness();

        testHarness.processElement(new StreamRecord<>("hello"));
        testHarness.processElement(new StreamRecord<>("ciao"));

        testHarness.processWatermark(Long.MAX_VALUE);

        ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
        expectedOutput.add(new StreamRecord<>("hello", Long.MAX_VALUE));
        expectedOutput.add(new StreamRecord<>("ciao", Long.MAX_VALUE));
        expectedOutput.add(new Watermark(Long.MAX_VALUE));

        assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));
    }

    private KeyedOneInputStreamOperatorTestHarness<String, String, String> createTestHarness()
            throws Exception {
        BatchGroupedReduceOperator<String, Object> operator =
                new BatchGroupedReduceOperator<>(new Concatenator(), StringSerializer.INSTANCE);

        KeyedOneInputStreamOperatorTestHarness<String, String, String> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, in -> in, BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        return testHarness;
    }

    static class Concatenator implements ReduceFunction<String> {
        @Override
        public String reduce(String value1, String value2) throws Exception {
            return value1 + value2;
        }
    }
}
