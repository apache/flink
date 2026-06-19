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

package org.apache.flink.runtime.iterative.event;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Tests for {@link IterationEventWithAggregators}. */
public class EventWithAggregatorsTest {

    private ClassLoader cl = ClassLoader.getSystemClassLoader();

    @Test
    public void testSerializationOfEmptyEvent() {
        AllWorkersDoneEvent e = new AllWorkersDoneEvent();
        IterationEventWithAggregators deserialized = pipeThroughSerialization(e);

        Assert.assertEquals(0, deserialized.getAggregatorNames().length);
        Assert.assertEquals(0, deserialized.getAggregates(cl).length);
    }

    @Test
    public void testSerializationOfEventWithAggregateValues() {
        StringValue stringValue = new StringValue("test string");
        LongValue longValue = new LongValue(68743254);

        String stringValueName = "stringValue";
        String longValueName = "longValue";

        Aggregator<StringValue> stringAgg = new TestAggregator<StringValue>(stringValue);
        Aggregator<LongValue> longAgg = new TestAggregator<LongValue>(longValue);

        Map<String, Aggregator<?>> aggMap = new HashMap<String, Aggregator<?>>();
        aggMap.put(stringValueName, stringAgg);
        aggMap.put(longValueName, longAgg);

        Set<String> allNames = new HashSet<String>();
        allNames.add(stringValueName);
        allNames.add(longValueName);

        Set<Value> allVals = new HashSet<Value>();
        allVals.add(stringValue);
        allVals.add(longValue);

        // run the serialization
        AllWorkersDoneEvent e = new AllWorkersDoneEvent(aggMap);
        IterationEventWithAggregators deserialized = pipeThroughSerialization(e);

        // verify the result
        String[] names = deserialized.getAggregatorNames();
        Value[] aggregates = deserialized.getAggregates(cl);

        Assert.assertEquals(allNames.size(), names.length);
        Assert.assertEquals(allVals.size(), aggregates.length);

        // check that all the correct names and values are returned
        for (String s : names) {
            allNames.remove(s);
        }
        for (Value v : aggregates) {
            allVals.remove(v);
        }

        Assert.assertTrue(allNames.isEmpty());
        Assert.assertTrue(allVals.isEmpty());
    }

    private IterationEventWithAggregators pipeThroughSerialization(
            IterationEventWithAggregators event) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            event.write(new DataOutputViewStreamWrapper(baos));

            byte[] data = baos.toByteArray();
            baos.close();

            DataInputViewStreamWrapper in =
                    new DataInputViewStreamWrapper(new ByteArrayInputStream(data));
            IterationEventWithAggregators newEvent = event.getClass().newInstance();
            newEvent.read(in);
            in.close();

            return newEvent;
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assert.fail("Test threw an exception: " + e.getMessage());
            return null;
        }
    }

    private static class TestAggregator<T extends Value> implements Aggregator<T> {

        private static final long serialVersionUID = 1L;

        private final T val;

        public TestAggregator(T val) {
            this.val = val;
        }

        @Override
        public T getAggregate() {
            return val;
        }

        @Override
        public void aggregate(T element) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() {
            throw new UnsupportedOperationException();
        }
    }
}
