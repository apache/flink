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

package org.apache.flink.api.java.summarize.aggregation;

import org.apache.flink.types.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;

import java.util.List;

/** Tests for {@link SummaryAggregatorFactory}. */
public class SummaryAggregatorFactoryTest {

    @Test
    public void testCreate() throws Exception {
        // supported primitive types
        Assertions.assertEquals(
                StringSummaryAggregator.class,
                SummaryAggregatorFactory.create(String.class).getClass());
        Assertions.assertEquals(
                ShortSummaryAggregator.class,
                SummaryAggregatorFactory.create(Short.class).getClass());
        Assertions.assertEquals(
                IntegerSummaryAggregator.class,
                SummaryAggregatorFactory.create(Integer.class).getClass());
        Assertions.assertEquals(
                LongSummaryAggregator.class,
                SummaryAggregatorFactory.create(Long.class).getClass());
        Assertions.assertEquals(
                FloatSummaryAggregator.class,
                SummaryAggregatorFactory.create(Float.class).getClass());
        Assertions.assertEquals(
                DoubleSummaryAggregator.class,
                SummaryAggregatorFactory.create(Double.class).getClass());
        Assertions.assertEquals(
                BooleanSummaryAggregator.class,
                SummaryAggregatorFactory.create(Boolean.class).getClass());

        // supported value types
        Assertions.assertEquals(
                ValueSummaryAggregator.StringValueSummaryAggregator.class,
                SummaryAggregatorFactory.create(StringValue.class).getClass());
        Assertions.assertEquals(
                ValueSummaryAggregator.ShortValueSummaryAggregator.class,
                SummaryAggregatorFactory.create(ShortValue.class).getClass());
        Assertions.assertEquals(
                ValueSummaryAggregator.IntegerValueSummaryAggregator.class,
                SummaryAggregatorFactory.create(IntValue.class).getClass());
        Assertions.assertEquals(
                ValueSummaryAggregator.LongValueSummaryAggregator.class,
                SummaryAggregatorFactory.create(LongValue.class).getClass());
        Assertions.assertEquals(
                ValueSummaryAggregator.FloatValueSummaryAggregator.class,
                SummaryAggregatorFactory.create(FloatValue.class).getClass());
        Assertions.assertEquals(
                ValueSummaryAggregator.DoubleValueSummaryAggregator.class,
                SummaryAggregatorFactory.create(DoubleValue.class).getClass());
        Assertions.assertEquals(
                ValueSummaryAggregator.BooleanValueSummaryAggregator.class,
                SummaryAggregatorFactory.create(BooleanValue.class).getClass());

        // some not well supported types - these fallback to ObjectSummaryAggregator
        Assertions.assertEquals(
                ObjectSummaryAggregator.class,
                SummaryAggregatorFactory.create(Object.class).getClass());
        Assertions.assertEquals(
                ObjectSummaryAggregator.class,
                SummaryAggregatorFactory.create(List.class).getClass());
    }
}
