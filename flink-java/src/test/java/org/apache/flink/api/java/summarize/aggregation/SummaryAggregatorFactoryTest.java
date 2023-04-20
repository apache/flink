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

import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.ShortValue;
import org.apache.flink.types.StringValue;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SummaryAggregatorFactory}. */
class SummaryAggregatorFactoryTest {

    @Test
    void testCreate() {
        // supported primitive types
        assertThat(SummaryAggregatorFactory.create(String.class).getClass())
                .isEqualTo(StringSummaryAggregator.class);
        assertThat(SummaryAggregatorFactory.create(Short.class).getClass())
                .isEqualTo(ShortSummaryAggregator.class);
        assertThat(SummaryAggregatorFactory.create(Integer.class).getClass())
                .isEqualTo(IntegerSummaryAggregator.class);
        assertThat(SummaryAggregatorFactory.create(Long.class).getClass())
                .isEqualTo(LongSummaryAggregator.class);
        assertThat(SummaryAggregatorFactory.create(Float.class).getClass())
                .isEqualTo(FloatSummaryAggregator.class);
        assertThat(SummaryAggregatorFactory.create(Double.class).getClass())
                .isEqualTo(DoubleSummaryAggregator.class);
        assertThat(SummaryAggregatorFactory.create(Boolean.class).getClass())
                .isEqualTo(BooleanSummaryAggregator.class);

        // supported value types
        assertThat(SummaryAggregatorFactory.create(StringValue.class).getClass())
                .isEqualTo(ValueSummaryAggregator.StringValueSummaryAggregator.class);
        assertThat(SummaryAggregatorFactory.create(ShortValue.class).getClass())
                .isEqualTo(ValueSummaryAggregator.ShortValueSummaryAggregator.class);
        assertThat(SummaryAggregatorFactory.create(IntValue.class).getClass())
                .isEqualTo(ValueSummaryAggregator.IntegerValueSummaryAggregator.class);
        assertThat(SummaryAggregatorFactory.create(LongValue.class).getClass())
                .isEqualTo(ValueSummaryAggregator.LongValueSummaryAggregator.class);
        assertThat(SummaryAggregatorFactory.create(FloatValue.class).getClass())
                .isEqualTo(ValueSummaryAggregator.FloatValueSummaryAggregator.class);
        assertThat(SummaryAggregatorFactory.create(DoubleValue.class).getClass())
                .isEqualTo(ValueSummaryAggregator.DoubleValueSummaryAggregator.class);
        assertThat(SummaryAggregatorFactory.create(BooleanValue.class).getClass())
                .isEqualTo(ValueSummaryAggregator.BooleanValueSummaryAggregator.class);

        // some not well supported types - these fallback to ObjectSummaryAggregator
        assertThat(SummaryAggregatorFactory.create(Object.class).getClass())
                .isEqualTo(ObjectSummaryAggregator.class);
        assertThat(SummaryAggregatorFactory.create(List.class).getClass())
                .isEqualTo(ObjectSummaryAggregator.class);
    }
}
