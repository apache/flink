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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.summarize.BooleanColumnSummary;

/** Aggregator for {@link Boolean} type. */
@Internal
public class BooleanSummaryAggregator implements Aggregator<Boolean, BooleanColumnSummary> {

    private long trueCount = 0L;
    private long falseCount = 0L;
    private long nullCount = 0L;

    @Override
    public void aggregate(Boolean value) {
        if (value == null) {
            nullCount++;
        } else if (value) {
            trueCount++;
        } else {
            falseCount++;
        }
    }

    @Override
    public void combine(Aggregator<Boolean, BooleanColumnSummary> otherSameType) {
        BooleanSummaryAggregator other = (BooleanSummaryAggregator) otherSameType;
        trueCount += other.trueCount;
        falseCount += other.falseCount;
        nullCount += other.nullCount;
    }

    @Override
    public BooleanColumnSummary result() {
        return new BooleanColumnSummary(trueCount, falseCount, nullCount);
    }
}
