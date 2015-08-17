/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.flink.kafka_backport.common.metrics.stats;

import org.apache.flink.kafka_backport.common.metrics.MetricConfig;

import java.util.List;

// ----------------------------------------------------------------------------
//  This class is copied from the Apache Kafka project.
// 
//  The class is part of a "backport" of the new consumer API, in order to
//  give Flink access to its functionality until the API is properly released.
// 
//  This is a temporary workaround!
// ----------------------------------------------------------------------------

/**
 * A {@link SampledStat} that gives the min over its samples.
 */
public class Min extends SampledStat {

    public Min() {
        super(Double.MIN_VALUE);
    }

    @Override
    protected void update(Sample sample, MetricConfig config, double value, long now) {
        sample.value = Math.min(sample.value, value);
    }

    @Override
    public double combine(List<Sample> samples, MetricConfig config, long now) {
        double max = Double.MAX_VALUE;
        for (int i = 0; i < samples.size(); i++)
            max = Math.min(max, samples.get(i).value);
        return max;
    }

}
