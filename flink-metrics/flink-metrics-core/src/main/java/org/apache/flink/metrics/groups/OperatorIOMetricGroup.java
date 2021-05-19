/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.groups;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

/**
 * Metric group that contains shareable pre-defined IO-related metrics for operators.
 *
 * <p>You should only update the metrics in the main operator thread.
 */
public interface OperatorIOMetricGroup extends MetricGroup {
    /**
     * The total number of input records since the operator started. Will also populate
     * numRecordsInPerSecond meter.
     */
    Counter getNumRecordsInCounter();

    /**
     * The total number of output records since the operator started. Will also populate
     * numRecordsOutPerSecond meter.
     */
    Counter getNumRecordsOutCounter();

    /**
     * The total number of input bytes since the task started. Will also populate
     * numBytesInPerSecond meter.
     */
    Counter getNumBytesInCounter();

    /**
     * The total number of output bytes since the task started. Will also populate
     * numBytesOutPerSecond meter.
     */
    Counter getNumBytesOutCounter();
}
