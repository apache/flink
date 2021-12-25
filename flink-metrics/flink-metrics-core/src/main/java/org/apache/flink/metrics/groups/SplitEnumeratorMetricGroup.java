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

import org.apache.flink.metrics.Gauge;

/**
 * Pre-defined metrics for {@code SplitEnumerator}.
 *
 * <p>You should only update the metrics in the main operator thread.
 */
public interface SplitEnumeratorMetricGroup extends OperatorCoordinatorMetricGroup {
    /**
     * Sets an optional gauge for the number of splits that have been enumerated but not yet
     * assigned. For example, this would be the number of files that are in the backlog.
     *
     * <p>Note that not every source can report this metric in an plausible and efficient way.
     *
     * @return the supplied gauge
     */
    <G extends Gauge<Long>> G setUnassignedSplitsGauge(G unassignedSplitsGauge);
}
