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
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.SettableGauge;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Pre-defined metrics for {@code SourceReader}.
 *
 * <p>All metrics can only be accessed in the main operator thread.
 */
@NotThreadSafe
public interface SourceReaderMetricGroup extends OperatorMetricGroup {
    /** The total number of record that failed to consume, process, or emit. */
    Counter getNumRecordsInErrorsCounter();

    /**
     * Sets an optional gauge for the number of bytes that have not been fetched by the source. e.g.
     * the remaining bytes in a file after the file descriptor reading position.
     *
     * <p>Note that not every source can report this metric in an plausible and efficient way.
     *
     * @return the supplied gauge
     * @see SettableGauge SettableGauge to continuously update the value.
     */
    <G extends Gauge<Long>> G setPendingBytesGauge(G pendingBytesGauge);

    /**
     * Sets an optional gauge for the number of records that have not been fetched by the source.
     * e.g. the available records after the consumer offset in a Kafka partition.
     *
     * <p>Note that not every source can report this metric in an plausible and efficient way.
     *
     * @return the supplied gauge
     * @see SettableGauge SettableGauge to continuously update the value.
     */
    <G extends Gauge<Long>> G setPendingRecordsGauge(G pendingRecordsGauge);
}
