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

package org.apache.flink.table.connector.source.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;

/**
 * Enables to push down a watermark strategy into a {@link ScanTableSource}.
 *
 * <p>The concept of watermarks defines when time operations based on an event time attribute will
 * be triggered. A watermark tells operators that no elements with a timestamp older or equal to the
 * watermark timestamp should arrive at the operator. Thus, watermarks are a trade-off between
 * latency and completeness.
 *
 * <p>Given the following SQL:
 *
 * <pre>{@code
 * CREATE TABLE t (i INT, ts TIMESTAMP(3), WATERMARK FOR ts AS ts - INTERVAL '5' SECOND)  // `ts` becomes a time attribute
 * }</pre>
 *
 * <p>In the above example, generated watermarks are lagging 5 seconds behind the highest seen
 * timestamp.
 *
 * <p>For correctness, it might be necessary to perform the watermark generation as early as
 * possible in order to be close to the actual data generation within a source's data partition.
 *
 * <p>If the {@link ScanTableSource#getScanRuntimeProvider(ScanTableSource.ScanContext)} returns
 * {@link SourceProvider}, watermarks will be automatically pushed into the runtime source operator
 * by the framework. In this case, this interface does not need to be implemented.
 *
 * <p>If the {@link ScanTableSource} does not return a {@link SourceProvider} and this interface is
 * not implemented, watermarks are generated in a subsequent operation after the source. In this
 * case, it is recommended to implement this interface to perform the watermark generation within
 * source's data partition.
 *
 * <p>This interface provides a {@link WatermarkStrategy} that needs to be applied to the runtime
 * implementation. Most built-in Flink sources provide a way of setting the watermark generator.
 *
 * @see SupportsSourceWatermark
 */
@PublicEvolving
public interface SupportsWatermarkPushDown {

    /**
     * Provides a {@link WatermarkStrategy} which defines how to generate {@link Watermark}s in the
     * stream source.
     *
     * <p>The {@link WatermarkStrategy} is a builder/factory for the actual runtime implementation
     * consisting of {@link TimestampAssigner} (assigns the event-time timestamps to each record)
     * and the {@link WatermarkGenerator} (generates the watermarks).
     *
     * <p>Note: If necessary, the watermark strategy will contain required computed column
     * expressions and consider metadata columns (if {@link SupportsReadingMetadata} is
     * implemented).
     */
    void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy);
}
