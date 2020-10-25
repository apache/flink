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
import org.apache.flink.table.connector.source.ScanTableSource;

/**
 * Enables to push down watermarks into a {@link ScanTableSource}.
 *
 * <p>The concept of watermarks defines when time operations based on an event time attribute will be
 * triggered. A watermark tells operators that no elements with a timestamp older or equal to the watermark
 * timestamp should arrive at the operator. Thus, watermarks are a trade-off between latency and completeness.
 *
 * <p>Given the following SQL:
 * <pre>{@code
 *   CREATE TABLE t (i INT, ts TIMESTAMP(3), WATERMARK FOR ts AS ts - INTERVAL '5' SECOND)  // `ts` becomes a time attribute
 * }</pre>
 *
 * <p>In the above example, generated watermarks are lagging 5 seconds behind the highest seen timestamp.
 *
 * <p>By default, if this interface is not implemented, watermarks are generated in a subsequent operation
 * after the source.
 *
 * <p>However, for correctness, it might be necessary to perform the watermark generation as early as
 * possible in order to be close to the actual data generation within a source's data partition.
 *
 * <p>This interface provides a {@link WatermarkProvider} that needs to be applied to a runtime
 * implementation. Most built-in Flink sources provide a way of setting the watermark generator.
 *
 * <p>Note: In many cases, watermarks are generated from computed columns. If a source implements this
 * interface, it is recommended to also implement {@link SupportsComputedColumnPushDown}.
 */
@PublicEvolving
public interface SupportsWatermarkPushDown {

	/**
	 * Provides actual runtime implementation for generating watermarks.
	 *
	 * <p>There exist different interfaces for runtime implementation which is why {@link WatermarkProvider}
	 * serves as the base interface. Concrete {@link WatermarkProvider} interfaces might be located
	 * in other Flink modules.
	 *
	 * <p>See {@code org.apache.flink.table.connector.source.abilities} in {@code flink-table-api-java-bridge}.
	 *
	 * <p>Implementations need to perform an {@code instanceof} check and fail with an exception if the given
	 * {@link WatermarkProvider} is unsupported.
	 */
	void applyWatermark(WatermarkProvider provider);

	// --------------------------------------------------------------------------------------------

	/**
	 * Provides actual runtime implementation for generating watermarks.
	 *
	 * <p>There exist different interfaces for runtime implementation which is why {@link WatermarkProvider}
	 * serves as the base interface.
	 */
	interface WatermarkProvider {
		// marker interface that will be filled after FLIP-126:
		// WatermarkGenerator<RowData> getWatermarkGenerator();
	}
}
