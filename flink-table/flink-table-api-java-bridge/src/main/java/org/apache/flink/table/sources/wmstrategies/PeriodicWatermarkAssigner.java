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

package org.apache.flink.table.sources.wmstrategies;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * A periodic watermark assigner.
 */
@PublicEvolving
public abstract class PeriodicWatermarkAssigner extends WatermarkStrategy {

	/**
	 * Updates the assigner with the next timestamp.
	 *
	 * @param timestamp The next timestamp to update the assigner.
	 */
	public abstract void nextTimestamp(long timestamp);

	/**
	 * Returns the current watermark.
	 *
	 * @return The current watermark.
	 */
	public abstract Watermark getWatermark();

}
