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
import org.apache.flink.table.descriptors.Rowtime;

import java.util.HashMap;
import java.util.Map;

/**
 * A watermark strategy for ascending rowtime attributes.
 *
 * <p>Emits a watermark of the maximum observed timestamp so far minus 1.
 * Rows that have a timestamp equal to the max timestamp are not late.
 */
@PublicEvolving
public final class AscendingTimestamps extends PeriodicWatermarkAssigner {

	private static final long serialVersionUID = 1L;

	private long maxTimestamp = Long.MIN_VALUE + 1;

	@Override
	public void nextTimestamp(long timestamp) {
		if (timestamp > maxTimestamp) {
			maxTimestamp = timestamp;
		}
	}

	@Override
	public Map<String, String> toProperties() {
		Map<String, String> map = new HashMap<>();
		map.put(
				Rowtime.ROWTIME_WATERMARKS_TYPE,
				Rowtime.ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_ASCENDING);
		return map;
	}

	@Override
	public int hashCode() {
		return AscendingTimestamps.class.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof AscendingTimestamps;
	}

	@Override
	public Watermark getWatermark() {
		return new Watermark(maxTimestamp - 1);
	}
}
