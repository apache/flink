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
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.utils.EncodingUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides a strategy to generate watermarks for a rowtime attribute.
 *
 * <p>A watermark strategy is either a {@code PeriodicWatermarkAssigner} or
 * {@code PunctuatedWatermarkAssigner}.
 */
@PublicEvolving
public abstract class WatermarkStrategy implements Serializable, Descriptor {

	/**
	 * This method is a default implementation that uses java serialization and it is discouraged.
	 * All implementation should provide a more specific set of properties.
	 */
	@Override
	public Map<String, String> toProperties() {
		Map<String, String> properties = new HashMap<>();
		properties.put(Rowtime.ROWTIME_WATERMARKS_TYPE, Rowtime.ROWTIME_WATERMARKS_TYPE_VALUE_CUSTOM);
		properties.put(Rowtime.ROWTIME_WATERMARKS_CLASS, this.getClass().getName());
		properties.put(Rowtime.ROWTIME_WATERMARKS_SERIALIZED, EncodingUtils.encodeObjectToString(this));
		return properties;
	}
}
