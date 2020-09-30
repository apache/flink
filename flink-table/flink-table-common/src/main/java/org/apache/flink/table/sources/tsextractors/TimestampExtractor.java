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

package org.apache.flink.table.sources.tsextractors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.sources.FieldComputer;
import org.apache.flink.table.utils.EncodingUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides an expression to extract the timestamp for a rowtime attribute.
 *
 * @deprecated This interface will not be supported in the new source design around {@link DynamicTableSource}
 *             which only works with the Blink planner. Use the concept of computed columns instead.
 *             See FLIP-95 for more information.
 */
@Deprecated
@PublicEvolving
public abstract class TimestampExtractor implements FieldComputer<Long>, Serializable, Descriptor {

	@Override
	public TypeInformation<Long> getReturnType() {
		return Types.LONG;
	}

	/**
	 * This method is a default implementation that uses java serialization and it is discouraged.
	 * All implementation should provide a more specific set of properties.
	 */
	@Override
	public Map<String, String> toProperties() {
		Map<String, String> properties = new HashMap<>();
		properties.put(Rowtime.ROWTIME_TIMESTAMPS_TYPE, Rowtime.ROWTIME_TIMESTAMPS_TYPE_VALUE_CUSTOM);
		properties.put(Rowtime.ROWTIME_TIMESTAMPS_CLASS, this.getClass().getName());
		properties.put(Rowtime.ROWTIME_TIMESTAMPS_SERIALIZED, EncodingUtils.encodeObjectToString(this));
		return properties;
	}
}
