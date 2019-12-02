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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.SqlTimestampComparator;
import org.apache.flink.api.common.typeutils.base.SqlTimestampSerializer;

import java.sql.Timestamp;
import java.util.Objects;

/**
 * {@link TypeInformation} for {@link Timestamp}.
 *
 * <p>The difference between Types.SQL_TIMESTAMP is this TypeInformation holds a precision
 * Reminder: Conversion from DateType to TypeInformation (and back) exists in
 * TableSourceUtil.computeIndexMapping, which should be fixed after we remove Legacy TypeInformation
 * TODO: https://issues.apache.org/jira/browse/FLINK-14927
 */
public class LegacyTimestampTypeInfo extends SqlTimeTypeInfo<Timestamp> {

	private static final long serialVersionUID = 1L;

	private final int precision;

	@SuppressWarnings("unchecked")
	public LegacyTimestampTypeInfo(int precision) {
		super(
			Timestamp.class,
			SqlTimestampSerializer.INSTANCE,
			(Class) SqlTimestampComparator.class);
		this.precision = precision;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof LegacyTimestampTypeInfo)) {
			return false;
		}
		LegacyTimestampTypeInfo that = (LegacyTimestampTypeInfo) obj;
		return this.precision == that.precision;
	}

	@Override
	public String toString() {
		return String.format("Timestamp(%d)", precision);
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.getClass().getCanonicalName(), precision);
	}

	public int getPrecision() {
		return precision;
	}
}
