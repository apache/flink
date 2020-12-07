/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.functions.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.time.Duration;
import java.time.Period;
import java.time.ZoneOffset;

/**
 * A {@link CallContext} backed by Calcite abstractions.
 */
@Internal
public abstract class AbstractSqlCallContext implements CallContext {

	private final DataTypeFactory dataTypeFactory;

	private final FunctionDefinition definition;

	private final String name;

	protected AbstractSqlCallContext(
			DataTypeFactory dataTypeFactory,
			FunctionDefinition definition,
			String name) {
		this.dataTypeFactory = dataTypeFactory;
		this.definition = definition;
		this.name = name;
	}

	@Override
	public DataTypeFactory getDataTypeFactory() {
		return dataTypeFactory;
	}

	@Override
	public FunctionDefinition getFunctionDefinition() {
		return definition;
	}

	@Override
	public String getName() {
		return name;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Helper interface for abstracting accessing literals.
	 */
	protected interface LiteralValueAccessor {
		<T> T getValueAs(Class<T> clazz);
	}

	/**
	 * Bridges to {@link ValueLiteralExpression#getValueAs(Class)}.
	 */
	@SuppressWarnings("unchecked")
	protected static <T> T getLiteralValueAs(LiteralValueAccessor accessor, Class<T> clazz) {
		Preconditions.checkArgument(!clazz.isPrimitive());

		Object convertedValue = null;

		if (clazz == Duration.class) {
			final long longVal = accessor.getValueAs(Long.class);
			convertedValue = Duration.ofMillis(longVal);
		}

		else if (clazz == Period.class) {
			final long longVal = accessor.getValueAs(Long.class);
			if (longVal <= Integer.MAX_VALUE && longVal >= Integer.MIN_VALUE) {
				convertedValue = Period.ofMonths((int) longVal);
			}
		}

		else if (clazz == java.time.LocalDate.class) {
			final DateString dateString = accessor.getValueAs(DateString.class);
			convertedValue = java.time.LocalDate.parse(dateString.toString());
		}

		else if (clazz == java.time.LocalTime.class) {
			final TimeString timeString = accessor.getValueAs(TimeString.class);
			convertedValue = java.time.LocalTime.parse(timeString.toString());
		}

		else if (clazz == java.time.LocalDateTime.class) {
			final TimestampString timestampString = accessor.getValueAs(TimestampString.class);
			convertedValue = java.time.LocalDateTime.parse(timestampString.toString().replace(' ', 'T'));
		}

		else if (clazz == java.time.Instant.class) {
			// timestamp string is in UTC, convert back to an instant
			final TimestampString timestampString = accessor.getValueAs(TimestampString.class);
			convertedValue = java.time.LocalDateTime.parse(timestampString.toString().replace(' ', 'T'))
				.atOffset(ZoneOffset.UTC)
				.toInstant();
		}

		if (convertedValue != null) {
			return (T) convertedValue;
		}

		return accessor.getValueAs(clazz);
	}
}
