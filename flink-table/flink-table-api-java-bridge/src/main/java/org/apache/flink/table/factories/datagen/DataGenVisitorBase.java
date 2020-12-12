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

package org.apache.flink.table.factories.datagen;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.function.Supplier;

import static java.time.temporal.ChronoField.MILLI_OF_DAY;

/**
 * Base class for translating {@link LogicalType LogicalTypes} to {@link DataGeneratorContainer}'s.
 */
public abstract class DataGenVisitorBase extends LogicalTypeDefaultVisitor<DataGeneratorContainer> {

	protected final String name;

	protected final ReadableConfig config;

	protected DataGenVisitorBase(String name, ReadableConfig config) {
		this.name = name;
		this.config = config;
	}

	@Override
	public DataGeneratorContainer visit(DateType dateType) {
		return DataGeneratorContainer.of(TimeGenerator.of(() -> (int) LocalDate.now().toEpochDay()));
	}

	@Override
	public DataGeneratorContainer visit(TimeType timeType) {
		return DataGeneratorContainer.of(TimeGenerator.of(() -> LocalTime.now().get(MILLI_OF_DAY)));
	}

	@Override
	public DataGeneratorContainer visit(TimestampType timestampType) {
		return DataGeneratorContainer.of(TimeGenerator.of(() -> TimestampData.fromEpochMillis(System.currentTimeMillis())));
	}

	@Override
	public DataGeneratorContainer visit(ZonedTimestampType zonedTimestampType) {
		return DataGeneratorContainer.of(TimeGenerator.of(() -> TimestampData.fromEpochMillis(System.currentTimeMillis())));
	}

	@Override
	public DataGeneratorContainer visit(LocalZonedTimestampType localZonedTimestampType) {
		return DataGeneratorContainer.of(TimeGenerator.of(() -> TimestampData.fromEpochMillis(System.currentTimeMillis())));
	}

	@Override
	protected DataGeneratorContainer defaultMethod(LogicalType logicalType) {
		throw new ValidationException("Unsupported type: " + logicalType);
	}

	private interface SerializableSupplier<T> extends Supplier<T>, Serializable { }

	private abstract static class TimeGenerator<T> implements DataGenerator<T> {

		public static <T> TimeGenerator<T> of(SerializableSupplier<T> supplier) {
			return new TimeGenerator<T>() {
				@Override
				public T next() {
					return supplier.get();
				}
			};
		}

		@Override
		public void open(
			String name,
			FunctionInitializationContext context,
			RuntimeContext runtimeContext) throws Exception { }

		@Override
		public boolean hasNext() {
			return true;
		}
	}
}
