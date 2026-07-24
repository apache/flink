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

package org.apache.flink.connector.datagen.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.function.Supplier;

import static java.time.temporal.ChronoField.MILLI_OF_DAY;

/**
 * Base class for translating {@link LogicalType LogicalTypes} to {@link DataGeneratorContainer}'s.
 */
@Internal
public abstract class DataGenVisitorBase extends LogicalTypeDefaultVisitor<DataGeneratorContainer> {

    protected final String name;

    protected final ReadableConfig config;

    protected DataGenVisitorBase(String name, ReadableConfig config) {
        this.name = name;
        this.config = config;
    }

    @Override
    public DataGeneratorContainer visit(DateType dateType) {
        return DataGeneratorContainer.of(timeGenerator(() -> (int) LocalDate.now().toEpochDay()));
    }

    @Override
    public DataGeneratorContainer visit(TimeType timeType) {
        return DataGeneratorContainer.of(timeGenerator(() -> LocalTime.now().get(MILLI_OF_DAY)));
    }

    @Override
    protected DataGeneratorContainer defaultMethod(LogicalType logicalType) {
        throw new ValidationException("Unsupported type: " + logicalType);
    }

    private interface SerializableSupplier<T> extends Supplier<T>, Serializable {}

    private static <T> GeneratorFunction<Long, T> timeGenerator(SerializableSupplier<T> supplier) {
        return value -> supplier.get();
    }
}
