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

package org.apache.flink.table.types.inference.utils;

import org.apache.flink.table.catalog.DataTypeLookup;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

/**
 * {@link DataTypeLookup} mock for testing purposes.
 */
public class DataTypeLookupMock implements DataTypeLookup {

	public Optional<DataType> dataType = Optional.empty();

	public Optional<Class<?>> expectedClass = Optional.empty();

	@Override
	public Optional<DataType> lookupDataType(String name) {
		return Optional.of(TypeConversions.fromLogicalToDataType(LogicalTypeParser.parse(name)));
	}

	@Override
	public Optional<DataType> lookupDataType(UnresolvedIdentifier identifier) {
		return dataType;
	}

	@Override
	public DataType resolveRawDataType(Class<?> clazz) {
		expectedClass.ifPresent(expected -> assertEquals(expected, clazz));
		return dataType.orElseThrow(IllegalStateException::new);
	}
}
