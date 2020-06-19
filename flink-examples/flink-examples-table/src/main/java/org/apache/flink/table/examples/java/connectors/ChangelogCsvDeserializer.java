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

package org.apache.flink.table.examples.java.connectors;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.RuntimeConverter.Context;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.List;
import java.util.regex.Pattern;

/**
 * The {@link ChangelogCsvDeserializer} contains a simple parsing logic for converting bytes into
 * {@link Row} of {@link Integer} and {@link String} with a {@link RowKind}.
 *
 * <p>The final conversion step converts those into internal data structures.
 */
public final class ChangelogCsvDeserializer implements DeserializationSchema<RowData> {

	private final List<LogicalType> parsingTypes;
	private final DataStructureConverter converter;
	private final TypeInformation<RowData> producedTypeInfo;
	private final String columnDelimiter;

	public ChangelogCsvDeserializer(
			List<LogicalType> parsingTypes,
			DataStructureConverter converter,
			TypeInformation<RowData> producedTypeInfo,
			String columnDelimiter) {
		this.parsingTypes = parsingTypes;
		this.converter = converter;
		this.producedTypeInfo = producedTypeInfo;
		this.columnDelimiter = columnDelimiter;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		// return the type information required by Flink's core interfaces
		return producedTypeInfo;
	}

	@Override
	public void open(InitializationContext context) {
		// converters must be opened
		converter.open(Context.create(ChangelogCsvDeserializer.class.getClassLoader()));
	}

	@Override
	public RowData deserialize(byte[] message) {
		// parse the columns including a changelog flag
		final String[] columns = new String(message).split(Pattern.quote(columnDelimiter));
		final RowKind kind = RowKind.valueOf(columns[0]);
		final Row row = new Row(kind, parsingTypes.size());
		for (int i = 0; i < parsingTypes.size(); i++) {
			row.setField(i, parse(parsingTypes.get(i).getTypeRoot(), columns[i + 1]));
		}
		// convert to internal data structure
		return (RowData) converter.toInternal(row);
	}

	private static Object parse(LogicalTypeRoot root, String value) {
		switch (root) {
			case INTEGER:
				return Integer.parseInt(value);
			case VARCHAR:
				return value;
			default:
				throw new IllegalArgumentException();
		}
	}

	@Override
	public boolean isEndOfStream(RowData nextElement) {
		return false;
	}
}
