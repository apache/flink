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

package org.apache.flink.formats.csv;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * RowData {@link Encoder} to serialize {@link RowData}.
 *
 * <p>This encoder is mainly used in FileSystemTableSink.
 */
@Internal
public class CsvRowDataEncoder implements Encoder<RowData> {

	private static final long serialVersionUID = 1L;
	private final CsvRowDataSerializationSchema serializationSchema;
	private transient Charset charset;

	private CsvRowDataEncoder(CsvRowDataSerializationSchema serializationSchema) {
		this.serializationSchema = checkNotNull(serializationSchema);
	}

	@Override
	public void encode(RowData element, OutputStream stream) throws IOException {
		if (charset == null) {
			charset = Charset.forName("UTF-8");
		}
		stream.write(serializationSchema.serialize(element));
	}

	public static Builder builder(TableSchema tableSchema) {
		return new Builder(tableSchema);
	}

	/**
	 * Builder to create a {@link CsvRowDataEncoder}.
	 */
	public static class Builder implements Serializable {
		private CsvRowDataSerializationSchema.Builder serializationSchemaBuilder;

		private Builder(TableSchema tableSchema) {
			checkNotNull(tableSchema);
			RowType rowType = RowType.of(
				Arrays.asList(tableSchema.getFieldDataTypes()).stream()
					.map(DataType::getLogicalType)
					.toArray(LogicalType[]::new),
				tableSchema.getFieldNames());
			this.serializationSchemaBuilder = new CsvRowDataSerializationSchema.Builder(rowType);
		}

		public Builder setLineDelimiter(String lineDelimiter) {
			checkNotNull(lineDelimiter, "Delimiter must not be null.");
			if (!lineDelimiter.equals("\n") &&
				!lineDelimiter.equals("\r") &&
				!lineDelimiter.equals("\r\n") &&
				!lineDelimiter.equals("")) {
				throw new IllegalArgumentException(
					"Unsupported new line delimiter. Only \\n, \\r, \\r\\n, or empty string are supported.");
			}
			this.serializationSchemaBuilder.setLineDelimiter(lineDelimiter);
			return this;
		}

		public Builder setFieldDelimiter(char c) {
			this.serializationSchemaBuilder.setFieldDelimiter(c);
			return this;
		}

		public Builder setArrayElementDelimiter(String delimiter) {
			checkNotNull(delimiter, "Delimiter must not be null.");
			this.serializationSchemaBuilder.setArrayElementDelimiter(delimiter);
			return this;
		}

		public Builder disableQuoteCharacter() {
			this.serializationSchemaBuilder.disableQuoteCharacter();
			return this;
		}

		public Builder setQuoteCharacter(char c) {
			this.serializationSchemaBuilder.setQuoteCharacter(c);
			return this;
		}

		public Builder setEscapeCharacter(char c) {
			this.serializationSchemaBuilder.setEscapeCharacter(c);
			return this;
		}

		public Builder setNullLiteral(String s) {
			this.serializationSchemaBuilder.setNullLiteral(s);
			return this;
		}

		public CsvRowDataEncoder build() {
			return new CsvRowDataEncoder(serializationSchemaBuilder.build());
		}
	}
}
