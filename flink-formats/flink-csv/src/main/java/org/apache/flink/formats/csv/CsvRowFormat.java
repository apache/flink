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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MappingIterator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A reader format for the {@link org.apache.flink.connector.file.src.FileSource} that reads
 * CSV files into {@link Row Rows}.
 *
 * <p>Internally, this format uses the Jackson Library's CSV decoder.
 */
public class CsvRowFormat extends SimpleStreamFormat<Row> {

	private static final long serialVersionUID = 1L;

	private final CsvSchema csvSchema;

	private final String[] selectedFieldNames;

	private final RowTypeInfo rowType;

	private final boolean ignoreParseErrors;

	/**
	 * Creates a new {@code CsvRowFormat}. This constructor cannot be called directly, construction
	 * should go through the builder, for example via the {@link #builder(RowTypeInfo)} method.
	 */
	private CsvRowFormat(
			final CsvSchema csvSchema,
			final String[] selectedFieldNames,
			final RowTypeInfo rowType,
			boolean ignoreParseErrors) {

		this.csvSchema = checkNotNull(csvSchema);
		this.selectedFieldNames = checkNotNull(selectedFieldNames);
		this.rowType = checkNotNull(rowType);
		this.ignoreParseErrors = ignoreParseErrors;
		checkArgument(selectedFieldNames.length == rowType.getArity());
	}

	@Override
	public Reader createReader(Configuration config, FSDataInputStream inputStream) throws IOException {
		final CsvRowDeserializationSchema.RuntimeConverter[] fieldConvertes =
				CsvRowDeserializationSchema.createFieldRuntimeConverters(ignoreParseErrors, rowType.getFieldTypes());

		final MappingIterator<JsonNode> iterator = new CsvMapper()
			.readerFor(JsonNode.class)
			.with(csvSchema)
			.readValues(inputStream);

		return new Reader(iterator, selectedFieldNames, fieldConvertes, ignoreParseErrors);
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return rowType;
	}

	// ------------------------------------------------------------------------
	//  Builder / Factory
	// ------------------------------------------------------------------------

	/**
	 * Starts building a {@link CsvRowFormat} based on a row type information.
	 */
	public static Builder builder(RowTypeInfo rowType) {
		return new Builder(rowType);
	}

	/**
	 * A builder for creating a {@link RowCsvInputFormat}.
	 */
	public static final class Builder {

		private final RowTypeInfo rowType;
		private int[] selectedFields;
		private CsvSchema csvSchema;
		private boolean ignoreParseErrors;

		/**
		 * Creates a row CSV input format for the given {@link TypeInformation} and file paths
		 * with optional parameters.
		 */
		private Builder(RowTypeInfo rowTypeInfo) {
			this.rowType = checkNotNull(rowTypeInfo, "Type information must not be null.");
			this.csvSchema = CsvRowSchemaConverter.convert(rowTypeInfo);
		}

		public Builder setFieldDelimiter(char delimiter) {
			this.csvSchema = this.csvSchema.rebuild().setColumnSeparator(delimiter).build();
			return this;
		}

		public Builder setAllowComments(boolean allowComments) {
			this.csvSchema = this.csvSchema.rebuild().setAllowComments(allowComments).build();
			return this;
		}

		public Builder setArrayElementDelimiter(String delimiter) {
			checkNotNull(delimiter, "Array element delimiter must not be null.");
			this.csvSchema = this.csvSchema.rebuild().setArrayElementSeparator(delimiter).build();
			return this;
		}

		public Builder setQuoteCharacter(char c) {
			this.csvSchema = this.csvSchema.rebuild().setQuoteChar(c).build();
			return this;
		}

		public Builder setEscapeCharacter(char c) {
			this.csvSchema = this.csvSchema.rebuild().setEscapeChar(c).build();
			return this;
		}

		public Builder setNullLiteral(String nullLiteral) {
			checkNotNull(nullLiteral, "Null literal must not be null.");
			this.csvSchema = this.csvSchema.rebuild().setNullValue(nullLiteral).build();
			return this;
		}

		public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
			this.ignoreParseErrors = ignoreParseErrors;
			return this;
		}

		public Builder setSelectedFields(int... selectedFields) {
			this.selectedFields = selectedFields;
			return this;
		}

		public CsvRowFormat build() {
			final TypeInformation<?>[] fieldTypes = rowType.getFieldTypes();

			if (selectedFields == null) {
				selectedFields = new int[fieldTypes.length];
				for (int i = 0; i < fieldTypes.length; i++) {
					selectedFields[i] = i;
				}
			}

			final String[] selectedFieldNames = Arrays.stream(selectedFields)
				.mapToObj(csvSchema::columnName)
				.toArray(String[]::new);

			return new CsvRowFormat(
				csvSchema,
				selectedFieldNames,
				rowType,
				ignoreParseErrors);
		}
	}

	// ------------------------------------------------------------------------
	//   Reader Implementation
	// ------------------------------------------------------------------------

	/**
	 * The reader implementation that reads from the stream and decodes the CSV rows.
	 */
	public static class Reader implements StreamFormat.Reader<Row> {

		private final MappingIterator<JsonNode> iterator;

		private final String[] selectedFieldNames;

		private final CsvRowDeserializationSchema.RuntimeConverter[] fieldConverters;

		private final boolean ignoreParseErrors;

		protected Reader(
				final MappingIterator<JsonNode> iterator,
				final String[] selectedFieldNames,
				final CsvRowDeserializationSchema.RuntimeConverter[] fieldConverters,
				final boolean ignoreParseErrors) {

			this.iterator = checkNotNull(iterator);
			this.selectedFieldNames = selectedFieldNames;
			this.fieldConverters = checkNotNull(fieldConverters);
			this.ignoreParseErrors = ignoreParseErrors;
		}

		@Nullable
		@Override
		public Row read() throws IOException {
			// we need to loop because we possibly skip records (ignored parse errors)
			while (true) {

				// the first step handles exceptions only for Jackson, because there
				// we need to differentiate between forwarded IO Errors and parse exceptions
				final JsonNode node;
				try {
					node = iterator.nextValue();
				}
				catch (NoSuchElementException e) {
					return null;
				}
				catch (Exception e) {
					if (isParseException(e) && ignoreParseErrors) {
						continue;
					} else {
						throw e instanceof IOException
								? (IOException) e
								: new IOException("Failed to deserialize CSV row.", e);
					}
				}

				if (!validateArity(node)) {
					continue;
				}

				try {
					return decodeToRow(node);
				}
				catch (Exception e) {
					if (!(isParseException(e) && ignoreParseErrors)) {
						throw e instanceof IOException
							? (IOException) e
							: new IOException("Failed to deserialize CSV row.", e);
					}
				}
			}
		}

		@Override
		public void close() throws IOException {
			iterator.close();
		}

		private Row decodeToRow(JsonNode node) {
			final int nodeSize = node.size();
			final Row row = new Row(selectedFieldNames.length);

			for (int i = 0; i < Math.min(selectedFieldNames.length, nodeSize); i++) {
				// Jackson only supports mapping by name in the first level
				row.setField(i, fieldConverters[i].convert(node.get(selectedFieldNames[i])));
			}
			return row;
		}

		private boolean validateArity(JsonNode node) throws IOException {
			if (selectedFieldNames.length == node.size()) {
				return true;
			}
			else if (ignoreParseErrors) {
				return false;
			} else {
				throw new IOException(String.format("Row length mismatch: %d fields expected but was %d.",
						selectedFieldNames.length, node.size()));
			}
		}

		private static boolean isParseException(Exception e) {
			// TODO: this needs proper logic
			return true;
		}
	}
}
