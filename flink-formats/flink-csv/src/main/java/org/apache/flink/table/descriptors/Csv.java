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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.utils.TypeStringUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Map;

import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_ALLOW_COMMENTS;
import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_ARRAY_ELEMENT_DELIMITER;
import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_DISABLE_QUOTE_CHARACTER;
import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_ESCAPE_CHARACTER;
import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_FIELD_DELIMITER;
import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_IGNORE_PARSE_ERRORS;
import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_LINE_DELIMITER;
import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_NULL_LITERAL;
import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_QUOTE_CHARACTER;
import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_SCHEMA;
import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_TYPE_VALUE;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA;

/**
 * Format descriptor for comma-separated values (CSV).
 *
 * <p>This descriptor aims to comply with RFC-4180 ("Common Format and MIME Type for
 * Comma-Separated Values (CSV) Files") proposed by the Internet Engineering Task Force (IETF).
 *
 * <p>Note: This descriptor does not describe Flink's old non-standard CSV table
 * source/sink. Currently, this descriptor can be used when writing to Kafka. The old one is
 * still available under "org.apache.flink.table.descriptors.OldCsv" for stream/batch
 * filesystem operations.
 */
@PublicEvolving
public class Csv extends FormatDescriptor {

	private DescriptorProperties internalProperties = new DescriptorProperties(true);

	/**
	 * Format descriptor for comma-separated values (CSV).
	 *
	 * <p>This descriptor aims to comply with RFC-4180 ("Common Format and MIME Type for
	 * Comma-Separated Values (CSV) Files) proposed by the Internet Engineering Task Force (IETF).
	 */
	public Csv() {
		super(FORMAT_TYPE_VALUE, 1);
	}

	/**
	 * Sets the field delimiter character (',' by default).
	 *
	 * @param delimiter the field delimiter character
	 */
	public Csv fieldDelimiter(char delimiter) {
		internalProperties.putCharacter(FORMAT_FIELD_DELIMITER, delimiter);
		return this;
	}

	/**
	 * Sets the line delimiter ("\n" by default; otherwise "\r", "\r\n", or "" are allowed).
	 *
	 * @param delimiter the line delimiter
	 */
	public Csv lineDelimiter(String delimiter) {
		Preconditions.checkNotNull(delimiter);
		internalProperties.putString(FORMAT_LINE_DELIMITER, delimiter);
		return this;
	}

	/**
	 * Disable the quote character for enclosing field values.
	 */
	public Csv disableQuoteCharacter() {
		internalProperties.putBoolean(FORMAT_DISABLE_QUOTE_CHARACTER, true);
		return this;
	}

	/**
	 * Sets the quote character for enclosing field values ('"' by default).
	 *
	 * @param quoteCharacter the quote character
	 */
	public Csv quoteCharacter(char quoteCharacter) {
		internalProperties.putCharacter(FORMAT_QUOTE_CHARACTER, quoteCharacter);
		return this;
	}

	/**
	 * Ignores comment lines that start with '#' (disabled by default). If enabled, make sure to
	 * also ignore parse errors to allow empty rows.
	 */
	public Csv allowComments() {
		internalProperties.putBoolean(FORMAT_ALLOW_COMMENTS, true);
		return this;
	}

	/**
	 * Skip fields and rows with parse errors instead of failing. Fields are set to {@code null}
	 * in case of errors. By default, an exception is thrown.
	 */
	public Csv ignoreParseErrors() {
		internalProperties.putBoolean(FORMAT_IGNORE_PARSE_ERRORS, true);
		return this;
	}

	/**
	 * Sets the array element delimiter string for separating array or row element
	 * values (";" by default).
	 *
	 * @param delimiter the array element delimiter
	 */
	public Csv arrayElementDelimiter(String delimiter) {
		Preconditions.checkNotNull(delimiter);
		internalProperties.putString(FORMAT_ARRAY_ELEMENT_DELIMITER, delimiter);
		return this;
	}

	/**
	 * Sets the escape character for escaping values (disabled by default).
	 *
	 * @param escapeCharacter escaping character (e.g. backslash)
	 */
	public Csv escapeCharacter(char escapeCharacter) {
		internalProperties.putCharacter(FORMAT_ESCAPE_CHARACTER, escapeCharacter);
		return this;
	}

	/**
	 * Sets the null literal string that is interpreted as a null value (disabled by default).
	 *
	 * @param nullLiteral null literal (e.g. "null" or "n/a")
	 */
	public Csv nullLiteral(String nullLiteral) {
		Preconditions.checkNotNull(nullLiteral);
		internalProperties.putString(FORMAT_NULL_LITERAL, nullLiteral);
		return this;
	}

	/**
	 * Sets the format schema with field names and the types. Required if schema is not derived.
	 *
	 * @param schemaType type information that describes the schema
	 * @deprecated {@link Csv} supports derive schema from table schema by default,
	 *             it is no longer necessary to explicitly declare the format schema.
	 *             This method will be removed in the future.
	 */
	@Deprecated
	public Csv schema(TypeInformation<Row> schemaType) {
		Preconditions.checkNotNull(schemaType);
		internalProperties.putString(FORMAT_SCHEMA, TypeStringUtils.writeTypeInfo(schemaType));
		return this;
	}

	/**
	 * Derives the format schema from the table's schema. Required if no format schema is defined.
	 *
	 * <p>This allows for defining schema information only once.
	 *
	 * <p>The names, types, and fields' order of the format are determined by the table's
	 * schema. Time attributes are ignored if their origin is not a field. A "from" definition
	 * is interpreted as a field renaming in the format.
	 * @deprecated Derivation format schema from table's schema is the default behavior now.
	 * 	So there is no need to explicitly declare to derive schema.
	 */
	@Deprecated
	public Csv deriveSchema() {
		internalProperties.putBoolean(FORMAT_DERIVE_SCHEMA, true);
		return this;
	}

	@Override
	protected Map<String, String> toFormatProperties() {
		final DescriptorProperties properties = new DescriptorProperties();
		properties.putProperties(internalProperties);
		return properties.asMap();
	}
}
