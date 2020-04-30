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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;

import java.util.Arrays;

/**
  * Validator for {@link Csv}.
  */
@Internal
public class CsvValidator extends FormatDescriptorValidator {

	public static final String FORMAT_TYPE_VALUE = "csv";
	public static final String FORMAT_FIELD_DELIMITER = "format.field-delimiter";
	public static final String FORMAT_LINE_DELIMITER = "format.line-delimiter";
	public static final String FORMAT_DISABLE_QUOTE_CHARACTER = "format.disable-quote-character";
	public static final String FORMAT_QUOTE_CHARACTER = "format.quote-character";
	public static final String FORMAT_ALLOW_COMMENTS = "format.allow-comments";
	public static final String FORMAT_IGNORE_PARSE_ERRORS = "format.ignore-parse-errors";
	public static final String FORMAT_ARRAY_ELEMENT_DELIMITER = "format.array-element-delimiter";
	public static final String FORMAT_ESCAPE_CHARACTER = "format.escape-character";
	public static final String FORMAT_NULL_LITERAL = "format.null-literal";
	public static final String FORMAT_SCHEMA = "format.schema";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateString(FORMAT_FIELD_DELIMITER, true, 1, 1);
		properties.validateEnumValues(FORMAT_LINE_DELIMITER, true, Arrays.asList("\r", "\n", "\r\n", ""));
		properties.validateBoolean(FORMAT_DISABLE_QUOTE_CHARACTER, true);
		properties.validateString(FORMAT_QUOTE_CHARACTER, true, 1, 1);
		properties.validateBoolean(FORMAT_ALLOW_COMMENTS, true);
		properties.validateBoolean(FORMAT_IGNORE_PARSE_ERRORS, true);
		properties.validateString(FORMAT_ARRAY_ELEMENT_DELIMITER, true, 1);
		properties.validateString(FORMAT_ESCAPE_CHARACTER, true, 1, 1);
		properties.validateBoolean(FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA, true);

		final boolean hasSchema = properties.containsKey(FORMAT_SCHEMA);
		final boolean isDerived = properties
			.getOptionalBoolean(FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA)
			.orElse(true);
		// if a schema is defined, no matter derive schema is set or not, will use the defined schema
		if (hasSchema) {
			properties.validateType(FORMAT_SCHEMA, false, true);
		} else if (!isDerived) {
			throw new ValidationException(
				"A definition of a schema is required if derivation from the table's schema is disabled.");
		}

		final boolean hasQuoteCharacter = properties.containsKey(FORMAT_QUOTE_CHARACTER);
		final boolean isDisabledQuoteCharacter = properties
			.getOptionalBoolean(FORMAT_DISABLE_QUOTE_CHARACTER)
			.orElse(false);
		if (isDisabledQuoteCharacter && hasQuoteCharacter){
			throw new ValidationException(
				"Format cannot define a quote character and disabled quote character at the same time.");
		}
	}
}
