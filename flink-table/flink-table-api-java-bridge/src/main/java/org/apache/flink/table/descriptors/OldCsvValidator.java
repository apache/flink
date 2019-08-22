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

/**
 * Validator for {@link OldCsv}.
 *
 * @deprecated Use the RFC-compliant {@code Csv} format in the dedicated
 *             flink-formats/flink-csv module instead.
 */
@Deprecated
@Internal
public class OldCsvValidator extends FormatDescriptorValidator {

	public static final String FORMAT_TYPE_VALUE = "csv";
	public static final String FORMAT_FIELD_DELIMITER = "format.field-delimiter";
	public static final String FORMAT_LINE_DELIMITER = "format.line-delimiter";
	public static final String FORMAT_QUOTE_CHARACTER = "format.quote-character";
	public static final String FORMAT_COMMENT_PREFIX = "format.comment-prefix";
	public static final String FORMAT_IGNORE_FIRST_LINE = "format.ignore-first-line";
	public static final String FORMAT_IGNORE_PARSE_ERRORS = "format.ignore-parse-errors";
	public static final String FORMAT_FIELDS = "format.fields";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(FORMAT_TYPE, FORMAT_TYPE_VALUE, false);
		properties.validateString(FORMAT_FIELD_DELIMITER, true, 1);
		properties.validateString(FORMAT_LINE_DELIMITER, true, 1);
		properties.validateString(FORMAT_QUOTE_CHARACTER, true, 1, 1);
		properties.validateString(FORMAT_COMMENT_PREFIX, true, 1);
		properties.validateBoolean(FORMAT_IGNORE_FIRST_LINE, true);
		properties.validateBoolean(FORMAT_IGNORE_PARSE_ERRORS, true);
		properties.validateTableSchema(FORMAT_FIELDS, false);
	}
}
