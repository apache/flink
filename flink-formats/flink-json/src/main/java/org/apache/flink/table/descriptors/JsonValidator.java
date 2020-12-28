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

/** Validator for {@link Json}. */
@Internal
public class JsonValidator extends FormatDescriptorValidator {

    public static final String FORMAT_TYPE_VALUE = "json";
    public static final String FORMAT_SCHEMA = "format.schema";
    public static final String FORMAT_JSON_SCHEMA = "format.json-schema";
    public static final String FORMAT_FAIL_ON_MISSING_FIELD = "format.fail-on-missing-field";
    public static final String FORMAT_IGNORE_PARSE_ERRORS = "format.ignore-parse-errors";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        properties.validateBoolean(FORMAT_DERIVE_SCHEMA, true);
        final boolean deriveSchema =
                properties.getOptionalBoolean(FORMAT_DERIVE_SCHEMA).orElse(true);
        final boolean hasSchema = properties.containsKey(FORMAT_SCHEMA);
        final boolean hasSchemaString = properties.containsKey(FORMAT_JSON_SCHEMA);
        // if a schema is defined, no matter derive schema is set or not, will use the defined
        // schema
        if (!deriveSchema && hasSchema && hasSchemaString) {
            throw new ValidationException(
                    "A definition of both a schema and JSON schema is not allowed.");
        } else if (!deriveSchema && !hasSchema && !hasSchemaString) {
            throw new ValidationException(
                    "A definition of a schema or JSON schema is required "
                            + "if derivation from table's schema is disabled.");
        } else if (hasSchema) {
            properties.validateType(FORMAT_SCHEMA, false, true);
        } else if (hasSchemaString) {
            properties.validateString(FORMAT_JSON_SCHEMA, false, 1);
        }

        properties.validateBoolean(FORMAT_FAIL_ON_MISSING_FIELD, true);
        properties.validateBoolean(FORMAT_IGNORE_PARSE_ERRORS, true);
        boolean failOnMissingField =
                properties.getOptionalBoolean(FORMAT_FAIL_ON_MISSING_FIELD).orElse(false);
        boolean ignoreParseErrors =
                properties.getOptionalBoolean(FORMAT_IGNORE_PARSE_ERRORS).orElse(false);
        if (ignoreParseErrors && failOnMissingField) {
            throw new ValidationException(
                    FORMAT_FAIL_ON_MISSING_FIELD
                            + " and "
                            + FORMAT_IGNORE_PARSE_ERRORS
                            + " shouldn't both be true.");
        }
    }
}
