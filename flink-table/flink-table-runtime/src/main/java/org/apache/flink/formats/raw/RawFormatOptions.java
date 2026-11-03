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

package org.apache.flink.formats.raw;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import java.nio.charset.StandardCharsets;

import static org.apache.flink.configuration.description.TextElement.text;

/** Options for the "raw" format. */
@PublicEvolving
public class RawFormatOptions {

    public static final String BIG_ENDIAN = "big-endian";
    public static final String LITTLE_ENDIAN = "little-endian";

    public static final ConfigOption<String> ENDIANNESS =
            ConfigOptions.key("endianness")
                    .stringType()
                    .defaultValue(BIG_ENDIAN)
                    .withDescription("Defines the endianness for bytes of numeric values.");

    public static final ConfigOption<String> CHARSET =
            ConfigOptions.key("charset")
                    .stringType()
                    .defaultValue(StandardCharsets.UTF_8.displayName())
                    .withDescription("Defines the string charset.");

    public static final ConfigOption<String> LINE_DELIMITER =
            ConfigOptions.key("line-delimiter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional line delimiter. Supports Java escape sequences (e.g. '\\n', '\\r\\n'). "
                                    + "When set, deserialization splits each message by this delimiter and emits "
                                    + "one RowData per part. Serialization appends the delimiter after each row's value.");

    public static final ConfigOption<VariantEncoding> VARIANT_ENCODING =
            ConfigOptions.key("variant-encoding")
                    .enumType(VariantEncoding.class)
                    .defaultValue(VariantEncoding.JSON)
                    .withDescription(
                            "Defines how a VARIANT column is encoded. 'json' reads and writes a UTF-8 "
                                    + "JSON document, which is convenient but lossy: JSON has no type "
                                    + "for dates, times, timestamps, bytes or UUIDs. 'binary' reads and "
                                    + "writes the Variant binary encoding, the metadata followed by the "
                                    + "value, which keeps every Variant type and skips JSON parsing. "
                                    + "'charset' does not apply to 'binary'.");

    /** Encoding of a VARIANT column, see {@link #VARIANT_ENCODING}. */
    @PublicEvolving
    public enum VariantEncoding implements DescribedEnum {
        JSON("json", text("A UTF-8 JSON document. Types JSON cannot represent are lost.")),
        BINARY("binary", text("The Variant binary encoding, the metadata followed by the value."));

        private final String value;
        private final InlineElement description;

        VariantEncoding(String value, InlineElement description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    private RawFormatOptions() {}
}
