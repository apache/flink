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

package org.apache.flink.formats.avro;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.avro.file.DataFileConstants.SNAPPY_CODEC;
import static org.apache.flink.configuration.description.TextElement.text;

/** Options for the avro format. */
@PublicEvolving
public class AvroFormatOptions {

    public static final ConfigOption<String> AVRO_OUTPUT_CODEC =
            ConfigOptions.key("codec")
                    .stringType()
                    .defaultValue(SNAPPY_CODEC)
                    .withDescription("The compression codec for avro");
    public static final ConfigOption<AvroEncoding> AVRO_ENCODING =
            ConfigOptions.key("encoding")
                    .enumType(AvroEncoding.class)
                    .defaultValue(AvroEncoding.BINARY)
                    .withDescription(
                            "The encoding to use for serialization. "
                                    + "Binary offers a more compact, space-efficient way "
                                    + "to represent objects, while JSON offers a more "
                                    + "human-readable option.");

    /** Serialization types for Avro encoding, see {@link #AVRO_ENCODING}. */
    public enum AvroEncoding implements DescribedEnum {
        BINARY("binary", text("Use binary encoding for serialization and deserialization.")),
        JSON("json", text("Use JSON encoding for serialization and deserialization."));

        private final String value;
        private final InlineElement description;

        AvroEncoding(String value, InlineElement description) {
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

    public static final ConfigOption<Boolean> AVRO_TIMESTAMP_LEGACY_MAPPING =
            ConfigOptions.key("timestamp_mapping.legacy")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Use the legacy mapping of timestamp in avro. "
                                    + "Before 1.19, The default behavior of Flink wrongly mapped "
                                    + "both SQL TIMESTAMP and TIMESTAMP_LTZ type to AVRO TIMESTAMP. "
                                    + "The correct behavior is Flink SQL TIMESTAMP maps Avro LOCAL "
                                    + "TIMESTAMP and Flink SQL TIMESTAMP_LTZ maps Avro TIMESTAMP, "
                                    + "you can obtain the correct mapping by disable using this legacy mapping."
                                    + " Use legacy behavior by default for compatibility consideration.");

    private AvroFormatOptions() {}
}
