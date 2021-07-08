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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Describes a {@link Format format} and its options for use with {@link TableDescriptor}.
 *
 * <p>Formats are responsible for encoding and decoding data in table connectors. Note that not
 * every connector has a format, while others may have multiple formats (e.g. the Kafka connector
 * has separate formats for keys and values). Common formats are "json", "csv", "avro", etc. See
 * {@link Format} for more details.
 */
@PublicEvolving
public final class FormatDescriptor {

    private final String format;
    private final Map<String, String> options;

    private FormatDescriptor(String format, Map<String, String> options) {
        this.format = format;
        this.options = Collections.unmodifiableMap(options);
    }

    /**
     * Creates a new {@link Builder} describing a format with the given format identifier.
     *
     * @param format The factory identifier for the format.
     */
    public static Builder forFormat(String format) {
        Preconditions.checkNotNull(format, "Format descriptors require a format identifier.");
        return new Builder(format);
    }

    public String getFormat() {
        return format;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    // ---------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.format("%s[%s]", format, options);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        FormatDescriptor that = (FormatDescriptor) obj;
        return format.equals(that.format) && options.equals(that.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(format, options);
    }

    // ---------------------------------------------------------------------------------------------

    /** Builder for {@link FormatDescriptor}. */
    public static class Builder {
        private final String format;
        private final Map<String, String> options = new HashMap<>();

        private Builder(String format) {
            this.format = format;
        }

        /** Sets the given option on the format. */
        public <T> Builder option(ConfigOption<T> configOption, T value) {
            Preconditions.checkNotNull(configOption, "Config option must not be null.");
            Preconditions.checkNotNull(value, "Value must not be null.");
            options.put(configOption.key(), ConfigurationUtils.convertValue(value, String.class));
            return this;
        }

        /**
         * Sets the given option on the format.
         *
         * <p>Note that format options must not be prefixed with the format identifier itself here.
         * For example,
         *
         * <pre>{@code
         * FormatDescriptor.forFormat("json")
         *   .option("ignore-parse-errors", "true")
         *   .build();
         * }</pre>
         *
         * <p>will automatically be converted into its prefixed form:
         *
         * <pre>{@code
         * 'format' = 'json'
         * 'json.ignore-parse-errors' = 'true'
         * }</pre>
         */
        public Builder option(String key, String value) {
            Preconditions.checkNotNull(key, "Key must not be null.");
            Preconditions.checkNotNull(value, "Value must not be null.");
            options.put(key, value);
            return this;
        }

        /** Returns an immutable instance of {@link FormatDescriptor}. */
        public FormatDescriptor build() {
            return new FormatDescriptor(format, options);
        }
    }
}
