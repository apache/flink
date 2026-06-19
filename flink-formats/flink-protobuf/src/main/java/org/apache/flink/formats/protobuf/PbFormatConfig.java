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

package org.apache.flink.formats.protobuf;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.formats.protobuf.PbFormatOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.formats.protobuf.PbFormatOptions.READ_DEFAULT_VALUES;
import static org.apache.flink.formats.protobuf.PbFormatOptions.WRITE_NULL_STRING_LITERAL;

/** Config of protobuf configs. */
public class PbFormatConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String messageClassName;
    private final boolean ignoreParseErrors;
    private final boolean readDefaultValues;
    private final String writeNullStringLiterals;

    public PbFormatConfig(
            String messageClassName,
            boolean ignoreParseErrors,
            boolean readDefaultValues,
            String writeNullStringLiterals) {
        this.messageClassName = messageClassName;
        this.ignoreParseErrors = ignoreParseErrors;
        this.readDefaultValues = readDefaultValues;
        this.writeNullStringLiterals = writeNullStringLiterals;
    }

    public String getMessageClassName() {
        return messageClassName;
    }

    public boolean isIgnoreParseErrors() {
        return ignoreParseErrors;
    }

    public boolean isReadDefaultValues() {
        return readDefaultValues;
    }

    public String getWriteNullStringLiterals() {
        return writeNullStringLiterals;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PbFormatConfig that = (PbFormatConfig) o;
        return ignoreParseErrors == that.ignoreParseErrors
                && readDefaultValues == that.readDefaultValues
                && Objects.equals(messageClassName, that.messageClassName)
                && Objects.equals(writeNullStringLiterals, that.writeNullStringLiterals);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                messageClassName, ignoreParseErrors, readDefaultValues, writeNullStringLiterals);
    }

    /** Builder of PbFormatConfig. */
    public static class PbFormatConfigBuilder {
        private String messageClassName;
        private boolean ignoreParseErrors = IGNORE_PARSE_ERRORS.defaultValue();
        private boolean readDefaultValues = READ_DEFAULT_VALUES.defaultValue();
        private String writeNullStringLiterals = WRITE_NULL_STRING_LITERAL.defaultValue();

        public PbFormatConfigBuilder messageClassName(String messageClassName) {
            this.messageClassName = messageClassName;
            return this;
        }

        public PbFormatConfigBuilder ignoreParseErrors(boolean ignoreParseErrors) {
            this.ignoreParseErrors = ignoreParseErrors;
            return this;
        }

        public PbFormatConfigBuilder readDefaultValues(boolean readDefaultValues) {
            this.readDefaultValues = readDefaultValues;
            return this;
        }

        public PbFormatConfigBuilder writeNullStringLiterals(String writeNullStringLiterals) {
            this.writeNullStringLiterals = writeNullStringLiterals;
            return this;
        }

        public PbFormatConfig build() {
            return new PbFormatConfig(
                    messageClassName,
                    ignoreParseErrors,
                    readDefaultValues,
                    writeNullStringLiterals);
        }
    }
}
