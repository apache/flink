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

package org.apache.flink.formats.json;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;

import javax.annotation.Nullable;

import java.io.IOException;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Tool class used to convert fields from {@link JsonParser} to {@link RowData} which has a higher
 * parsing efficiency.
 */
@Internal
public class JsonParserRowDataDeserializationSchema extends AbstractJsonDeserializationSchema {

    /**
     * Runtime converter that converts {@link JsonParser}s into objects of Flink SQL internal data
     * structures.
     */
    private final JsonParserToRowDataConverters.JsonParserToRowDataConverter runtimeConverter;

    public JsonParserRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {
        this(rowType, resultTypeInfo, failOnMissingField, ignoreParseErrors, timestampFormat, null);
    }

    public JsonParserRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat,
            @Nullable String[][] projectedFields) {
        super(rowType, resultTypeInfo, failOnMissingField, ignoreParseErrors, timestampFormat);
        this.runtimeConverter =
                new JsonParserToRowDataConverters(
                                failOnMissingField, ignoreParseErrors, timestampFormat)
                        .createConverter(projectedFields, checkNotNull(rowType));
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        // return null when there is no token
        if (message == null || message.length == 0) {
            return null;
        }
        try (JsonParser root = objectMapper.getFactory().createParser(message)) {
            /* First: must point to a token; if not pointing to one, advance.
             * This occurs before first read from JsonParser, as well as
             * after clearing of current token.
             */
            if (root.currentToken() == null) {
                root.nextToken();
            }
            if (root.currentToken() != JsonToken.START_OBJECT) {
                throw JsonMappingException.from(root, "No content to map due to end-of-input");
            }
            return (RowData) runtimeConverter.convert(root);
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new IOException(
                    format("Failed to deserialize JSON '%s'.", new String(message)), t);
        }
    }
}
