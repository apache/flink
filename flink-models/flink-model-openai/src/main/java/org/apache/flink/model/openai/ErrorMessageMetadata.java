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

package org.apache.flink.model.openai;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.ExceptionUtils;

import com.openai.core.http.Headers;
import com.openai.errors.OpenAIServiceException;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Metadata that can be read from the output row about error messages. Referenced from Flink HTTP
 * Connector's ReadableMetadata.
 */
public enum ErrorMessageMetadata {
    ERROR_STRING(
            "error-string",
            DataTypes.STRING(),
            x -> BinaryStringData.fromString(x.getMessage()),
            "A message associated with the error"),
    HTTP_STATUS_CODE(
            "http-status-code",
            DataTypes.INT(),
            e ->
                    ExceptionUtils.findThrowable(e, OpenAIServiceException.class)
                            .map(OpenAIServiceException::statusCode)
                            .orElse(null),
            "The HTTP status code"),
    HTTP_HEADERS_MAP(
            "http-headers-map",
            DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.STRING())),
            e ->
                    ExceptionUtils.findThrowable(e, OpenAIServiceException.class)
                            .map(
                                    e1 -> {
                                        Map<StringData, ArrayData> map = new HashMap<>();
                                        Headers headers = e1.headers();
                                        for (String name : headers.names()) {
                                            map.put(
                                                    BinaryStringData.fromString(name),
                                                    new GenericArrayData(
                                                            headers.values(name).stream()
                                                                    .map(
                                                                            BinaryStringData
                                                                                    ::fromString)
                                                                    .toArray()));
                                        }
                                        return new GenericMapData(map);
                                    })
                            .orElse(null),
            "The headers returned with the response");

    final String key;
    final DataType dataType;
    final Function<Throwable, Object> converter;
    final String description;

    ErrorMessageMetadata(
            String key,
            DataType dataType,
            Function<Throwable, Object> converter,
            String description) {
        this.key = key;
        this.dataType = dataType;
        this.converter = converter;
        this.description = description;
    }

    static @Nullable ErrorMessageMetadata get(String key) {
        for (ErrorMessageMetadata value : values()) {
            if (value.key.equals(key)) {
                return value;
            }
        }
        return null;
    }

    static String getAllKeysAndDescriptions() {
        return Arrays.stream(values())
                .map(value -> value.key + ":\t" + value.description)
                .collect(Collectors.joining("\n"));
    }
}
