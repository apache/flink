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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Table format factory for providing configured instances of Protobuf to RowData {@link
 * SerializationSchema} and {@link DeserializationSchema}.
 */
public class PbFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "protobuf";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        return new PbDecodingFormat(buildConfig(formatOptions));
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        return new PbEncodingFormat(buildConfig(formatOptions));
    }

    private static PbFormatConfig buildConfig(ReadableConfig formatOptions) {
        PbFormatConfig.PbFormatConfigBuilder configBuilder =
                new PbFormatConfig.PbFormatConfigBuilder();
        configBuilder.messageClassName(formatOptions.get(PbFormatOptions.MESSAGE_CLASS_NAME));
        formatOptions
                .getOptional(PbFormatOptions.IGNORE_PARSE_ERRORS)
                .ifPresent(configBuilder::ignoreParseErrors);
        formatOptions
                .getOptional(PbFormatOptions.READ_DEFAULT_VALUES)
                .ifPresent(configBuilder::readDefaultValues);
        formatOptions
                .getOptional(PbFormatOptions.WRITE_NULL_STRING_LITERAL)
                .ifPresent(configBuilder::writeNullStringLiterals);
        return configBuilder.build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> result = new HashSet<>();
        result.add(PbFormatOptions.MESSAGE_CLASS_NAME);
        return result;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> result = new HashSet<>();
        result.add(PbFormatOptions.IGNORE_PARSE_ERRORS);
        result.add(PbFormatOptions.READ_DEFAULT_VALUES);
        result.add(PbFormatOptions.WRITE_NULL_STRING_LITERAL);
        return result;
    }
}
