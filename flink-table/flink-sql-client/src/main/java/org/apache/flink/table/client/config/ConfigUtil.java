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

package org.apache.flink.table.client.config;

import org.apache.flink.table.descriptors.DescriptorProperties;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.io.IOContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Auxiliary functions for configuration file handling.
 *
 * @deprecated This will be removed in Flink 1.14 with dropping support of {@code sql-client.yaml}
 *     configuration file.
 */
@Deprecated
public class ConfigUtil {

    private ConfigUtil() {
        // private
    }

    /** Normalizes key-value properties from Yaml in the normalized format of the Table API. */
    public static DescriptorProperties normalizeYaml(Map<String, Object> yamlMap) {
        final Map<String, String> normalized = new HashMap<>();
        yamlMap.forEach((k, v) -> normalizeYamlObject(normalized, k, v));
        final DescriptorProperties properties = new DescriptorProperties(true);
        properties.putProperties(normalized);
        return properties;
    }

    private static void normalizeYamlObject(
            Map<String, String> normalized, String key, Object value) {
        if (value instanceof Map) {
            final Map<?, ?> map = (Map<?, ?>) value;
            map.forEach((k, v) -> normalizeYamlObject(normalized, key + "." + k, v));
        } else if (value instanceof List) {
            final List<?> list = (List<?>) value;
            for (int i = 0; i < list.size(); i++) {
                normalizeYamlObject(normalized, key + "." + i, list.get(i));
            }
        } else {
            normalized.put(key, value.toString());
        }
    }

    // --------------------------------------------------------------------------------------------

    /** Modified object mapper that converts to lower-case keys. */
    public static class LowerCaseYamlMapper extends ObjectMapper {
        public LowerCaseYamlMapper() {
            super(
                    new YAMLFactory() {
                        @Override
                        protected YAMLParser _createParser(InputStream in, IOContext ctxt)
                                throws IOException {
                            final Reader r = _createReader(in, null, ctxt);
                            // normalize all key to lower case keys
                            return new YAMLParser(
                                    ctxt,
                                    _getBufferRecycler(),
                                    _parserFeatures,
                                    _yamlParserFeatures,
                                    _objectCodec,
                                    r) {
                                @Override
                                public String getCurrentName() throws IOException {
                                    if (_currToken == JsonToken.FIELD_NAME) {
                                        return _currentFieldName.toLowerCase();
                                    }
                                    return super.getCurrentName();
                                }

                                @Override
                                public String getText() throws IOException {
                                    if (_currToken == JsonToken.FIELD_NAME) {
                                        return _currentFieldName.toLowerCase();
                                    }
                                    return super.getText();
                                }
                            };
                        }
                    });
        }
    }
}
