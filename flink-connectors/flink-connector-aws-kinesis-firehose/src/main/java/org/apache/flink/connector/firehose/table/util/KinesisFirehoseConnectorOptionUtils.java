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

package org.apache.flink.connector.firehose.table.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.aws.table.util.AsyncClientOptionsUtils;
import org.apache.flink.connector.base.table.sink.options.AsyncSinkConfigurationValidator;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.firehose.table.KinesisFirehoseConnectorOptions.DELIVERY_STREAM;
import static org.apache.flink.connector.firehose.table.KinesisFirehoseConnectorOptions.SINK_FAIL_ON_ERROR;

/** Class for extracting firehose configurations from table options. */
@Internal
public class KinesisFirehoseConnectorOptionUtils {

    public static final String FIREHOSE_CLIENT_PROPERTIES_KEY = "sink.client.properties";

    private final AsyncSinkConfigurationValidator asyncSinkConfigurationValidator;
    private final AsyncClientOptionsUtils asyncClientOptionsUtils;
    private final Map<String, String> resolvedOptions;
    private final ReadableConfig tableOptions;

    public KinesisFirehoseConnectorOptionUtils(
            Map<String, String> resolvedOptions, ReadableConfig tableOptions) {
        this.asyncSinkConfigurationValidator = new AsyncSinkConfigurationValidator(tableOptions);
        this.asyncClientOptionsUtils = new AsyncClientOptionsUtils(resolvedOptions);
        this.resolvedOptions = resolvedOptions;
        this.tableOptions = tableOptions;
    }

    public List<String> getNonValidatedPrefixes() {
        return this.asyncClientOptionsUtils.getNonValidatedPrefixes();
    }

    public Properties getSinkProperties() {
        Properties properties = asyncSinkConfigurationValidator.getValidatedConfigurations();
        properties.put(DELIVERY_STREAM.key(), tableOptions.get(DELIVERY_STREAM));
        Properties kinesisClientProps = asyncClientOptionsUtils.getValidatedConfigurations();
        properties.put(FIREHOSE_CLIENT_PROPERTIES_KEY, kinesisClientProps);
        if (tableOptions.getOptional(SINK_FAIL_ON_ERROR).isPresent()) {
            properties.put(
                    SINK_FAIL_ON_ERROR.key(), tableOptions.getOptional(SINK_FAIL_ON_ERROR).get());
        }
        return properties;
    }
}
