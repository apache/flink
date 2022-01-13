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

package org.apache.flink.streaming.connectors.kinesis.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.aws.table.util.AWSOptionUtils;
import org.apache.flink.connector.kinesis.table.util.KinesisAsyncClientOptionsUtils;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.kinesis.table.KinesisConnectorOptions.STREAM;

/**
 * Class for handling kinesis table options, including key mapping and validations and property
 * extraction. Class uses options decorators {@link AWSOptionUtils}, {@link
 * KinesisAsyncClientOptionsUtils} and {@link KinesisConsumerOptionsUtil} for handling each
 * specified set of options.
 */
@Internal
public class KinesisConnectorOptionsUtil {

    private final KinesisConsumerOptionsUtil kinesisConsumerOptionsUtil;
    private final Map<String, String> resolvedOptions;
    private final ReadableConfig tableOptions;

    public KinesisConnectorOptionsUtil(Map<String, String> options, ReadableConfig tableOptions) {
        this.resolvedOptions = options;
        this.tableOptions = tableOptions;
        this.kinesisConsumerOptionsUtil =
                new KinesisConsumerOptionsUtil(resolvedOptions, tableOptions.get(STREAM));
    }

    public Properties getValidatedSourceConfigurations() {
        return kinesisConsumerOptionsUtil.getValidatedConfigurations();
    }

    public List<String> getNonValidatedPrefixes() {
        return kinesisConsumerOptionsUtil.getNonValidatedPrefixes();
    }
}
