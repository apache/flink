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
package org.apache.flink.streaming.connectors.gcp.pubsub.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** PubSub-backed {@link ScanTableSource}. */
@Internal
public class PubsubDynamicSource implements ScanTableSource {

    private static Logger logger = LoggerFactory.getLogger(PubsubDynamicSource.class);

    /** Name of the PubSub project backing this table. */
    private final String project;
    /** Name of the PubSub subscription backing this table. */
    private final String subscription;
    /** Scan format for decoding records from PubSub. */
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    /** Data type that describes the final output of the source. */
    private final DataType producedDataType;

    private boolean checkpointDisabled;

    public PubsubDynamicSource(
            String project,
            String subscription,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            DataType producedDataType,
            boolean checkpointDisabled) {

        this.project = project;
        this.subscription = subscription;
        this.decodingFormat = decodingFormat;
        this.producedDataType = producedDataType;
        this.checkpointDisabled = checkpointDisabled;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final DeserializationSchema<RowData> deserializer =
                decodingFormat.createRuntimeDecoder(runtimeProviderContext, producedDataType);

        try {
            PubSubSource<RowData> source =
                    PubSubSource.newBuilder()
                            .withDeserializationSchema(deserializer)
                            .withProjectName(project)
                            .withSubscriptionName(subscription)
                            .disableCheckpoint(checkpointDisabled)
                            .build();
            return SourceFunctionProvider.of(source, false);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create PubSub source.", e);
        }
    }

    @Override
    public DynamicTableSource copy() {
        return new PubsubDynamicSource(
                project, subscription, decodingFormat, producedDataType, checkpointDisabled);
    }

    @Override
    public String asSummaryString() {
        return "PubSub";
    }
}
