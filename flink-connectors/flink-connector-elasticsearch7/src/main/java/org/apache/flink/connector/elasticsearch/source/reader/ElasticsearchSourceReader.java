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

package org.apache.flink.connector.elasticsearch.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.elasticsearch.common.NetworkClientConfig;
import org.apache.flink.connector.elasticsearch.source.ElasticsearchSourceConfiguration;
import org.apache.flink.connector.elasticsearch.source.split.ElasticsearchSplit;
import org.apache.flink.connector.elasticsearch.source.split.ElasticsearchSplitState;

import java.util.Map;

/** The source reader for Elasticsearch. */
public class ElasticsearchSourceReader<OUT>
        extends SingleThreadMultiplexSourceReaderBase<
                ElasticsearchRecord<OUT>, OUT, ElasticsearchSplit, ElasticsearchSplitState> {

    public ElasticsearchSourceReader(
            Configuration configuration,
            SourceReaderContext readerContext,
            ElasticsearchSourceConfiguration sourceConfiguration,
            NetworkClientConfig networkClientConfig,
            ElasticsearchSearchHitDeserializationSchema<OUT> deserializationSchema) {
        super(
                () ->
                        new ElasticsearchSplitReader<>(
                                sourceConfiguration, networkClientConfig, deserializationSchema),
                new ElasticsearchRecordEmitter<>(),
                configuration,
                readerContext);
    }

    @Override
    public void start() {
        // we request a split only if we did not get splits during the checkpoint restore
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, ElasticsearchSplitState> finishedSplitIds) {
        context.sendSplitRequest();
    }

    @Override
    protected ElasticsearchSplitState initializedState(ElasticsearchSplit split) {
        return new ElasticsearchSplitState(split);
    }

    @Override
    protected ElasticsearchSplit toSplitType(String splitId, ElasticsearchSplitState splitState) {
        return splitState.toElasticsearchSplit();
    }
}
