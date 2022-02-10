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
import org.apache.flink.connector.elasticsearch.source.Elasticsearch7SourceConfiguration;
import org.apache.flink.connector.elasticsearch.source.split.Elasticsearch7Split;
import org.apache.flink.connector.elasticsearch.source.split.Elasticsearch7SplitState;

import org.elasticsearch.search.SearchHit;

import java.util.Map;

/** The source reader for Elasticsearch. */
public class Elasticsearch7SourceReader<OUT>
        extends SingleThreadMultiplexSourceReaderBase<
                SearchHit, OUT, Elasticsearch7Split, Elasticsearch7SplitState> {

    public Elasticsearch7SourceReader(
            Configuration configuration,
            SourceReaderContext readerContext,
            Elasticsearch7SourceConfiguration sourceConfiguration,
            NetworkClientConfig networkClientConfig,
            Elasticsearch7SearchHitDeserializationSchema<OUT> deserializationSchema) {
        super(
                () -> new Elasticsearch7SplitReader(sourceConfiguration, networkClientConfig),
                new Elasticsearch7RecordEmitter<>(deserializationSchema),
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
    protected void onSplitFinished(Map<String, Elasticsearch7SplitState> finishedSplitIds) {
        context.sendSplitRequest();
    }

    @Override
    protected Elasticsearch7SplitState initializedState(Elasticsearch7Split split) {
        return new Elasticsearch7SplitState(split);
    }

    @Override
    protected Elasticsearch7Split toSplitType(String splitId, Elasticsearch7SplitState splitState) {
        return splitState.toElasticsearchSplit();
    }
}
