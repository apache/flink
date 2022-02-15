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

package org.apache.flink.connector.elasticsearch.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.elasticsearch.common.ElasticsearchUtil;
import org.apache.flink.connector.elasticsearch.common.NetworkClientConfig;
import org.apache.flink.connector.elasticsearch.source.Elasticsearch7SourceConfiguration;
import org.apache.flink.connector.elasticsearch.source.split.Elasticsearch7Split;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The enumerator class for the ElasticsearchSource. To read data from Elasticsearch the enumerator
 * has to initialize an Elasticsearch pit first. A pit (point in time) is a lightweight view into
 * the state of the data as it existed when initialized. Changes happening after the pit was
 * initialized are only visible to more recent pits. Using the same pit for all search requests
 * allows the source to prevent inconsistencies in case data is added or removed between search
 * requests. After initializing the pit, the enumerator creates the number of source splits defined
 * by the number of search slices. More information about the splits can be found in {@link
 * Elasticsearch7Split}.
 */
@Internal
public class Elasticsearch7SourceEnumerator
        implements SplitEnumerator<Elasticsearch7Split, Elasticsearch7SourceEnumState> {
    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch7SourceEnumerator.class);

    private final Elasticsearch7SourceConfiguration sourceConfiguration;

    private final NetworkClientConfig networkClientConfig;

    private final SplitEnumeratorContext<Elasticsearch7Split> context;

    private ArrayList<Elasticsearch7Split> splits;

    private RestHighLevelClient client;

    private final LinkedHashMap<Integer, String> readersAwaitingSplit;

    private boolean isInitialized = false;

    public Elasticsearch7SourceEnumerator(
            Elasticsearch7SourceConfiguration sourceConfiguration,
            NetworkClientConfig networkClientConfig,
            SplitEnumeratorContext<Elasticsearch7Split> context) {
        this(sourceConfiguration, networkClientConfig, context, Collections.emptySet());
    }

    public Elasticsearch7SourceEnumerator(
            Elasticsearch7SourceConfiguration sourceConfiguration,
            NetworkClientConfig networkClientConfig,
            SplitEnumeratorContext<Elasticsearch7Split> context,
            Collection<Elasticsearch7Split> restoredSplits) {
        this.sourceConfiguration = sourceConfiguration;
        this.networkClientConfig = networkClientConfig;
        this.context = context;
        this.splits = new ArrayList<>(restoredSplits);
        this.readersAwaitingSplit = new LinkedHashMap<>();
    }

    @Override
    public void start() {
        client = getRestClient();
        LOG.info("Starting the ElasticsearchSourceEnumerator.");

        if (splits.isEmpty()) {
            context.callAsync(this::initializePointInTime, this::handlePointInTimeCreation);
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String hostname) {
        readersAwaitingSplit.put(subtaskId, hostname);
        assignSplits();
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        LOG.error("Received unrecognized event: {}", sourceEvent);
    }

    @Override
    public void addSplitsBack(List<Elasticsearch7Split> splits, int subtaskId) {
        LOG.debug("ElasticsearchEnumerator adds splits back: {}", splits);
        this.splits.addAll(splits);
    }

    @Override
    public Elasticsearch7SourceEnumState snapshotState(long checkpointId) throws Exception {
        return Elasticsearch7SourceEnumState.fromCollectionSnapshot(splits);
    }

    // ----------------- private methods -------------------

    private RestHighLevelClient getRestClient() {
        return new RestHighLevelClient(
                ElasticsearchUtil.configureRestClientBuilder(
                        RestClient.builder(sourceConfiguration.getHosts().toArray(new HttpHost[0])),
                        networkClientConfig));
    }

    // This method should only be invoked in the coordinator executor thread.
    private String initializePointInTime() throws IOException {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(sourceConfiguration.getIndex());
        request.keepAlive(
                TimeValue.timeValueMillis(sourceConfiguration.getPitKeepAlive().toMillis()));
        OpenPointInTimeResponse response = client.openPointInTime(request, RequestOptions.DEFAULT);
        String pitId = response.getPointInTimeId();
        LOG.debug("Created point in time (pit) with ID {}", pitId);

        return pitId;
    }

    // This method should only be invoked in the coordinator executor thread.
    private void handlePointInTimeCreation(String pitId, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to create point in time (pit) due to ", t);
        }

        splits =
                IntStream.range(0, sourceConfiguration.getNumberOfSlices())
                        .mapToObj(i -> new Elasticsearch7Split(pitId, i))
                        .collect(Collectors.toCollection(ArrayList::new));

        isInitialized = true;
        assignSplits();
    }

    private void assignSplits() {
        final Iterator<Map.Entry<Integer, String>> waitingReaders =
                readersAwaitingSplit.entrySet().iterator();

        while (waitingReaders.hasNext()) {
            final Map.Entry<Integer, String> nextReader = waitingReaders.next();

            // reader failed between sending the request and now
            if (!context.registeredReaders().containsKey(nextReader.getKey())) {
                waitingReaders.remove();
                continue;
            }

            final int subtaskId = nextReader.getKey();
            final Optional<Elasticsearch7Split> nextSplit = getNextSplit();
            if (nextSplit.isPresent()) {
                final Elasticsearch7Split split = nextSplit.get();
                context.assignSplit(split, subtaskId);
                LOG.info("Assigned split to subtask {} : {}", subtaskId, split);
                waitingReaders.remove();
            } else {
                if (isInitialized) {
                    context.signalNoMoreSplits(subtaskId);
                    LOG.info("No more splits available for subtask {}", subtaskId);
                    waitingReaders.remove();
                } else {
                    break;
                }
            }
        }
    }

    private Optional<Elasticsearch7Split> getNextSplit() {
        int size = splits.size();
        return size == 0 ? Optional.empty() : Optional.of(splits.remove(size - 1));
    }
}
