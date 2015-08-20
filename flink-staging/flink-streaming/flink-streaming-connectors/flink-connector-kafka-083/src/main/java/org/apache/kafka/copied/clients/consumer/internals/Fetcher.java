/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.copied.clients.consumer.internals;

import org.apache.kafka.copied.clients.ClientResponse;
import org.apache.kafka.copied.clients.Metadata;
import org.apache.kafka.copied.clients.consumer.ConsumerRecord;
import org.apache.kafka.copied.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.copied.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.copied.common.Cluster;
import org.apache.kafka.copied.common.MetricName;
import org.apache.kafka.copied.common.Node;
import org.apache.kafka.copied.common.PartitionInfo;
import org.apache.kafka.copied.common.TopicPartition;
import org.apache.kafka.copied.common.errors.DisconnectException;
import org.apache.kafka.copied.common.errors.InvalidMetadataException;
import org.apache.kafka.copied.common.metrics.Metrics;
import org.apache.kafka.copied.common.metrics.Sensor;
import org.apache.kafka.copied.common.metrics.stats.Avg;
import org.apache.kafka.copied.common.metrics.stats.Count;
import org.apache.kafka.copied.common.metrics.stats.Max;
import org.apache.kafka.copied.common.metrics.stats.Rate;
import org.apache.kafka.copied.common.protocol.ApiKeys;
import org.apache.kafka.copied.common.protocol.Errors;
import org.apache.kafka.copied.common.record.LogEntry;
import org.apache.kafka.copied.common.record.MemoryRecords;
import org.apache.kafka.copied.common.requests.FetchRequest;
import org.apache.kafka.copied.common.requests.FetchResponse;
import org.apache.kafka.copied.common.requests.ListOffsetRequest;
import org.apache.kafka.copied.common.requests.ListOffsetResponse;
import org.apache.kafka.copied.common.serialization.Deserializer;
import org.apache.kafka.copied.common.utils.Time;
import org.apache.kafka.copied.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * This class manage the fetching process with the brokers.
 */
public class Fetcher<K, V> {
    private static final long EARLIEST_OFFSET_TIMESTAMP = -2L;
    private static final long LATEST_OFFSET_TIMESTAMP = -1L;

    private static final Logger log = LoggerFactory.getLogger(Fetcher.class);

    private final ConsumerNetworkClient client;
    private final Time time;
    private final int minBytes;
    private final int maxWaitMs;
    private final int fetchSize;
    private final long retryBackoffMs;
    private final boolean checkCrcs;
    private final Metadata metadata;
    private final FetchManagerMetrics sensors;
    private final SubscriptionState subscriptions;
    private final List<PartitionRecords<K, V>> records;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    public Fetcher(ConsumerNetworkClient client,
                   int minBytes,
                   int maxWaitMs,
                   int fetchSize,
                   boolean checkCrcs,
                   Deserializer<K> keyDeserializer,
                   Deserializer<V> valueDeserializer,
                   Metadata metadata,
                   SubscriptionState subscriptions,
                   Metrics metrics,
                   String metricGrpPrefix,
                   Map<String, String> metricTags,
                   Time time,
                   long retryBackoffMs) {

        this.time = time;
        this.client = client;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.minBytes = minBytes;
        this.maxWaitMs = maxWaitMs;
        this.fetchSize = fetchSize;
        this.checkCrcs = checkCrcs;

        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;

        this.records = new LinkedList<PartitionRecords<K, V>>();

        this.sensors = new FetchManagerMetrics(metrics, metricGrpPrefix, metricTags);
        this.retryBackoffMs = retryBackoffMs;
    }

    /**
     * Set-up a fetch request for any node that we have assigned partitions for which doesn't have one.
     *
     * @param cluster The current cluster metadata
     */
    public void initFetches(Cluster cluster) {
        for (Map.Entry<Node, FetchRequest> fetchEntry: createFetchRequests(cluster).entrySet()) {
            final FetchRequest fetch = fetchEntry.getValue();
            client.send(fetchEntry.getKey(), ApiKeys.FETCH, fetch)
                    .addListener(new RequestFutureListener<ClientResponse>() {
                        @Override
                        public void onSuccess(ClientResponse response) {
                            handleFetchResponse(response, fetch);
                        }

                        @Override
                        public void onFailure(RuntimeException e) {
                            log.debug("Fetch failed", e);
                        }
                    });
        }
    }

    /**
     * Update the fetch positions for the provided partitions.
     * @param partitions
     */
    public void updateFetchPositions(Set<TopicPartition> partitions) {
        // reset the fetch position to the committed position
        for (TopicPartition tp : partitions) {
            // skip if we already have a fetch position
            if (subscriptions.fetched(tp) != null)
                continue;

            // TODO: If there are several offsets to reset, we could submit offset requests in parallel
            if (subscriptions.isOffsetResetNeeded(tp)) {
                resetOffset(tp);
            } else if (subscriptions.committed(tp) == null) {
                // there's no committed position, so we need to reset with the default strategy
                subscriptions.needOffsetReset(tp);
                resetOffset(tp);
            } else {
                log.debug("Resetting offset for partition {} to the committed offset {}",
                        tp, subscriptions.committed(tp));
                subscriptions.seek(tp, subscriptions.committed(tp));
            }
        }
    }

    /**
     * Reset offsets for the given partition using the offset reset strategy.
     *
     * @param partition The given partition that needs reset offset
     * @throws NoOffsetForPartitionException If no offset reset strategy is defined
     */
    private void resetOffset(TopicPartition partition) {
        OffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
        final long timestamp;
        if (strategy == OffsetResetStrategy.EARLIEST)
            timestamp = EARLIEST_OFFSET_TIMESTAMP;
        else if (strategy == OffsetResetStrategy.LATEST)
            timestamp = LATEST_OFFSET_TIMESTAMP;
        else
            throw new NoOffsetForPartitionException("No offset is set and no reset policy is defined");

        log.debug("Resetting offset for partition {} to {} offset.", partition, strategy.name().toLowerCase());
        long offset = listOffset(partition, timestamp);
        this.subscriptions.seek(partition, offset);
    }

    /**
     * Fetch a single offset before the given timestamp for the partition.
     *
     * @param partition The partition that needs fetching offset.
     * @param timestamp The timestamp for fetching offset.
     * @return The offset of the message that is published before the given timestamp
     */
    private long listOffset(TopicPartition partition, long timestamp) {
        while (true) {
            RequestFuture<Long> future = sendListOffsetRequest(partition, timestamp);
            client.poll(future);

            if (future.succeeded())
                return future.value();

            if (!future.isRetriable())
                throw future.exception();

            if (future.exception() instanceof InvalidMetadataException)
                client.awaitMetadataUpdate();
            else
                Utils.sleep(retryBackoffMs);
        }
    }

    /**
     * Return the fetched records, empty the record buffer and update the consumed position.
     *
     * @return The fetched records per partition
     */
    public Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchedRecords() {
        if (this.subscriptions.partitionAssignmentNeeded()) {
            return Collections.emptyMap();
        } else {
            Map<TopicPartition, List<ConsumerRecord<K, V>>> drained = new HashMap<TopicPartition, List<ConsumerRecord<K, V>>>();
            for (PartitionRecords<K, V> part : this.records) {
                Long consumed = subscriptions.consumed(part.partition);
                if (this.subscriptions.assignedPartitions().contains(part.partition)
                    && (consumed == null || part.fetchOffset == consumed)) {
                    List<ConsumerRecord<K, V>> records = drained.get(part.partition);
                    if (records == null) {
                        records = part.records;
                        drained.put(part.partition, records);
                    } else {
                        records.addAll(part.records);
                    }
                    subscriptions.consumed(part.partition, part.records.get(part.records.size() - 1).offset() + 1);
                } else {
                    // these records aren't next in line based on the last consumed position, ignore them
                    // they must be from an obsolete request
                    log.debug("Ignoring fetched records for {} at offset {}", part.partition, part.fetchOffset);
                }
            }
            this.records.clear();
            return drained;
        }
    }

    /**
     * Fetch a single offset before the given timestamp for the partition.
     *
     * @param topicPartition The partition that needs fetching offset.
     * @param timestamp The timestamp for fetching offset.
     * @return A response which can be polled to obtain the corresponding offset.
     */
    private RequestFuture<Long> sendListOffsetRequest(final TopicPartition topicPartition, long timestamp) {
        Map<TopicPartition, ListOffsetRequest.PartitionData> partitions = new HashMap<TopicPartition, ListOffsetRequest.PartitionData>(1);
        partitions.put(topicPartition, new ListOffsetRequest.PartitionData(timestamp, 1));
        PartitionInfo info = metadata.fetch().partition(topicPartition);
        if (info == null) {
            metadata.add(topicPartition.topic());
            log.debug("Partition {} is unknown for fetching offset, wait for metadata refresh", topicPartition);
            return RequestFuture.staleMetadata();
        } else if (info.leader() == null) {
            log.debug("Leader for partition {} unavailable for fetching offset, wait for metadata refresh", topicPartition);
            return RequestFuture.leaderNotAvailable();
        } else {
            Node node = info.leader();
            ListOffsetRequest request = new ListOffsetRequest(-1, partitions);
            return client.send(node, ApiKeys.LIST_OFFSETS, request)
                    .compose(new RequestFutureAdapter<ClientResponse, Long>() {
                        @Override
                        public void onSuccess(ClientResponse response, RequestFuture<Long> future) {
                            handleListOffsetResponse(topicPartition, response, future);
                        }
                    });
        }
    }

    /**
     * Callback for the response of the list offset call above.
     * @param topicPartition The partition that was fetched
     * @param clientResponse The response from the server.
     */
    private void handleListOffsetResponse(TopicPartition topicPartition,
                                          ClientResponse clientResponse,
                                          RequestFuture<Long> future) {
        if (clientResponse.wasDisconnected()) {
            future.raise(new DisconnectException());
        } else {
            ListOffsetResponse lor = new ListOffsetResponse(clientResponse.responseBody());
            short errorCode = lor.responseData().get(topicPartition).errorCode;
            if (errorCode == Errors.NONE.code()) {
                List<Long> offsets = lor.responseData().get(topicPartition).offsets;
                if (offsets.size() != 1)
                    throw new IllegalStateException("This should not happen.");
                long offset = offsets.get(0);
                log.debug("Fetched offset {} for partition {}", offset, topicPartition);

                future.complete(offset);
            } else if (errorCode == Errors.NOT_LEADER_FOR_PARTITION.code()
                    || errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code()) {
                log.warn("Attempt to fetch offsets for partition {} failed due to obsolete leadership information, retrying.",
                        topicPartition);
                future.raise(Errors.forCode(errorCode));
            } else {
                log.error("Attempt to fetch offsets for partition {} failed due to: {}",
                        topicPartition, Errors.forCode(errorCode).exception().getMessage());
                future.raise(new StaleMetadataException());
            }
        }
    }

    /**
     * Create fetch requests for all nodes for which we have assigned partitions
     * that have no existing requests in flight.
     */
    private Map<Node, FetchRequest> createFetchRequests(Cluster cluster) {
        // create the fetch info
        Map<Node, Map<TopicPartition, FetchRequest.PartitionData>> fetchable = new HashMap<Node, Map<TopicPartition, FetchRequest.PartitionData>>();
        for (TopicPartition partition : subscriptions.assignedPartitions()) {
            Node node = cluster.leaderFor(partition);
            if (node == null) {
                metadata.requestUpdate();
            } else if (this.client.pendingRequestCount(node) == 0) {
                // if there is a leader and no in-flight requests, issue a new fetch
                Map<TopicPartition, FetchRequest.PartitionData> fetch = fetchable.get(node);
                if (fetch == null) {
                    fetch = new HashMap<TopicPartition, FetchRequest.PartitionData>();
                    fetchable.put(node, fetch);
                }
                long offset = this.subscriptions.fetched(partition);
                fetch.put(partition, new FetchRequest.PartitionData(offset, this.fetchSize));
            }
        }

        // create the fetches
        Map<Node, FetchRequest> requests = new HashMap<Node, FetchRequest>();
        for (Map.Entry<Node, Map<TopicPartition, FetchRequest.PartitionData>> entry : fetchable.entrySet()) {
            Node node = entry.getKey();
            FetchRequest fetch = new FetchRequest(this.maxWaitMs, this.minBytes, entry.getValue());
            requests.put(node, fetch);
        }
        return requests;
    }

    /**
     * The callback for fetch completion
     */
    private void handleFetchResponse(ClientResponse resp, FetchRequest request) {
        if (resp.wasDisconnected()) {
            int correlation = resp.request().request().header().correlationId();
            log.debug("Cancelled fetch request {} with correlation id {} due to node {} being disconnected",
                resp.request(), correlation, resp.request().request().destination());
        } else {
            int totalBytes = 0;
            int totalCount = 0;
            FetchResponse response = new FetchResponse(resp.responseBody());
            for (Map.Entry<TopicPartition, FetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                FetchResponse.PartitionData partition = entry.getValue();
                if (!subscriptions.assignedPartitions().contains(tp)) {
                    log.debug("Ignoring fetched data for partition {} which is no longer assigned.", tp);
                } else if (partition.errorCode == Errors.NONE.code()) {
                    int bytes = 0;
                    ByteBuffer buffer = partition.recordSet;
                    MemoryRecords records = MemoryRecords.readableRecords(buffer);
                    long fetchOffset = request.fetchData().get(tp).offset;
                    List<ConsumerRecord<K, V>> parsed = new ArrayList<ConsumerRecord<K, V>>();
                    for (LogEntry logEntry : records) {
                        parsed.add(parseRecord(tp, logEntry));
                        bytes += logEntry.size();
                    }
                    if (parsed.size() > 0) {
                        ConsumerRecord<K, V> record = parsed.get(parsed.size() - 1);
                        this.subscriptions.fetched(tp, record.offset() + 1);
                        this.records.add(new PartitionRecords<K, V>(fetchOffset, tp, parsed));
                        this.sensors.recordsFetchLag.record(partition.highWatermark - record.offset());
                    }
                    this.sensors.recordTopicFetchMetrics(tp.topic(), bytes, parsed.size());
                    totalBytes += bytes;
                    totalCount += parsed.size();
                } else if (partition.errorCode == Errors.NOT_LEADER_FOR_PARTITION.code()
                    || partition.errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code()) {
                    this.metadata.requestUpdate();
                } else if (partition.errorCode == Errors.OFFSET_OUT_OF_RANGE.code()) {
                    // TODO: this could be optimized by grouping all out-of-range partitions
                    log.info("Fetch offset {} is out of range, resetting offset", subscriptions.fetched(tp));
                    subscriptions.needOffsetReset(tp);
                } else if (partition.errorCode == Errors.UNKNOWN.code()) {
                    log.warn("Unknown error fetching data for topic-partition {}", tp);
                } else {
                    throw new IllegalStateException("Unexpected error code " + partition.errorCode + " while fetching data");
                }
            }
            this.sensors.bytesFetched.record(totalBytes);
            this.sensors.recordsFetched.record(totalCount);
        }
        this.sensors.fetchLatency.record(resp.requestLatencyMs());
    }

    /**
     * Parse the record entry, deserializing the key / value fields if necessary
     */
    private ConsumerRecord<K, V> parseRecord(TopicPartition partition, LogEntry logEntry) {
        if (this.checkCrcs)
            logEntry.record().ensureValid();

        long offset = logEntry.offset();
        ByteBuffer keyBytes = logEntry.record().key();
        K key = keyBytes == null ? null : this.keyDeserializer.deserialize(partition.topic(), Utils.toArray(keyBytes));
        ByteBuffer valueBytes = logEntry.record().value();
        V value = valueBytes == null ? null : this.valueDeserializer.deserialize(partition.topic(), Utils.toArray(valueBytes));

        return new ConsumerRecord<K, V>(partition.topic(), partition.partition(), offset, key, value);
    }

    private static class PartitionRecords<K, V> {
        public long fetchOffset;
        public TopicPartition partition;
        public List<ConsumerRecord<K, V>> records;

        public PartitionRecords(long fetchOffset, TopicPartition partition, List<ConsumerRecord<K, V>> records) {
            this.fetchOffset = fetchOffset;
            this.partition = partition;
            this.records = records;
        }
    }

    private class FetchManagerMetrics {
        public final Metrics metrics;
        public final String metricGrpName;

        public final Sensor bytesFetched;
        public final Sensor recordsFetched;
        public final Sensor fetchLatency;
        public final Sensor recordsFetchLag;


        public FetchManagerMetrics(Metrics metrics, String metricGrpPrefix, Map<String, String> tags) {
            this.metrics = metrics;
            this.metricGrpName = metricGrpPrefix + "-fetch-manager-metrics";

            this.bytesFetched = metrics.sensor("bytes-fetched");
            this.bytesFetched.add(new MetricName("fetch-size-avg",
                this.metricGrpName,
                "The average number of bytes fetched per request",
                tags), new Avg());
            this.bytesFetched.add(new MetricName("fetch-size-max",
                this.metricGrpName,
                "The maximum number of bytes fetched per request",
                tags), new Max());
            this.bytesFetched.add(new MetricName("bytes-consumed-rate",
                this.metricGrpName,
                "The average number of bytes consumed per second",
                tags), new Rate());

            this.recordsFetched = metrics.sensor("records-fetched");
            this.recordsFetched.add(new MetricName("records-per-request-avg",
                this.metricGrpName,
                "The average number of records in each request",
                tags), new Avg());
            this.recordsFetched.add(new MetricName("records-consumed-rate",
                this.metricGrpName,
                "The average number of records consumed per second",
                tags), new Rate());

            this.fetchLatency = metrics.sensor("fetch-latency");
            this.fetchLatency.add(new MetricName("fetch-latency-avg",
                this.metricGrpName,
                "The average time taken for a fetch request.",
                tags), new Avg());
            this.fetchLatency.add(new MetricName("fetch-latency-max",
                this.metricGrpName,
                "The max time taken for any fetch request.",
                tags), new Max());
            this.fetchLatency.add(new MetricName("fetch-rate",
                this.metricGrpName,
                "The number of fetch requests per second.",
                tags), new Rate(new Count()));

            this.recordsFetchLag = metrics.sensor("records-lag");
            this.recordsFetchLag.add(new MetricName("records-lag-max",
                this.metricGrpName,
                "The maximum lag in terms of number of records for any partition in this window",
                tags), new Max());
        }

        public void recordTopicFetchMetrics(String topic, int bytes, int records) {
            // record bytes fetched
            String name = "topic." + topic + ".bytes-fetched";
            Sensor bytesFetched = this.metrics.getSensor(name);
            if (bytesFetched == null)
                bytesFetched = this.metrics.sensor(name);
            bytesFetched.record(bytes);

            // record records fetched
            name = "topic." + topic + ".records-fetched";
            Sensor recordsFetched = this.metrics.getSensor(name);
            if (recordsFetched == null)
                recordsFetched = this.metrics.sensor(name);
            recordsFetched.record(records);
        }
    }
}
