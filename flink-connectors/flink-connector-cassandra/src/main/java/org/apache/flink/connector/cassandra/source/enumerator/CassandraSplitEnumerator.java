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

package org.apache.flink.connector.cassandra.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.cassandra.source.split.CassandraSplit;
import org.apache.flink.connector.cassandra.source.split.SplitsGenerator;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** {@link SplitEnumerator} that splits Cassandra cluster into {@link CassandraSplit}s. */
public final class CassandraSplitEnumerator
        implements SplitEnumerator<CassandraSplit, CassandraEnumeratorState> {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraSplitEnumerator.class);
    private static final String MURMUR3PARTITIONER = "org.apache.cassandra.dht.Murmur3Partitioner";

    private final SplitEnumeratorContext<CassandraSplit> enumeratorContext;
    private final CassandraEnumeratorState state;
    private final Cluster cluster;

    public CassandraSplitEnumerator(
            SplitEnumeratorContext<CassandraSplit> enumeratorContext,
            CassandraEnumeratorState state,
            ClusterBuilder clusterBuilder) {
        this.enumeratorContext = enumeratorContext;
        this.state = state == null ? new CassandraEnumeratorState() : state /* snapshot restore*/;
        this.cluster = clusterBuilder.getCluster();
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        assignUnprocessedSplitsToReader(subtaskId);
    }

    @Override
    public void start() {
        // discover the splits and update unprocessed splits and then assign them.
        // There is only an initial splits discovery, no periodic discovery.
        enumeratorContext.callAsync(
                this::discoverSplits,
                (splits, throwable) -> {
                    LOG.info("Add {} splits to CassandraSplitEnumerator.", splits.size());
                    state.addNewSplits(splits, enumeratorContext.currentParallelism());
                });
    }

    private List<CassandraSplit> discoverSplits() {
        final int numberOfSplits = enumeratorContext.currentParallelism();
        final Metadata clusterMetadata = cluster.getMetadata();
        final String partitioner = clusterMetadata.getPartitioner();
        final SplitsGenerator splitsGenerator = new SplitsGenerator(partitioner);
        if (MURMUR3PARTITIONER.equals(partitioner)) {
            LOG.info("Murmur3Partitioner detected, splitting");
            List<BigInteger> tokens =
                    clusterMetadata.getTokenRanges().stream()
                            .map(
                                    tokenRange ->
                                            new BigInteger(
                                                    tokenRange.getEnd().getValue().toString()))
                            .collect(Collectors.toList());
            return splitsGenerator.generateSplits(numberOfSplits, tokens);
        } else {
            // Murmur3Partitioner is the default and recommended partitioner for Cassandra 1.2+
            // see
            // https://docs.datastax.com/en/cassandra-oss/3.x/cassandra/architecture/archPartitionerAbout.html
            LOG.warn(
                    "The current Cassandra partitioner is {}, only Murmur3Partitioner is supported "
                            + "for splitting, using an single split",
                    partitioner);
            return splitsGenerator.generateSplits(1, Collections.emptyList());
        }
    }

    @Override
    public void addSplitsBack(List<CassandraSplit> splits, int subtaskId) {
        LOG.info("Add {} splits back to CassandraSplitEnumerator.", splits.size());
        state.addNewSplits(splits, enumeratorContext.currentParallelism());
        assignUnprocessedSplitsToReader(subtaskId);
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.info("Adding reader {} to CassandraSplitEnumerator.", subtaskId);
        assignUnprocessedSplitsToReader(subtaskId);
    }

    private void assignUnprocessedSplitsToReader(int readerId) {
        checkReaderRegistered(readerId);

        final Set<CassandraSplit> splitsForReader = state.getSplitsForReader(readerId);
        if (splitsForReader != null && !splitsForReader.isEmpty()) {
            Map<Integer, List<CassandraSplit>> assignment = new HashMap<>();
            assignment.put(readerId, Lists.newArrayList(splitsForReader));
            LOG.info("Assigning splits to reader {}", assignment);
            enumeratorContext.assignSplits(new SplitsAssignment<>(assignment));
        }

        // periodically partition discovery is disabled, signal NoMoreSplitsEvent to the reader
        LOG.debug(
                "No more CassandraSplits to assign. Sending NoMoreSplitsEvent to reader {}.",
                readerId);
        enumeratorContext.signalNoMoreSplits(readerId);
    }

    private void checkReaderRegistered(int readerId) {
        if (!enumeratorContext.registeredReaders().containsKey(readerId)) {
            throw new IllegalStateException(
                    String.format("Reader %d is not registered to source coordinator", readerId));
        }
    }

    @Override
    public CassandraEnumeratorState snapshotState(long checkpointId) throws Exception {
        return state;
    }

    @Override
    public void close() throws IOException {
        cluster.close();
    }
}
