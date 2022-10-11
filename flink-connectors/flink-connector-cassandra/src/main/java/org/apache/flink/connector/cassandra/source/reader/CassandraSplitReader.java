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

package org.apache.flink.connector.cassandra.source.reader;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.cassandra.source.split.CassandraSplit;
import org.apache.flink.connector.cassandra.source.split.CassandraSplitState;
import org.apache.flink.connector.cassandra.source.split.RingRange;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * {@link SplitReader} for Cassandra source. This class is responsible for fetching the records as
 * {@link CassandraRow}. For that, it executes a range query (query that outputs records belonging
 * to a {@link RingRange}) based on the user specified query. This class manages the Cassandra
 * cluster and session.
 */
public class CassandraSplitReader implements SplitReader<CassandraRow, CassandraSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraSplitReader.class);
    public static final String SELECT_REGEXP = "(?i)select .+ from (\\w+)\\.(\\w+).*;$";

    private final Cluster cluster;
    private final Session session;
    private final Set<CassandraSplitState> unprocessedSplits;
    private final AtomicBoolean wakeup = new AtomicBoolean(false);
    private final String query;

    public CassandraSplitReader(ClusterBuilder clusterBuilder, String query) {
        // need a thread safe set
        this.unprocessedSplits = ConcurrentHashMap.newKeySet();
        this.query = query;
        cluster = clusterBuilder.getCluster();
        session = cluster.connect();
    }

    @Override
    public RecordsWithSplitIds<CassandraRow> fetch() {
        Map<String, Collection<CassandraRow>> recordsBySplit = new HashMap<>();
        Set<String> finishedSplits = new HashSet<>();
        Metadata clusterMetadata = cluster.getMetadata();

        String partitionKey = getPartitionKey(clusterMetadata);
        String finalQuery = generateRangeQuery(query, partitionKey);
        PreparedStatement preparedStatement = session.prepare(finalQuery);
        // Set wakeup to false to start consuming.
        wakeup.compareAndSet(true, false);
        for (CassandraSplitState cassandraSplitState : unprocessedSplits) {
            // allow to interrupt the reading of splits as requested in the API
            if (wakeup.get()) {
                break;
            }
            if (!cassandraSplitState.isEmpty()) {
                try {
                    final Set<RingRange> ringRanges =
                            cassandraSplitState.getUnprocessedRingRanges();
                    final String cassandraSplitId = cassandraSplitState.getSplitId();

                    for (RingRange ringRange : ringRanges) {
                        Token startToken =
                                clusterMetadata.newToken(ringRange.getStart().toString());
                        Token endToken = clusterMetadata.newToken(ringRange.getEnd().toString());
                        if (ringRange.isWrapping()) {
                            // A wrapping range is one that overlaps from the end of the partitioner
                            // range and its
                            // start (ie : when the start token of the split is greater than the end
                            // token)
                            // We need to generate two queries here : one that goes from the start
                            // token to the end
                            // of
                            // the partitioner range, and the other from the start of the
                            // partitioner range to the
                            // end token of the split.

                            addRecordsToOutput(
                                    session.execute(
                                            getLowestSplitQuery(
                                                    query, partitionKey, ringRange.getEnd())),
                                    recordsBySplit,
                                    cassandraSplitId,
                                    ringRange);
                            addRecordsToOutput(
                                    session.execute(
                                            getHighestSplitQuery(
                                                    query, partitionKey, ringRange.getStart())),
                                    recordsBySplit,
                                    cassandraSplitId,
                                    ringRange);
                        } else {
                            addRecordsToOutput(
                                    session.execute(
                                            preparedStatement
                                                    .bind()
                                                    .setToken(0, startToken)
                                                    .setToken(1, endToken)),
                                    recordsBySplit,
                                    cassandraSplitId,
                                    ringRange);
                        }
                        cassandraSplitState.markRingRangeAsFinished(ringRange);
                    }
                    // put the already read split to finished splits
                    finishedSplits.add(cassandraSplitState.getSplitId());
                    // for reentrant calls: if fetch is woken up,
                    // do not reprocess the already processed splits
                    unprocessedSplits.remove(cassandraSplitState);
                } catch (Exception ex) {
                    LOG.error("Error while reading split ", ex);
                }
            } else {
                finishedSplits.add(cassandraSplitState.getSplitId());
            }
        }
        return new RecordsBySplits<>(recordsBySplit, finishedSplits);
    }

    private String getPartitionKey(Metadata clusterMetadata) {
        Matcher queryMatcher = Pattern.compile(SELECT_REGEXP).matcher(query);
        if (!queryMatcher.matches()) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to extract keyspace and table out of the provided query: %s",
                            query));
        }
        String keyspace = queryMatcher.group(1);
        String table = queryMatcher.group(2);
        return clusterMetadata.getKeyspace(keyspace).getTable(table).getPartitionKey().stream()
                .map(ColumnMetadata::getName)
                .collect(Collectors.joining(","));
    }

    @Override
    public void wakeUp() {
        wakeup.compareAndSet(false, true);
    }

    @Override
    public void handleSplitsChanges(SplitsChange<CassandraSplit> splitsChanges) {
        for (CassandraSplit cassandraSplit : splitsChanges.splits()) {
            unprocessedSplits.add(
                    new CassandraSplitState(
                            cassandraSplit.getRingRanges(), cassandraSplit.splitId()));
        }
    }

    @VisibleForTesting
    static String getHighestSplitQuery(String query, String partitionKey, BigInteger highest) {
        return generateQuery(
                query, partitionKey, highest, " (token(%s) >= %d) AND", " WHERE (token(%s) >= %d)");
    }

    @VisibleForTesting
    static String getLowestSplitQuery(String query, String partitionKey, BigInteger lowest) {
        return generateQuery(
                query, partitionKey, lowest, " (token(%s) < %d) AND", " WHERE (token(%s) < %d)");
    }

    @VisibleForTesting
    static String generateRangeQuery(String query, String partitionKey) {
        return generateQuery(
                query,
                partitionKey,
                null,
                " (token(%s) >= ?) AND (token(%s) < ?) AND",
                " WHERE (token(%s) >= ?) AND (token(%s) < ?)");
    }

    private static String generateQuery(
            String query,
            String partitionKey,
            @Nullable BigInteger token,
            String whereFilter,
            String noWhereFilter) {
        Matcher queryMatcher = Pattern.compile(SELECT_REGEXP).matcher(query);
        if (!queryMatcher.matches()) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to extract keyspace and table out of the provided query: %s",
                            query));
        }
        final int whereIndex = query.toLowerCase().indexOf("where");
        int insertionPoint;
        String filter;
        if (whereIndex != -1) {
            insertionPoint = whereIndex + "where".length();
            filter =
                    (token == null)
                            ? String.format(whereFilter, partitionKey, partitionKey)
                            : String.format(whereFilter, partitionKey, token);
        } else {
            // end of keyspace.table
            insertionPoint = queryMatcher.end(2);
            filter =
                    (token == null)
                            ? String.format(noWhereFilter, partitionKey, partitionKey)
                            : String.format(noWhereFilter, partitionKey, token);
        }
        return String.format(
                "%s%s%s",
                query.substring(0, insertionPoint), filter, query.substring(insertionPoint));
    }

    /**
     * This method populates the {@code Map<String, Collection<CassandraRow>> recordsBySplit} map
     * that is used to create the {@link RecordsBySplits} that are output by the fetch method. It
     * modifies its {@code output} parameter.
     */
    private void addRecordsToOutput(
            ResultSet resultSet,
            Map<String, Collection<CassandraRow>> output,
            String cassandraSplitId,
            RingRange ringRange) {
        resultSet.forEach(
                row ->
                        output.computeIfAbsent(cassandraSplitId, id -> new ArrayList<>())
                                .add(
                                        new CassandraRow(
                                                row, ringRange, resultSet.getExecutionInfo())));
    }

    @Override
    public void close() throws Exception {
        try {
            if (session != null) {
                session.close();
            }
        } catch (Exception e) {
            LOG.error("Error while closing session.", e);
        }
        try {
            if (cluster != null) {
                cluster.close();
            }
        } catch (Exception e) {
            LOG.error("Error while closing cluster.", e);
        }
    }
}
