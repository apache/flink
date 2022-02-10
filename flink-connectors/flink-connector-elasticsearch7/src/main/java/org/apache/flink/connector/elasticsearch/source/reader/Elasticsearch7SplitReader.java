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

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.elasticsearch.common.NetworkClientConfig;
import org.apache.flink.connector.elasticsearch.source.Elasticsearch7SourceConfiguration;
import org.apache.flink.connector.elasticsearch.source.split.Elasticsearch7Split;
import org.apache.flink.util.Collector;

import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/**
 * A {@link SplitReader} implementation that reads records from {@link Elasticsearch7Split}s.
 *
 * @param <T> the type of the record to be emitted from the Source.
 */
public class Elasticsearch7SplitReader<T> implements SplitReader<T, Elasticsearch7Split> {
    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch7SplitReader.class);
    private final Elasticsearch7SourceConfiguration sourceConfiguration;
    private final NetworkClientConfig networkClientConfig;
    private final Queue<Elasticsearch7Split> splits;

    @Nullable private Elasticsearch7SearchHitReader currentReader;
    @Nullable private String currentSplitId;

    private final Elasticsearch7SearchHitDeserializationSchema<T> deserializationSchema;
    private final SimpleCollector<T> collector;

    public Elasticsearch7SplitReader(
            Elasticsearch7SourceConfiguration sourceConfiguration,
            NetworkClientConfig networkClientConfig,
            Elasticsearch7SearchHitDeserializationSchema<T> deserializationSchema) {
        this.sourceConfiguration = sourceConfiguration;
        this.networkClientConfig = networkClientConfig;
        this.deserializationSchema = deserializationSchema;
        this.splits = new ArrayDeque<>();
        this.collector = new SimpleCollector<>();
    }

    private RecordsWithSplitIds<T> createRecordsFromSearchResults(Collection<SearchHit> searchHits)
            throws IOException {
        Collection<T> recordsForSplit = new ArrayList<>();
        for (SearchHit hit : searchHits) {
            try {
                deserializationSchema.deserialize(hit, collector);
                recordsForSplit.addAll(collector.getRecords());
            } catch (Exception e) {
                throw new IOException("Failed to deserialize consumer record due to", e);
            } finally {
                collector.reset();
            }
        }

        return ElasticsearchSplitRecords.forRecords(currentSplitId, recordsForSplit);
    }

    @Override
    public RecordsWithSplitIds<T> fetch() throws IOException {
        checkSplitOrStartNext();

        final Collection<SearchHit> nextSearchHits = currentReader.readNextSearchHits();
        return nextSearchHits == null
                ? finishSplit()
                : createRecordsFromSearchResults(nextSearchHits);
    }

    @Override
    public void handleSplitsChanges(SplitsChange<Elasticsearch7Split> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        LOG.debug("Handling split change {}", splitsChanges);
        splits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        if (currentReader != null) {
            currentReader.close();
        }
    }

    private void checkSplitOrStartNext() throws IOException {
        if (currentReader != null) {
            return;
        }

        final Elasticsearch7Split nextSplit = splits.poll();
        if (nextSplit == null) {
            throw new IOException("Cannot fetch another split - no splits remaining.");
        }

        currentSplitId = nextSplit.splitId();
        currentReader =
                new Elasticsearch7SearchHitReader(
                        sourceConfiguration, networkClientConfig, nextSplit);
    }

    private ElasticsearchSplitRecords<T> finishSplit() throws IOException {
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
        }

        final ElasticsearchSplitRecords<T> finishedRecords =
                ElasticsearchSplitRecords.finishedSplit(currentSplitId);
        currentSplitId = null;
        return finishedRecords;
    }

    private static class ElasticsearchSplitRecords<T> implements RecordsWithSplitIds<T> {

        @Nullable private String splitId;

        @Nullable private Iterator<T> recordsForSplitCurrent;

        @Nullable private final Iterator<T> recordsForSplit;

        private final Set<String> finishedSplits;

        private ElasticsearchSplitRecords(
                @Nullable String splitId, Iterator<T> recordsForSplit, Set<String> finishedSplits) {
            this.splitId = splitId;
            this.recordsForSplit = recordsForSplit;
            this.finishedSplits = finishedSplits;
        }

        @Nullable
        @Override
        public String nextSplit() {
            // move the split one (from current value to null)
            final String nextSplit = this.splitId;
            this.splitId = null;

            // move the iterator, from null to value (if first move) or to null (if second move)
            this.recordsForSplitCurrent = (nextSplit != null) ? this.recordsForSplit : null;

            return nextSplit;
        }

        @Nullable
        @Override
        public T nextRecordFromSplit() {
            if (recordsForSplitCurrent != null) {
                if (recordsForSplitCurrent.hasNext()) {
                    return recordsForSplitCurrent.next();
                } else {
                    return null;
                }
            } else {
                throw new IllegalStateException();
            }
        }

        @Override
        public Set<String> finishedSplits() {
            return finishedSplits;
        }

        public static <T> ElasticsearchSplitRecords<T> forRecords(
                String splitId, Collection<T> recordsForSplit) {
            return new ElasticsearchSplitRecords<>(
                    splitId, recordsForSplit.iterator(), Collections.emptySet());
        }

        public static <T> ElasticsearchSplitRecords<T> finishedSplit(String splitId) {
            return new ElasticsearchSplitRecords<>(null, null, Collections.singleton(splitId));
        }
    }

    private static class SimpleCollector<T> implements Collector<T> {
        private final List<T> records = new ArrayList<>();

        @Override
        public void collect(T record) {
            records.add(record);
        }

        @Override
        public void close() {}

        private List<T> getRecords() {
            return records;
        }

        private void reset() {
            records.clear();
        }
    }
}
