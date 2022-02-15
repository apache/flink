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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.elasticsearch.common.NetworkClientConfig;
import org.apache.flink.connector.elasticsearch.source.Elasticsearch7SourceConfiguration;
import org.apache.flink.connector.elasticsearch.source.split.Elasticsearch7Split;

import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

/**
 * A {@link SplitReader} implementation that reads {@link SearchHit}s from {@link
 * Elasticsearch7Split}s.
 */
@Internal
public class Elasticsearch7SplitReader implements SplitReader<SearchHit, Elasticsearch7Split> {
    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch7SplitReader.class);
    private final Elasticsearch7SourceConfiguration sourceConfiguration;
    private final NetworkClientConfig networkClientConfig;
    private final Queue<Elasticsearch7Split> splits;

    @Nullable private Elasticsearch7SearchHitReader currentReader;
    @Nullable private String currentSplitId;

    public Elasticsearch7SplitReader(
            Elasticsearch7SourceConfiguration sourceConfiguration,
            NetworkClientConfig networkClientConfig) {
        this.sourceConfiguration = sourceConfiguration;
        this.networkClientConfig = networkClientConfig;
        this.splits = new ArrayDeque<>();
    }

    @Override
    public RecordsWithSplitIds<SearchHit> fetch() throws IOException {
        checkSplitOrStartNext();

        final Collection<SearchHit> nextSearchHits = currentReader.readNextSearchHits();
        return nextSearchHits == null
                ? finishSplit()
                : ElasticsearchSplitRecords.forRecords(currentSplitId, nextSearchHits);
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

    private ElasticsearchSplitRecords finishSplit() throws IOException {
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
        }

        final ElasticsearchSplitRecords finishedRecords =
                ElasticsearchSplitRecords.finishedSplit(currentSplitId);
        currentSplitId = null;
        return finishedRecords;
    }

    private static class ElasticsearchSplitRecords implements RecordsWithSplitIds<SearchHit> {

        @Nullable private String splitId;

        @Nullable private Iterator<SearchHit> recordsForSplitCurrent;

        @Nullable private final Iterator<SearchHit> recordsForSplit;

        private final Set<String> finishedSplits;

        private ElasticsearchSplitRecords(
                @Nullable String splitId,
                Iterator<SearchHit> recordsForSplit,
                Set<String> finishedSplits) {
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
        public SearchHit nextRecordFromSplit() {
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

        public static ElasticsearchSplitRecords forRecords(
                String splitId, Collection<SearchHit> recordsForSplit) {
            return new ElasticsearchSplitRecords(
                    splitId, recordsForSplit.iterator(), Collections.emptySet());
        }

        public static ElasticsearchSplitRecords finishedSplit(String splitId) {
            return new ElasticsearchSplitRecords(null, null, Collections.singleton(splitId));
        }
    }
}
