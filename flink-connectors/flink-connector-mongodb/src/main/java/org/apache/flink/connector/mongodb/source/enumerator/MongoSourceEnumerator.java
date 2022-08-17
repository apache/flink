/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.enumerator.assigner.MongoSplitAssigner;
import org.apache.flink.connector.mongodb.source.split.MongoSourceSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;

/** The enumerator class for {@link MongoSource}. */
@Internal
public class MongoSourceEnumerator
        implements SplitEnumerator<MongoSourceSplit, MongoSourceEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoSourceEnumerator.class);

    private final Boundedness boundedness;
    private final SplitEnumeratorContext<MongoSourceSplit> context;
    private final MongoSplitAssigner splitAssigner;
    private final TreeSet<Integer> readersAwaitingSplit;

    public MongoSourceEnumerator(
            Boundedness boundedness,
            SplitEnumeratorContext<MongoSourceSplit> context,
            MongoSplitAssigner splitAssigner) {
        this.boundedness = boundedness;
        this.context = context;
        this.splitAssigner = splitAssigner;
        this.readersAwaitingSplit = new TreeSet<>();
    }

    @Override
    public void start() {
        splitAssigner.open();
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        readersAwaitingSplit.add(subtaskId);
        assignSplits();
    }

    @Override
    public void addSplitsBack(List<MongoSourceSplit> splits, int subtaskId) {
        LOG.debug("Mongo Source Enumerator adds splits back: {}", splits);
        splitAssigner.addSplitsBack(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug("Adding reader {} to MongoSourceEnumerator.", subtaskId);
    }

    private void assignSplits() {
        final Iterator<Integer> awaitingReader = readersAwaitingSplit.iterator();

        while (awaitingReader.hasNext()) {
            int nextAwaiting = awaitingReader.next();
            // if the reader that requested another split has failed in the meantime, remove
            // it from the list of waiting readers
            if (!context.registeredReaders().containsKey(nextAwaiting)) {
                awaitingReader.remove();
                continue;
            }

            Optional<MongoSourceSplit> split = splitAssigner.getNext();
            if (split.isPresent()) {
                final MongoSourceSplit mongoSplit = split.get();
                context.assignSplit(mongoSplit, nextAwaiting);
                awaitingReader.remove();
                LOG.info("Assign split {} to subtask {}", mongoSplit, nextAwaiting);
                break;
            } else if (splitAssigner.noMoreSplits() && boundedness == Boundedness.BOUNDED) {
                LOG.info("All splits have been assigned");
                context.registeredReaders().keySet().forEach(context::signalNoMoreSplits);
                break;
            } else {
                // there is no available splits by now, skip assigning
                break;
            }
        }
    }

    @Override
    public MongoSourceEnumState snapshotState(long checkpointId) {
        return splitAssigner.snapshotState(checkpointId);
    }

    @Override
    public void close() throws IOException {
        splitAssigner.close();
    }
}
