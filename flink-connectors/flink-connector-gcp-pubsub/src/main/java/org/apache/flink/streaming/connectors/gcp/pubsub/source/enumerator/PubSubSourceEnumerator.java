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

package org.apache.flink.streaming.connectors.gcp.pubsub.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.streaming.connectors.gcp.pubsub.source.split.PubSubSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * The enumerator for the {@link
 * org.apache.flink.streaming.connectors.gcp.pubsub.source.PubSubSource}. It does not do any work
 * discovery as envisioned by FLIP-27 because GCP Pub/Sub hides partitions and other implementation
 * details.
 */
public class PubSubSourceEnumerator implements SplitEnumerator<PubSubSplit, PubSubEnumeratorState> {
    private final SplitEnumeratorContext<PubSubSplit> context;

    public PubSubSourceEnumerator(SplitEnumeratorContext<PubSubSplit> context) {
        this.context = context;
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

    @Override
    public void addSplitsBack(List<PubSubSplit> splits, int subtaskId) {}

    /**
     * When a new reader joins, the enumerator actively assigns it with a generic {@link
     * PubSubSplit} so that it can start pulling messages.
     *
     * @param subtaskId the subtask ID of the new source reader.
     */
    @Override
    public void addReader(int subtaskId) {
        context.assignSplit(new PubSubSplit(), subtaskId);
    }

    /**
     * In the case of GCP Pub/Sub, there's no state of the enumerator that could be captured in a
     * checkpoint.
     *
     * @return A checkpoint stub.
     * @throws Exception
     */
    @Override
    public PubSubEnumeratorState snapshotState() throws Exception {
        return new PubSubEnumeratorState();
    }

    @Override
    public void close() throws IOException {}
}
