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

package org.apache.flink.streaming.connectors.gcp.pubsub.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.streaming.connectors.gcp.pubsub.source.split.PubSubSplitState;

/**
 * A custom {@link RecordEmitter} to emit a record which includes the data of the received GCP
 * Pub/Sub message and the publication time of the message.
 *
 * @param <T> The type of the record data to be emitted.
 */
public class PubSubRecordEmitter<T> implements RecordEmitter<Tuple2<T, Long>, T, PubSubSplitState> {

    @Override
    public void emitRecord(
            Tuple2<T, Long> element, SourceOutput<T> output, PubSubSplitState splitState)
            throws Exception {
        output.collect(element.f0, element.f1);
    }
}
