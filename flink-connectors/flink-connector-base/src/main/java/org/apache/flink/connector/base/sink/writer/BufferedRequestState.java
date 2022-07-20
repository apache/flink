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

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

/**
 * Class holding state of {@link AsyncSinkWriter} needed at taking a snapshot. The state captures
 * the {@code bufferedRequestEntries} buffer for the writer at snapshot to resume the requests. This
 * guarantees at least once semantic in sending requests where restoring from a snapshot where
 * buffered requests were flushed to the sink will cause duplicate requests.
 *
 * @param <RequestEntryT> request type.
 */
@PublicEvolving
public class BufferedRequestState<RequestEntryT extends Serializable> implements Serializable {
    private final List<RequestEntryWrapper<RequestEntryT>> bufferedRequestEntries;
    private final long stateSize;

    public BufferedRequestState(Deque<RequestEntryWrapper<RequestEntryT>> bufferedRequestEntries) {
        this.bufferedRequestEntries = new ArrayList<>(bufferedRequestEntries);
        this.stateSize = calculateStateSize();
    }

    public BufferedRequestState(List<RequestEntryWrapper<RequestEntryT>> bufferedRequestEntries) {
        this.bufferedRequestEntries = new ArrayList<>(bufferedRequestEntries);
        this.stateSize = calculateStateSize();
    }

    public List<RequestEntryWrapper<RequestEntryT>> getBufferedRequestEntries() {
        return bufferedRequestEntries;
    }

    public long getStateSize() {
        return stateSize;
    }

    private long calculateStateSize() {
        long stateSize = 0;
        for (RequestEntryWrapper<RequestEntryT> requestEntryWrapper : bufferedRequestEntries) {
            stateSize += requestEntryWrapper.getSize();
        }

        return stateSize;
    }

    public static <T extends Serializable> BufferedRequestState<T> emptyState() {
        return new BufferedRequestState<>(Collections.emptyList());
    }
}
