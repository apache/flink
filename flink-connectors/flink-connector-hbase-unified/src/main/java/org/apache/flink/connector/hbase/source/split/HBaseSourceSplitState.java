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

package org.apache.flink.connector.hbase.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.hbase.HBaseEvent;
import org.apache.flink.connector.hbase.source.reader.HBaseSourceEvent;

/**
 * Contains the state of an {@link HBaseSourceSplit}. It tracks the timestamp of the last emitted
 * event to ensure no duplicates appear on recovery.
 */
@Internal
public class HBaseSourceSplitState {
    private final HBaseSourceSplit split;

    private long lastTimeStamp = -1;
    private int lastIndex = -1;

    public HBaseSourceSplitState(HBaseSourceSplit split) {
        this.split = split;
        this.lastTimeStamp = this.split.getFirstEventStamp().f0;
        this.lastIndex = this.split.getFirstEventStamp().f1;
    }

    public HBaseSourceSplit toSplit() {
        return split.withStamp(lastTimeStamp, lastIndex);
    }

    /**
     * Update the state of which element was emitted last.
     *
     * @param event An {@link HBaseEvent} that has been emitted by the {@link
     *     org.apache.flink.connector.hbase.source.reader.HBaseRecordEmitter}
     */
    public void notifyEmittedEvent(HBaseSourceEvent event) {
        lastTimeStamp = event.getTimestamp();
        lastIndex = event.getIndex();
    }

    public boolean isAlreadyProcessedEvent(HBaseSourceEvent event) {
        return !event.isLaterThan(lastTimeStamp, lastIndex);
    }
}
