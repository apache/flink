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

package org.apache.flink.connector.testframe.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.connector.testframe.source.split.FromElementsSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/** Mock enumerator. */
public class NoOpEnumerator implements SplitEnumerator<FromElementsSplit, NoOpEnumState> {
    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

    @Override
    public void addSplitsBack(List<FromElementsSplit> splits, int subtaskId) {}

    @Override
    public void addReader(int subtaskId) {}

    @Override
    public NoOpEnumState snapshotState(long checkpointId) throws Exception {
        return new NoOpEnumState();
    }

    @Override
    public void close() throws IOException {}
}
