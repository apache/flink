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

package org.apache.flink.connector.mongodb.source.enumerator.assigner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.mongodb.source.enumerator.MongoSourceEnumState;
import org.apache.flink.connector.mongodb.source.split.MongoSourceSplit;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

/** The split assigner for {@link MongoSourceSplit}. */
@Internal
public interface MongoSplitAssigner extends Serializable {

    /**
     * Called to open the assigner to acquire any resources, like threads or network connections.
     */
    void open();

    /**
     * Called to close the assigner, in case it holds on to any resources, like threads or network
     * connections.
     */
    void close() throws IOException;

    /** Gets the next split. */
    Optional<MongoSourceSplit> getNext();

    /**
     * Adds a set of splits to this assigner. This happens for example when some split processing
     * failed and the splits need to be re-added.
     */
    void addSplitsBack(Collection<MongoSourceSplit> splits);

    /** Snapshot the current assign state into checkpoint. */
    MongoSourceEnumState snapshotState(long checkpointId);

    /** Return if there are no more splits. */
    boolean noMoreSplits();
}
