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

package org.apache.flink.connector.mongodb.common;

import org.apache.flink.annotation.PublicEvolving;

/** Configuration keys for Mongodb service usage. */
@PublicEvolving
public class MongodbConfigConstants {

    /**
     * The readConcern option allows you to control the consistency and isolation properties of the
     * data read from replica sets and replica set shards.
     *
     * <p>Through the effective use of writeconcerns and read concerns, you can adjust the level of
     * consistency and availability guarantees as appropriate, such as waiting for stronger
     * consistency guarantees, or loosening consistency requirements to provide higher availability.
     */
    public static final String READ_CONCERN = "read.concern";

    /**
     * Read preference describes how MongoDB clients route read operations to the members of a
     * replica set.
     */
    public static final String READ_PREFERENCE = "read.preference";

    /**
     * Retryable reads allow MongoDB drivers to automatically retry certain read operations a single
     * time if they encounter certain network or server errors.
     */
    public static final String RETRYABLE_READS = "retryable.reads";

    /**
     * Retryable writes allow MongoDB drivers to automatically retry certain write operations a
     * single time if they encounter network errors, or if they cannot find a healthy primary in the
     * replica sets or sharded cluster.
     */
    public static final String RETRYABLE_WRITES = "retryable_writes";

    /**
     * Write concern describes the level of acknowledgment requested from MongoDB for write
     * operations to a standalone mongod or to replica sets or to sharded clusters. In sharded
     * clusters, mongos instances will pass the write concern on to the shards.
     */
    public static final String WRITE_CONCERN = "write_concern";

    /**
     * You can enable a driver option to compress messages which reduces the amount of data passed
     * over the network between MongoDB and your application.
     *
     * <p>The driver supports the following algorithms:
     *
     * <p>Snappy: available in MongoDB 3.4 and later. Zlib: available in MongoDB 3.6 and later.
     * Zstandard: available in MongoDB 4.2 and later.
     */
    public static final String NETWORK_COMPRESSION = "network.compression";

    /** Uses the settings from a ConnectionString object. */
    public static final String APPLY_CONNECTION_STRING = "apply.connection.string";

    /** applyToClusterSettings() */
    /** Sets the maximum time to select a primary node before throwing a timeout exception. */
    public static final String SERVER_SELECTION_TIMEOUT = "server.selection.timeout";

    /** applyToConnectionPoolSettings() */
    /** Sets the maximum amount of connections associated with a connection pool. */
    public static final String MAX_SIZE = "max.size";

    /** Sets the minimum amount of connections associated with a connection pool. */
    public static final String MIN_SIZE = "min.size";

    /** Sets the maximum time to wait for an available connection. */
    public static final String MAX_WAIT_TIME = "max.wait.time";
}
