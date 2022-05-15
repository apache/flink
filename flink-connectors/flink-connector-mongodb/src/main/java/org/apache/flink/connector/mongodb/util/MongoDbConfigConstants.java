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

package org.apache.flink.connector.mongodb.util;

import org.apache.flink.annotation.PublicEvolving;

/** config constants for a MongoDB client. */
@PublicEvolving
public class MongoDbConfigConstants {

    /** A read concern allows clients to choose a level of isolation for their reads. */
    public static final String READ_CONCERN = "read_concern";

    /** The read preference to use for queries, map-reduce, aggregation, and count. */
    public static final String READ_PREFERENCE = "read_preference";

    /**
     * true if reads should be retried if they fail due to a network error or other retryable error.
     * The default value is true.
     */
    public static final String RETRYABLE_READS = "retryable_reads";

    /**
     * true if writes should be retried if they fail due to a network error or other retryable
     * error. The default value is true.
     */
    public static final String RETRYABLE_WRITES = "retryable_writes";

    /** Controls the acknowledgment of write operations with various options. */
    public static final String WRITE_CONCERN = "write_concern";

    public static final String NETWORK_COMPRESSION = "network_compression";

    /** Uses the settings from a ConnectionString object. */
    public static final String APPLY_CONNECTION_STRING = "apply_connection_string";

    /** Sets the maximum time to select a primary node before throwing a timeout exception. */
    public static final String SERVER_SELECTION_TIMEOUT =
            "cluster_settings.server_selection_timeout";

    /** Sets the maximum amount of connections associated with a connection pool. */
    public static final String MAX_SIZE = "connection_pool.max.size";

    /** Sets the minimum amount of connections associated with a connection pool. */
    public static final String MIN_SIZE = "connection_pool.min.size";

    /** Sets the maximum time to wait for an available connection. */
    public static final String MAX_WAIT_TIME = "connection_pool.max.wait_time";

    /**
     * If true, then when a write fails, return without performing the remaining writes. If false,
     * then when a write fails, continue with the remaining writes, if any. Defaults to true.
     */
    public static final String BULK_WRITE_OPTIONS_ORDERED = "bulk_write_options.ordered";

    /** If true, allows the write to opt-out of document level validation. Defaults to false. */
    public static final String BULK_WRITE_OPTIONS_BYPASS_DOCUMENT_VALIDATION =
            "bulk_write_options.bypass_document_validation";
}
