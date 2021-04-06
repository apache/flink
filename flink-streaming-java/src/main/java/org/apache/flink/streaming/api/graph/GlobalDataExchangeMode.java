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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;

/**
 * This mode decides the default {@link ResultPartitionType} of job edges. Note that this only
 * affects job edges which are {@link ShuffleMode#UNDEFINED}.
 */
public enum GlobalDataExchangeMode {
    /** Set all job edges to be {@link ResultPartitionType#BLOCKING}. */
    ALL_EDGES_BLOCKING,

    /**
     * Set job edges with {@link ForwardPartitioner} to be {@link
     * ResultPartitionType#PIPELINED_BOUNDED} and other edges to be {@link
     * ResultPartitionType#BLOCKING}.
     */
    FORWARD_EDGES_PIPELINED,

    /**
     * Set job edges with {@link ForwardPartitioner} or {@link RescalePartitioner} to be {@link
     * ResultPartitionType#PIPELINED_BOUNDED} and other edges to be {@link
     * ResultPartitionType#BLOCKING}.
     */
    POINTWISE_EDGES_PIPELINED,

    /** Set all job edges {@link ResultPartitionType#PIPELINED_BOUNDED}. */
    ALL_EDGES_PIPELINED,

    /** Set all job edges {@link ResultPartitionType#PIPELINED_APPROXIMATE}. */
    ALL_EDGES_PIPELINED_APPROXIMATE
}
