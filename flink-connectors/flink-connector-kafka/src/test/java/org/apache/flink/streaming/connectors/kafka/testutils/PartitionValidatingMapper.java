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

package org.apache.flink.streaming.connectors.kafka.testutils;

import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashSet;
import java.util.Set;

/** {@link MapFunction} that verifies that he partitioning is identical. */
public class PartitionValidatingMapper implements MapFunction<Integer, Integer> {

    private static final long serialVersionUID = 1088381231244959088L;

    /* the partitions from which this function received data */
    private final Set<Integer> myPartitions = new HashSet<>();

    private final int numPartitions;
    private final int maxPartitions;

    public PartitionValidatingMapper(int numPartitions, int maxPartitions) {
        this.numPartitions = numPartitions;
        this.maxPartitions = maxPartitions;
    }

    @Override
    public Integer map(Integer value) throws Exception {
        // validate that the partitioning is identical
        int partition = value % numPartitions;
        myPartitions.add(partition);
        if (myPartitions.size() > maxPartitions) {
            throw new Exception(
                    "Error: Elements from too many different partitions: "
                            + myPartitions
                            + ". Expect elements only from "
                            + maxPartitions
                            + " partitions");
        }
        return value;
    }
}
