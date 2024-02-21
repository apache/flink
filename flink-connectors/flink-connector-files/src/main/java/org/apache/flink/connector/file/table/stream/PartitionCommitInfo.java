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

package org.apache.flink.connector.file.table.stream;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/**
 * The message sent by upstream.
 *
 * <p>Need to ensure that the partitions are ready to commit. That is to say, the files in the
 * partition have become readable rather than temporary.
 */
@Internal
public class PartitionCommitInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private long checkpointId;
    private int taskId;
    private int numberOfTasks;
    private String[] partitions;

    public PartitionCommitInfo() {}

    public PartitionCommitInfo(
            long checkpointId, int taskId, int numberOfTasks, String[] partitions) {
        this.checkpointId = checkpointId;
        this.taskId = taskId;
        this.numberOfTasks = numberOfTasks;
        this.partitions = partitions;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public void setCheckpointId(long checkpointId) {
        this.checkpointId = checkpointId;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public int getNumberOfTasks() {
        return numberOfTasks;
    }

    public void setNumberOfTasks(int numberOfTasks) {
        this.numberOfTasks = numberOfTasks;
    }

    public String[] getPartitions() {
        return partitions;
    }

    public void setPartitions(String[] partitions) {
        this.partitions = partitions;
    }
}
