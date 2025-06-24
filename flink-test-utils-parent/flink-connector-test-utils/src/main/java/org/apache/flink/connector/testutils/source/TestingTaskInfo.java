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

package org.apache.flink.connector.testutils.source;

import org.apache.flink.api.common.TaskInfo;

/** Test implementation for {@link TaskInfo}. */
public class TestingTaskInfo implements TaskInfo {

    private final String taskName;

    private final int maxNumberOfParallelSubtasks;

    private final int indexOfThisSubtask;

    private final int numberOfParallelSubtasks;

    private final int attemptNumber;

    private final String taskNameWithSubtasks;

    private final String allocationIDAsString;

    public TestingTaskInfo(
            String taskName,
            int maxNumberOfParallelSubtasks,
            int indexOfThisSubtask,
            int numberOfParallelSubtasks,
            int attemptNumber,
            String taskNameWithSubtasks,
            String allocationIDAsString) {
        this.taskName = taskName;
        this.maxNumberOfParallelSubtasks = maxNumberOfParallelSubtasks;
        this.indexOfThisSubtask = indexOfThisSubtask;
        this.numberOfParallelSubtasks = numberOfParallelSubtasks;
        this.attemptNumber = attemptNumber;
        this.taskNameWithSubtasks = taskNameWithSubtasks;
        this.allocationIDAsString = allocationIDAsString;
    }

    @Override
    public String getTaskName() {
        return taskName;
    }

    @Override
    public int getMaxNumberOfParallelSubtasks() {
        return maxNumberOfParallelSubtasks;
    }

    @Override
    public int getIndexOfThisSubtask() {
        return indexOfThisSubtask;
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return numberOfParallelSubtasks;
    }

    @Override
    public int getAttemptNumber() {
        return attemptNumber;
    }

    @Override
    public String getTaskNameWithSubtasks() {
        return taskNameWithSubtasks;
    }

    @Override
    public String getAllocationIDAsString() {
        return allocationIDAsString;
    }

    /** Builder for {@link TestingTaskInfo}. */
    public static class Builder {

        private String taskName = "";

        private int maxNumberOfParallelSubtasks = 0;

        private int indexOfThisSubtask = -1;

        private int numberOfParallelSubtasks = 0;

        private int attemptNumber = 0;

        private String taskNameWithSubtasks = "";

        private String allocationIDAsString = "";

        public Builder() {}

        public Builder setTaskName(String taskName) {
            this.taskName = taskName;
            return this;
        }

        public Builder setMaxNumberOfParallelSubtasks(int maxNumberOfParallelSubtasks) {
            this.maxNumberOfParallelSubtasks = maxNumberOfParallelSubtasks;
            return this;
        }

        public Builder setIndexOfThisSubtask(int indexOfThisSubtask) {
            this.indexOfThisSubtask = indexOfThisSubtask;
            return this;
        }

        public Builder setNumberOfParallelSubtasks(int numberOfParallelSubtasks) {
            this.numberOfParallelSubtasks = numberOfParallelSubtasks;
            return this;
        }

        public Builder setAttemptNumber(int attemptNumber) {
            this.attemptNumber = attemptNumber;
            return this;
        }

        public Builder setTaskNameWithSubtasks(String taskNameWithSubtasks) {
            this.taskNameWithSubtasks = taskNameWithSubtasks;
            return this;
        }

        public Builder setAllocationIDAsString(String allocationIDAsString) {
            this.allocationIDAsString = allocationIDAsString;
            return this;
        }

        public TaskInfo build() {
            return new TestingTaskInfo(
                    taskName,
                    maxNumberOfParallelSubtasks,
                    indexOfThisSubtask,
                    numberOfParallelSubtasks,
                    attemptNumber,
                    taskNameWithSubtasks,
                    allocationIDAsString);
        }
    }
}
