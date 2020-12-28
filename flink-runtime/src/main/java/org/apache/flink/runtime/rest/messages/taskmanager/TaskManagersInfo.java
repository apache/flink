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

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.Objects;

/** Class containing a collection of {@link TaskManagerInfo}. */
public class TaskManagersInfo implements ResponseBody {

    public static final String FIELD_NAME_TASK_MANAGERS = "taskmanagers";

    @JsonProperty(FIELD_NAME_TASK_MANAGERS)
    private final Collection<TaskManagerInfo> taskManagerInfos;

    @JsonCreator
    public TaskManagersInfo(
            @JsonProperty(FIELD_NAME_TASK_MANAGERS) Collection<TaskManagerInfo> taskManagerInfos) {
        this.taskManagerInfos = Preconditions.checkNotNull(taskManagerInfos);
    }

    public Collection<TaskManagerInfo> getTaskManagerInfos() {
        return taskManagerInfos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskManagersInfo that = (TaskManagersInfo) o;
        return Objects.equals(taskManagerInfos, that.taskManagerInfos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskManagerInfos);
    }
}
