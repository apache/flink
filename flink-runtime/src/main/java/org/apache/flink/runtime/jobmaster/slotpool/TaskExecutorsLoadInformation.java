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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;

import java.util.HashMap;
import java.util.Map;

/**
 * The interface defines the methods to get the resource utilization or loading of task executors.
 */
public interface TaskExecutorsLoadInformation {

    TaskExecutorsLoadInformation EMPTY =
            new TaskExecutorsLoadInformation() {
                @Override
                public Map<ResourceID, LoadingWeight> getTaskExecutorsLoadingWeight() {
                    return new HashMap<>();
                }
            };

    /**
     * Return the loading weight for per task executor.
     *
     * @return map of loading weight for per task executor.
     */
    Map<ResourceID, LoadingWeight> getTaskExecutorsLoadingWeight();
}
