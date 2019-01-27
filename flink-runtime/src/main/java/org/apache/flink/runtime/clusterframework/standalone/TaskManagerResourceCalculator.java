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

package org.apache.flink.runtime.clusterframework.standalone;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.TaskManagerResource;
import org.apache.flink.runtime.taskexecutor.TaskManagerServicesConfiguration;

import java.util.HashMap;
import java.util.Map;

public class TaskManagerResourceCalculator {

  // --------------------------------------------------------------------------------------------
  //  Static entry point
  // --------------------------------------------------------------------------------------------

  public static void main(String[] args) {

    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    final String configDir = parameterTool.get("configDir");

    final Configuration configuration = GlobalConfiguration.loadConfiguration(configDir);

    System.out.println(getTaskManagerResourceFromConf(configuration));
  }

  // Utility methods

  /**
   * In Standalone mode, taskmanager.sh use this method to get JVM_ARGS.
   * @param configuration flink configuration
   * @return All jvm args
   */
  @VisibleForTesting
  public static String getTaskManagerResourceFromConf(Configuration configuration) {
    // Set min of network buffer equals to max to make sure the task manager has sufficient direct memory.
    configuration.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN,
      configuration.getLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX));
    TaskManagerResource taskManagerResource =
        TaskManagerResource.fromConfiguration(configuration, initContainerResourceConfig(configuration), 1);
    return String.format("TotalHeapMemory:%s,YoungHeapMemory:%s,TotalDirectMemory:%s",
        taskManagerResource.getTotalHeapMemory(), taskManagerResource.getYoungHeapMemory(),
        taskManagerResource.getTotalDirectMemory());
  }

  public static int getTotalJavaMemorySizeMB(Configuration configuration) {
    TaskManagerResource taskManagerResource =
        TaskManagerResource.fromConfiguration(configuration, initContainerResourceConfig(configuration), 1);
    return taskManagerResource.getTotalHeapMemory() + taskManagerResource.getTotalDirectMemory();
  }

  /**
   * Calculates the amount of memory used for network buffers to be allocated for TaskManager.
   * @return memory to use for network buffers (in bytes)
   */
  public static long calculateNetworkBufferMemory(Configuration flinkConfig) {
    long networkBufBytes;
    if (TaskManagerServicesConfiguration.hasNewNetworkBufConf(flinkConfig)) {
      // get network buffer based on min, and max should be set same as min when start task manager
      networkBufBytes = flinkConfig.getLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN);
    } else {
      // use old (deprecated) network buffers parameter
      int networkBuffersNum = flinkConfig.getInteger(TaskManagerOptions.NETWORK_NUM_BUFFERS);
      long pageSize = flinkConfig.getInteger(TaskManagerOptions.MEMORY_SEGMENT_SIZE);
      networkBufBytes = pageSize * networkBuffersNum;
    }
    return networkBufBytes;
  }

  public static ResourceProfile initContainerResourceConfig(Configuration flinkConfig) {
    double core = flinkConfig.getDouble(TaskManagerOptions.TASK_MANAGER_CORE);
    int heapMemory = flinkConfig.getInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY);
    int nativeMemory = flinkConfig.getInteger(TaskManagerOptions.TASK_MANAGER_NATIVE_MEMORY);
    int directMemory = flinkConfig.getInteger(TaskManagerOptions.TASK_MANAGER_DIRECT_MEMORY);
    int networkMemory = (int) Math.ceil(calculateNetworkBufferMemory(flinkConfig) / (1024.0 * 1024.0));

    // Add managed memory to extended resources.
    long managedMemory = flinkConfig.getLong(TaskManagerOptions.MANAGED_MEMORY_SIZE);
    Map<String, Resource> resourceMap = new HashMap<>();
    resourceMap.put(ResourceSpec.MANAGED_MEMORY_NAME,
        new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, Math.max(0, managedMemory)));
    long floatingManagedMemory = flinkConfig.getLong(TaskManagerOptions.FLOATING_MANAGED_MEMORY_SIZE);
    resourceMap.put(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME,
        new CommonExtendedResource(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME, Math.max(0, floatingManagedMemory)));
    return new ResourceProfile(
        core,
        heapMemory,
        directMemory,
        nativeMemory,
        networkMemory,
        resourceMap);
  }
}
