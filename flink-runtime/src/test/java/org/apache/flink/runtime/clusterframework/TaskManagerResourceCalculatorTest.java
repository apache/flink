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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.standalone.TaskManagerResourceCalculator;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskManagerResourceCalculatorTest {

  public static int TM_HEAP_MEMORY_MB = 2048;
  public static int TM_PROCESS_NETTY_MEMORY_MB = 256;
  public static int TM_MANAGED_MEMORY_MB = 1024;
  public static int TM_FLOATING_MEMORY_MB = 1024;

  @Test
  public void testTaskManagerResourceCalculator(){
    Configuration flinkConfig = new Configuration();
    flinkConfig.setInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY, TM_HEAP_MEMORY_MB);
    flinkConfig.setInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NETTY_MEMORY, TM_PROCESS_NETTY_MEMORY_MB);
    flinkConfig.setBoolean(TaskManagerOptions.MEMORY_OFF_HEAP, false);
    flinkConfig.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, TM_MANAGED_MEMORY_MB);
    flinkConfig.setLong(TaskManagerOptions.FLOATING_MANAGED_MEMORY_SIZE, TM_FLOATING_MEMORY_MB);

    int totalHeapSizeMB = TM_HEAP_MEMORY_MB + TaskManagerOptions.TASK_MANAGER_PROCESS_HEAP_MEMORY.defaultValue()
      + TM_MANAGED_MEMORY_MB + TM_FLOATING_MEMORY_MB;
    String taskManagerResource = String.format("TotalHeapMemory:%s,YoungHeapMemory:%s,TotalDirectMemory:%s",
      totalHeapSizeMB, (int) (totalHeapSizeMB * TaskManagerOptions.TASK_MANAGER_MEMORY_DYNAMIC_YOUNG_RATIO.defaultValue()),
      TM_PROCESS_NETTY_MEMORY_MB + (TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX.defaultValue() >> 20));
    Assert.assertEquals(taskManagerResource, TaskManagerResourceCalculator.getTaskManagerResourceFromConf(flinkConfig));

    totalHeapSizeMB = TM_HEAP_MEMORY_MB + TaskManagerOptions.TASK_MANAGER_PROCESS_HEAP_MEMORY.defaultValue();
    flinkConfig.setBoolean(TaskManagerOptions.MEMORY_OFF_HEAP, true);
    taskManagerResource = String.format("TotalHeapMemory:%s,YoungHeapMemory:%s,TotalDirectMemory:%s",
      totalHeapSizeMB, (int) (totalHeapSizeMB * TaskManagerOptions.TASK_MANAGER_MEMORY_DYNAMIC_YOUNG_RATIO.defaultValue()),
      TM_PROCESS_NETTY_MEMORY_MB + (TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX.defaultValue() >> 20)
      + TM_MANAGED_MEMORY_MB + TM_FLOATING_MEMORY_MB);
    Assert.assertEquals(taskManagerResource, TaskManagerResourceCalculator.getTaskManagerResourceFromConf(flinkConfig));
    Assert.assertEquals(taskManagerResource, TaskManagerResourceCalculator.getTaskManagerResourceFromConf(flinkConfig));
  }

  @Test
  public void testCalculateNetworkBufferMemory() {
    Configuration flinkConf = mock(Configuration.class);
    when(flinkConf.getFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION)).thenReturn(0.2f);
    when(flinkConf.getLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN)).thenReturn(128L << 20);
    when(flinkConf.getLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX)).thenReturn(512L << 20);
    Assert.assertEquals(128L << 20, TaskManagerResourceCalculator.calculateNetworkBufferMemory(flinkConf));
  }
}
