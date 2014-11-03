package examples; /**
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

import java.io.File;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;


public class TestExecutor implements Executor {
  @Override
  public void registered(ExecutorDriver driver,
                         ExecutorInfo executorInfo,
                         FrameworkInfo frameworkInfo,
                         SlaveInfo slaveInfo) {
    System.out.println("Registered executor on " + slaveInfo.getHostname());
  }

  @Override
  public void reregistered(ExecutorDriver driver, SlaveInfo executorInfo) {}

  @Override
  public void disconnected(ExecutorDriver driver) {}

  @Override
  public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
    new Thread() { public void run() {
      try {
        TaskStatus status = TaskStatus.newBuilder()
          .setTaskId(task.getTaskId())
          .setState(TaskState.TASK_RUNNING).build();

        driver.sendStatusUpdate(status);

        System.out.println("Running task " + task.getTaskId());

        // This is where one would perform the requested task.

        status = TaskStatus.newBuilder()
          .setTaskId(task.getTaskId())
          .setState(TaskState.TASK_FINISHED).build();

        driver.sendStatusUpdate(status);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }}.start();
  }

  @Override
  public void killTask(ExecutorDriver driver, TaskID taskId) {}

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] data) {}

  @Override
  public void shutdown(ExecutorDriver driver) {}

  @Override
  public void error(ExecutorDriver driver, String message) {}

  public static void main(String[] args) throws Exception {
    MesosExecutorDriver driver = new MesosExecutorDriver(new TestExecutor());
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }
}
