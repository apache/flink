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

import com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;


public class TestMultipleExecutorsFramework {
  static class TestScheduler implements Scheduler {
    @Override
    public void registered(SchedulerDriver driver,
                           FrameworkID frameworkId,
                           MasterInfo masterInfo) {
      System.out.println("Registered! ID = " + frameworkId.getValue());
    }

    @Override
    public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {}

    @Override
    public void disconnected(SchedulerDriver driver) {}

    @Override
    public void resourceOffers(SchedulerDriver driver,
                               List<Offer> offers) {
      try {
        File file = new File("./test-executor");

        for (Offer offer : offers) {
          List<TaskInfo> tasks = new ArrayList<TaskInfo>();
          if (!fooLaunched) {
            TaskID taskId = TaskID.newBuilder()
              .setValue("foo")
              .build();

            System.out.println("Launching task " + taskId.getValue());

            TaskInfo task = TaskInfo.newBuilder()
              .setName("task " + taskId.getValue())
              .setTaskId(taskId)
              .setSlaveId(offer.getSlaveId())
              .addResources(Resource.newBuilder()
                            .setName("cpus")
                            .setType(Value.Type.SCALAR)
                            .setScalar(Value.Scalar.newBuilder()
                                       .setValue(1)
                                       .build())
                            .build())
              .addResources(Resource.newBuilder()
                            .setName("mem")
                            .setType(Value.Type.SCALAR)
                            .setScalar(Value.Scalar.newBuilder()
                                       .setValue(128)
                                       .build())
                            .build())
              .setExecutor(ExecutorInfo.newBuilder()
                           .setExecutorId(ExecutorID.newBuilder()
                                          .setValue("executor-foo")
                                          .build())
                           .setCommand(CommandInfo.newBuilder()
                                       .setValue(file.getCanonicalPath())
                                       .build())
                           .build())
              .setData(ByteString.copyFromUtf8("100000"))
              .build();

            tasks.add(task);

            fooLaunched = true;
          }

          if (!barLaunched) {
            TaskID taskId = TaskID.newBuilder()
              .setValue("bar")
              .build();

            System.out.println("Launching task " + taskId.getValue());

            TaskInfo task = TaskInfo.newBuilder()
              .setName("task " + taskId.getValue())
              .setTaskId(taskId)
              .setSlaveId(offer.getSlaveId())
              .addResources(Resource.newBuilder()
                            .setName("cpus")
                            .setType(Value.Type.SCALAR)
                            .setScalar(Value.Scalar.newBuilder()
                                       .setValue(1)
                                       .build())
                            .build())
              .addResources(Resource.newBuilder()
                            .setName("mem")
                            .setType(Value.Type.SCALAR)
                            .setScalar(Value.Scalar.newBuilder()
                                       .setValue(128)
                                       .build())
                            .build())
              .setExecutor(ExecutorInfo.newBuilder()
                           .setExecutorId(ExecutorID.newBuilder()
                                          .setValue("executor-bar")
                                          .build())
                           .setCommand(CommandInfo.newBuilder()
                                       .setValue(file.getCanonicalPath())
                                       .build())
                           .build())
              .setData(ByteString.copyFrom("100000".getBytes()))
              .build();

            tasks.add(task);

            barLaunched = true;
          }

          driver.launchTasks(offer.getId(), tasks);
        }
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, OfferID offerId) {}

    @Override
    public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
      System.out.println("Status update: task " + status.getTaskId() +
                         " is in state " + status.getState());
      if (status.getState() == TaskState.TASK_FINISHED) {
        if (status.getTaskId().getValue().equals("foo")) {
          fooFinished = true;
        }
        if (status.getTaskId().getValue().equals("bar")) {
          barFinished = true;
        }
      }

      if (fooFinished && barFinished) {
        driver.stop();
      }
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver,
                                 ExecutorID executorId,
                                 SlaveID slaveId,
                                 byte[] data) {}

    @Override
    public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {}

    @Override
    public void executorLost(SchedulerDriver driver,
                             ExecutorID executorId,
                             SlaveID slaveId,
                             int status) {}

    public void error(SchedulerDriver driver, String message) {
      System.out.println("Error: " + message);
    }

    private boolean fooLaunched = false;
    private boolean barLaunched = false;
    private boolean fooFinished = false;
    private boolean barFinished = false;
  }

  private static void usage() {
    String name = TestMultipleExecutorsFramework.class.getName();
    System.err.println("Usage: " + name + " master <tasks>");
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1 || args.length > 2) {
      usage();
      System.exit(1);
    }

    FrameworkInfo framework = FrameworkInfo.newBuilder()
        .setUser("") // Have Mesos fill in the current user.
        .setName("Test Multiple Executors Framework (Java)")
        .setPrincipal("test-multiple-executors-framework-java")
        .build();

    MesosSchedulerDriver driver = new MesosSchedulerDriver(
        new TestScheduler(),
        framework,
        args[0]);

    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }
}
