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
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

public class TestFramework {
  static class TestScheduler implements Scheduler {
    public TestScheduler(ExecutorInfo executor) {
      this(executor, 5);
    }

    public TestScheduler(ExecutorInfo executor, int totalTasks) {
      this.executor = executor;
      this.totalTasks = totalTasks;
    }

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
      for (Offer offer : offers) {
        List<TaskInfo> tasks = new ArrayList<TaskInfo>();
        if (launchedTasks < totalTasks) {
          TaskID taskId = TaskID.newBuilder()
            .setValue(Integer.toString(launchedTasks++)).build();

          System.out.println("Launching task " + taskId.getValue());

          TaskInfo task = TaskInfo.newBuilder()
            .setName("task " + taskId.getValue())
            .setTaskId(taskId)
            .setSlaveId(offer.getSlaveId())
            .addResources(Resource.newBuilder()
                          .setName("cpus")
                          .setType(Value.Type.SCALAR)
                          .setScalar(Value.Scalar.newBuilder().setValue(1)))
            .addResources(Resource.newBuilder()
                          .setName("mem")
                          .setType(Value.Type.SCALAR)
                          .setScalar(Value.Scalar.newBuilder().setValue(128)))
            .setExecutor(ExecutorInfo.newBuilder(executor))
            .build();
          tasks.add(task);
        }
        Filters filters = Filters.newBuilder().setRefuseSeconds(1).build();
        driver.launchTasks(offer.getId(), tasks, filters);
      }
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, OfferID offerId) {}

    @Override
    public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
      System.out.println("Status update: task " + status.getTaskId().getValue() +
                         " is in state " + status.getState());
      if (status.getState() == TaskState.TASK_FINISHED) {
        finishedTasks++;
        System.out.println("Finished tasks: " + finishedTasks);
        if (finishedTasks == totalTasks) {
          driver.stop();
        }
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

    private final ExecutorInfo executor;
    private final int totalTasks;
    private int launchedTasks = 0;
    private int finishedTasks = 0;
  }

  private static void usage() {
    String name = TestFramework.class.getName();
    System.err.println("Usage: " + name + " master <tasks>");
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1 || args.length > 2) {
      usage();
      System.exit(1);
    }

    String uri = new File("./test-executor").getCanonicalPath();

    ExecutorInfo executor = ExecutorInfo.newBuilder()
      .setExecutorId(ExecutorID.newBuilder().setValue("default"))
      .setCommand(CommandInfo.newBuilder().setValue(uri))
      .setName("Test Executor (Java)")
      .setSource("java_test")
      .build();

    FrameworkInfo.Builder frameworkBuilder = FrameworkInfo.newBuilder()
        .setUser("") // Have Mesos fill in the current user.
        .setName("Test Framework (Java)");

    // TODO(vinod): Make checkpointing the default when it is default
    // on the slave.
    if (System.getenv("MESOS_CHECKPOINT") != null) {
      System.out.println("Enabling checkpoint for the framework");
      frameworkBuilder.setCheckpoint(true);
    }

    Scheduler scheduler = args.length == 1
        ? new TestScheduler(executor)
        : new TestScheduler(executor, Integer.parseInt(args[1]));

    MesosSchedulerDriver driver = null;
    if (System.getenv("MESOS_AUTHENTICATE") != null) {
      System.out.println("Enabling authentication for the framework");

      if (System.getenv("DEFAULT_PRINCIPAL") == null) {
        System.err.println("Expecting authentication principal in the environment");
        System.exit(1);
      }

      if (System.getenv("DEFAULT_SECRET") == null) {
        System.err.println("Expecting authentication secret in the environment");
        System.exit(1);
      }

      Credential credential = Credential.newBuilder()
        .setPrincipal(System.getenv("DEFAULT_PRINCIPAL"))
        .setSecret(ByteString.copyFrom(System.getenv("DEFAULT_SECRET").getBytes()))
        .build();

      frameworkBuilder.setPrincipal(System.getenv("DEFAULT_PRINCIPAL"));

      driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), args[0], credential);
    } else {
      frameworkBuilder.setPrincipal("test-framework-java");

      driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), args[0]);
    }

    int status = driver.run() == Status.DRIVER_STOPPED ? 0 : 1;

    // Ensure that the driver process terminates.
    driver.stop();

    System.exit(status);
  }
}
