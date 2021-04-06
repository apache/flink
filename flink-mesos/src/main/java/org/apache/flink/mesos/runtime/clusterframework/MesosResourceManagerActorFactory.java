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

package org.apache.flink.mesos.runtime.clusterframework;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.mesos.scheduler.TaskSchedulerBuilder;

import akka.actor.ActorRef;
import org.apache.mesos.SchedulerDriver;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

import scala.concurrent.duration.FiniteDuration;

/** Factory class for actors used in mesos deployment. */
public interface MesosResourceManagerActorFactory {

    /** Create self actor for mesos resource manager. */
    ActorRef createSelfActorForMesosResourceManagerDriver(MesosResourceManagerDriver self);

    /** Create actor for the connection monitor. */
    ActorRef createConnectionMonitor(Configuration flinkConfig);

    /** Create actor for the task monitor. */
    ActorRef createTaskMonitor(
            Configuration flinkConfig,
            ActorRef resourceManagerActor,
            SchedulerDriver schedulerDriver);

    /** Create actor for the launch coordinator. */
    ActorRef createLaunchCoordinator(
            Configuration flinkConfig,
            ActorRef resourceManagerActor,
            SchedulerDriver schedulerDriver,
            TaskSchedulerBuilder optimizer);

    /** Create actor for the reconciliation coordinator. */
    ActorRef createReconciliationCoordinator(
            Configuration flinkConfig, SchedulerDriver schedulerDriver);

    /**
     * Tries to shut down the given actor gracefully.
     *
     * @param actorRef specifying the actor to shut down
     * @param timeout for the graceful shut down
     * @return A future that finishes with {@code true} iff. the actor could be stopped gracefully
     *     or {@code actorRef} was {@code null}.
     */
    CompletableFuture<Boolean> stopActor(@Nullable final ActorRef actorRef, FiniteDuration timeout);
}
