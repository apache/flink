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

package org.apache.flink.mesos.runtime.clusterframework.store;

import org.apache.mesos.Protos;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Objects;

import scala.Option;

import static java.util.Objects.requireNonNull;

/** A store of Mesos workers and associated framework information. */
public interface MesosWorkerStore {

    /** The template for naming the worker. */
    DecimalFormat TASKID_FORMAT = new DecimalFormat("taskmanager-00000");

    /** Start the worker store. */
    void start() throws Exception;

    /**
     * Stop the worker store.
     *
     * @param cleanup if true, cleanup any stored information.
     */
    void stop(boolean cleanup) throws Exception;

    /** Get the stored Mesos framework ID. */
    Option<Protos.FrameworkID> getFrameworkID() throws Exception;

    /** Set the stored Mesos framework ID. */
    void setFrameworkID(Option<Protos.FrameworkID> frameworkID) throws Exception;

    /** Recover the stored workers. */
    List<Worker> recoverWorkers() throws Exception;

    /** Generate a new task ID for a worker. */
    Protos.TaskID newTaskID() throws Exception;

    /** Put a worker into storage. */
    void putWorker(Worker worker) throws Exception;

    /**
     * Remove a worker from storage.
     *
     * @return true if the worker existed.
     */
    boolean removeWorker(Protos.TaskID taskID) throws Exception;

    /**
     * A stored worker.
     *
     * <p>The assigned slaveid/hostname is valid in Launched and Released states. The hostname is
     * needed by Fenzo for optimization purposes.
     */
    class Worker implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Protos.TaskID taskID;

        private final Option<Protos.SlaveID> slaveID;

        private final Option<String> hostname;

        private final WorkerState state;

        private Worker(
                Protos.TaskID taskID,
                Option<Protos.SlaveID> slaveID,
                Option<String> hostname,
                WorkerState state) {
            this.taskID = requireNonNull(taskID, "taskID");
            this.slaveID = requireNonNull(slaveID, "slaveID");
            this.hostname = requireNonNull(hostname, "hostname");
            this.state = requireNonNull(state, "state");
        }

        /** Get the worker's task ID. */
        public Protos.TaskID taskID() {
            return taskID;
        }

        /** Get the worker's assigned slave ID. */
        public Option<Protos.SlaveID> slaveID() {
            return slaveID;
        }

        /** Get the worker's assigned hostname. */
        public Option<String> hostname() {
            return hostname;
        }

        /** Get the worker's state. */
        public WorkerState state() {
            return state;
        }

        // valid transition methods

        /**
         * Create a new worker with the given taskID.
         *
         * @return a new worker instance.
         */
        public static Worker newWorker(Protos.TaskID taskID) {
            return new Worker(
                    taskID,
                    Option.<Protos.SlaveID>empty(),
                    Option.<String>empty(),
                    WorkerState.New);
        }

        /**
         * Transition the worker to a launched state.
         *
         * @return a new worker instance (does not mutate the current instance).
         */
        public Worker launchWorker(Protos.SlaveID slaveID, String hostname) {
            return new Worker(
                    taskID, Option.apply(slaveID), Option.apply(hostname), WorkerState.Launched);
        }

        /**
         * Transition the worker to a released state.
         *
         * @return a new worker instance (does not mutate the current instance).
         */
        public Worker releaseWorker() {
            return new Worker(taskID, slaveID, hostname, WorkerState.Released);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Worker worker = (Worker) o;
            return Objects.equals(taskID, worker.taskID)
                    && Objects.equals(slaveID, worker.slaveID)
                    && Objects.equals(hostname, worker.hostname)
                    && state == worker.state;
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskID, slaveID, hostname, state);
        }

        @Override
        public String toString() {
            return "Worker{"
                    + "taskID="
                    + taskID
                    + ", slaveID="
                    + slaveID
                    + ", hostname="
                    + hostname
                    + ", state="
                    + state
                    + '}';
        }
    }

    /** The (planned) state of the worker. */
    enum WorkerState {

        /** Indicates that the worker is new (not yet launched). */
        New,

        /** Indicates that the worker is launched. */
        Launched,

        /** Indicates that the worker is released. */
        Released
    }
}
