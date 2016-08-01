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
import scala.Option;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * A store of Mesos workers and associated framework information.
 *
 * Generates a framework ID as necessary.
 */
public interface MesosWorkerStore {

	static final DecimalFormat TASKID_FORMAT = new DecimalFormat("taskmanager-00000");

	void start() throws Exception;

	void stop() throws Exception;

	Option<Protos.FrameworkID> getFrameworkID() throws Exception;

	void setFrameworkID(Option<Protos.FrameworkID> frameworkID) throws Exception;

	List<Worker> recoverWorkers() throws Exception;

	Protos.TaskID newTaskID() throws Exception;

	void putWorker(Worker worker) throws Exception;

	void removeWorker(Protos.TaskID taskID) throws Exception;

	void cleanup() throws Exception;

	/**
	 * A stored task.
	 *
	 * The assigned slaveid/hostname is valid in Launched and Released states.  The hostname is needed
	 * by Fenzo for optimization purposes.
	 */
	class Worker implements Serializable {
		private Protos.TaskID taskID;

		private Option<Protos.SlaveID> slaveID;

		private Option<String> hostname;

		private TaskState state;

		public Worker(Protos.TaskID taskID, Option<Protos.SlaveID> slaveID, Option<String> hostname, TaskState state) {
			requireNonNull(taskID, "taskID");
			requireNonNull(slaveID, "slaveID");
			requireNonNull(hostname, "hostname");
			requireNonNull(state, "state");

			this.taskID = taskID;
			this.slaveID = slaveID;
			this.hostname = hostname;
			this.state = state;
		}

		public Protos.TaskID taskID() {
			return taskID;
		}

		public Option<Protos.SlaveID> slaveID() {
			return slaveID;
		}

		public Option<String> hostname() {
			return hostname;
		}

		public TaskState state() {
			return state;
		}

		// valid transition methods

		public static Worker newTask(Protos.TaskID taskID) {
			return new Worker(
				taskID,
				Option.<Protos.SlaveID>empty(), Option.<String>empty(),
				TaskState.New);
		}

		public Worker launchTask(Protos.SlaveID slaveID, String hostname) {
			return new Worker(taskID, Option.apply(slaveID), Option.apply(hostname), TaskState.Launched);
		}

		public Worker releaseTask() {
			return new Worker(taskID, slaveID, hostname, TaskState.Released);
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
			return Objects.equals(taskID, worker.taskID) &&
				Objects.equals(slaveID.isDefined() ? slaveID.get() : null, worker.slaveID.isDefined() ? worker.slaveID.get() : null) &&
				Objects.equals(hostname.isDefined() ? hostname.get() : null, worker.hostname.isDefined() ? worker.hostname.get() : null) &&
				state == worker.state;
		}

		@Override
		public int hashCode() {
			return Objects.hash(taskID, slaveID.isDefined() ? slaveID.get() : null, hostname.isDefined() ? hostname.get() : null, state);
		}

		@Override
		public String toString() {
			return "Worker{" +
				"taskID=" + taskID +
				", slaveID=" + slaveID +
				", hostname=" + hostname +
				", state=" + state +
				'}';
		}
	}

	enum TaskState {
		New,Launched,Released
	}
}
