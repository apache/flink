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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import scala.Option;

/**
 * A standalone Mesos worker store.
 *
 * @deprecated Apache Mesos support was deprecated in Flink 1.13 and is subject to removal in the
 *     future (see FLINK-22352 for further details).
 */
@Deprecated
public class StandaloneMesosWorkerStore implements MesosWorkerStore {

    private Option<Protos.FrameworkID> frameworkID = Option.empty();

    private int taskCount = 0;

    private Map<Protos.TaskID, Worker> storedWorkers = new LinkedHashMap<>();

    public StandaloneMesosWorkerStore() {}

    @Override
    public void start() throws Exception {}

    @Override
    public void stop(boolean cleanup) throws Exception {}

    @Override
    public Option<Protos.FrameworkID> getFrameworkID() throws Exception {
        return frameworkID;
    }

    @Override
    public void setFrameworkID(Option<Protos.FrameworkID> frameworkID) throws Exception {
        this.frameworkID = frameworkID;
    }

    @Override
    public List<Worker> recoverWorkers() throws Exception {
        List<Worker> workers = new ArrayList<>(storedWorkers.size());
        workers.addAll(storedWorkers.values());
        return workers;
    }

    @Override
    public Protos.TaskID newTaskID() throws Exception {
        Protos.TaskID taskID =
                Protos.TaskID.newBuilder().setValue(TASKID_FORMAT.format(++taskCount)).build();
        return taskID;
    }

    @Override
    public void putWorker(Worker worker) throws Exception {
        storedWorkers.put(worker.taskID(), worker);
    }

    @Override
    public boolean removeWorker(Protos.TaskID taskID) throws Exception {
        Worker prior = storedWorkers.remove(taskID);
        return prior != null;
    }
}
