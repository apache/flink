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

import org.apache.flink.mesos.runtime.clusterframework.store.MesosWorkerStore;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * A representation of a registered Mesos task managed by the {@link MesosResourceManagerDriver}.
 *
 * @deprecated Apache Mesos support was deprecated in Flink 1.13 and is subject to removal in the
 *     future (see FLINK-22352 for further details).
 */
@Deprecated
public class RegisteredMesosWorkerNode implements Serializable, ResourceIDRetrievable {

    private static final long serialVersionUID = 2;

    private final MesosWorkerStore.Worker worker;

    public RegisteredMesosWorkerNode(MesosWorkerStore.Worker worker) {
        this.worker = Preconditions.checkNotNull(worker);
        Preconditions.checkArgument(worker.slaveID().isDefined());
        Preconditions.checkArgument(worker.hostname().isDefined());
    }

    public MesosWorkerStore.Worker getWorker() {
        return worker;
    }

    @Override
    public ResourceID getResourceID() {
        return MesosResourceManagerDriver.extractResourceID(worker.taskID());
    }

    @Override
    public String toString() {
        return "RegisteredMesosWorkerNode{" + "worker=" + worker + '}';
    }
}
