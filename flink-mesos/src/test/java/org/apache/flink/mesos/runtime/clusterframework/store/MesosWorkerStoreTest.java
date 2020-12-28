/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.mesos.runtime.clusterframework.store;

import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import org.apache.mesos.Protos;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/** Tests for the {@link MesosWorkerStore}. */
public class MesosWorkerStoreTest extends TestLogger {

    /** Tests that {@link MesosWorkerStore.Worker} is serializable. */
    @Test
    public void workerIsSerializable() {
        final Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue("foobar").build();
        final Protos.SlaveID slaveId = Protos.SlaveID.newBuilder().setValue("barfoo").build();
        final String hostname = "foobar";

        final MesosWorkerStore.Worker worker = MesosWorkerStore.Worker.newWorker(taskId);
        final MesosWorkerStore.Worker launchedWorker = worker.launchWorker(slaveId, hostname);

        assertTrue(InstantiationUtil.isSerializable(worker));
        assertTrue(InstantiationUtil.isSerializable(launchedWorker));
    }
}
