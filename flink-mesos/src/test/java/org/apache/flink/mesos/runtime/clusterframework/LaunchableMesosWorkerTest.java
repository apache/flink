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
import org.apache.flink.mesos.util.MesosArtifactResolver;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.util.TestLogger;
import org.apache.mesos.Protos;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import scala.Option;

import java.util.List;

import static org.junit.Assert.*;

public class LaunchableMesosWorkerTest extends TestLogger {
	MesosTaskManagerParameters params = Mockito.mock(MesosTaskManagerParameters.class);
	MesosArtifactResolver resolver = Mockito.mock(MesosArtifactResolver.class);
	ContainerSpecification containerSpec = Mockito.mock(ContainerSpecification.class);
	Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue("1234").build();
	LaunchableMesosWorker worker = new LaunchableMesosWorker(resolver, params, containerSpec, taskId);

	@Test
	public void volumes() throws Exception {
		List<Protos.Volume> vols;
		assertEquals(worker.volumes(Option.<String>apply(null)).size(), 0);
		String spec1 = "/host/path:/container/path:RO,/container/path:ro,/host/path:/container/path,/container/path";
		vols = worker.volumes(Option.<String>apply(spec1));
		assertEquals(vols.size(), 4);
		assertEquals("/container/path", vols.get(0).getContainerPath());
		assertEquals("/host/path", vols.get(0).getHostPath());
		assertEquals(Protos.Volume.Mode.RO, vols.get(0).getMode());
		assertEquals("/container/path", vols.get(1).getContainerPath());
		assertEquals(Protos.Volume.Mode.RO, vols.get(1).getMode());
		assertEquals("/container/path", vols.get(2).getContainerPath());
		assertEquals("/host/path", vols.get(2).getHostPath());
		assertEquals(Protos.Volume.Mode.RW, vols.get(2).getMode());
		assertEquals("/container/path", vols.get(3).getContainerPath());
		assertEquals(Protos.Volume.Mode.RW, vols.get(3).getMode());

		// should handle empty strings, but not error
		assertEquals(0, worker.volumes(Option.<String>apply("")).size());
	}

	@Test(expected=IllegalArgumentException.class)
	public void volumesBadMode() throws Exception {
		worker.volumes(Option.<String>apply("/hp:/cp:RF"));
	}

	@Test(expected=IllegalArgumentException.class)
	public void volumesMalformed() throws Exception {
		worker.volumes(Option.<String>apply("/hp:/cp:ro:extra"));
	}

}
