/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.rpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import eu.stratosphere.nephele.client.AbstractJobResult;
import eu.stratosphere.nephele.client.JobCancelResult;
import eu.stratosphere.nephele.client.JobProgressResult;
import eu.stratosphere.nephele.client.JobSubmissionResult;
import eu.stratosphere.nephele.event.job.JobEvent;
import eu.stratosphere.nephele.event.job.VertexEvent;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheProfileRequest;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheProfileResponse;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheUpdate;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobStatus;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.types.IntegerRecord;

/**
 * This utility class provides a list of types frequently used by the RPC protocols included in this package.
 * 
 * @author warneke
 */
public class CommonTypeUtils {

	/**
	 * Private constructor to prevent instantiation.
	 */
	private CommonTypeUtils() {
	}

	/**
	 * Returns a list of types frequently used by the RPC protocols of this package and its parent packages.
	 * 
	 * @return a list of types frequently used by the RPC protocols of this package
	 */
	public static List<Class<?>> getRPCTypesToRegister() {

		final ArrayList<Class<?>> types = new ArrayList<Class<?>>();

		types.add(AbstractJobResult.class);
		types.add(AbstractJobResult.ReturnCode.class);
		types.add(ChannelID.class);
		types.add(ChannelType.class);
		types.add(CompressionLevel.class);
		types.add(ExecutionState.class);
		types.add(GateID.class);
		types.add(HashMap.class);
		types.add(IntegerRecord.class);
		types.add(JobCancelResult.class);
		types.add(JobEvent.class);
		types.add(JobGraph.class);
		types.add(JobID.class);
		types.add(JobProgressResult.class);
		types.add(JobStatus.class);
		types.add(JobSubmissionResult.class);
		types.add(JobVertexID.class);
		types.add(LibraryCacheProfileRequest.class);
		types.add(LibraryCacheProfileResponse.class);
		types.add(LibraryCacheUpdate.class);
		types.add(Path.class);
		types.add(Set.class);
		types.add(VertexEvent.class);

		return types;
	}
}
