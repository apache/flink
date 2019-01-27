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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.jobgraph.FormatUtil.MultiFormatStub;
import org.apache.flink.runtime.operators.util.TaskConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A task vertex that run an initialization on the master, trying to deserialize input/output formats
 * and initializing/finalizing them on master, if necessary.
 */
public class MultiInputOutputFormatVertex extends JobVertex {

	private static final long serialVersionUID = 1L;

	private Map<OperatorID, String> formatDescriptionMap = new HashMap<>();

	/**
	 * Creates a new task vertex with the specified name.
	 *
	 * @param name The name of the task vertex.
	 */
	public MultiInputOutputFormatVertex(String name) {
		super(name);
	}

	public MultiInputOutputFormatVertex(String name, JobVertexID id, List<JobVertexID> alternativeIds, List<OperatorID> operatorIds, List<OperatorID> alternativeOperatorIds) {
		super(name, id, alternativeIds, operatorIds, alternativeOperatorIds);
	}

	public void setFormatDescription(OperatorID operatorId, String formatDescription) {
		formatDescriptionMap.put(operatorId, formatDescription);
	}

	public String getFormatDescription(OperatorID operatorId) {
		return formatDescriptionMap.get(operatorId);
	}

	@Override
	public void initializeOnMaster(ClassLoader loader) throws Exception {
		final TaskConfig cfg = new TaskConfig(getConfiguration());

		MultiFormatStub stub = new MultiFormatStub(cfg, loader);

		FormatUtil.initializeInputFormatsOnMaster(this, stub, Collections.unmodifiableMap(formatDescriptionMap));
		FormatUtil.initializeOutputFormatsOnMaster(this, stub, Collections.unmodifiableMap(formatDescriptionMap));
	}

	@Override
	public void finalizeOnMaster(ClassLoader loader) throws Exception {
		final TaskConfig cfg = new TaskConfig(getConfiguration());

		MultiFormatStub stub = new MultiFormatStub(cfg, loader);

		FormatUtil.finalizeOutputFormatsOnMaster(this, stub, Collections.unmodifiableMap(formatDescriptionMap));
	}
}
