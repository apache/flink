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

import org.apache.flink.runtime.jobgraph.FormatUtil.OutputFormatStub;
import org.apache.flink.runtime.operators.util.TaskConfig;

import javax.annotation.Nullable;

import java.util.Collections;

/**
 * A task vertex that run an initialization on the master, trying to deserialize an output format
 * and initializing/finalizing it on master, if necessary.
 */
public class OutputFormatVertex extends JobVertex {

	private static final long serialVersionUID = 1L;

	private String formatDescription;

	private OperatorID sinkOperatorId;

	/**
	 * Creates a new task vertex with the specified name.
	 *
	 * @param name The name of the task vertex.
	 */
	public OutputFormatVertex(String name) {
		this(name, null);
	}

	public OutputFormatVertex(String name, @Nullable OperatorID sinkOperatorId) {
		super(name);
		this.sinkOperatorId = (sinkOperatorId != null) ? sinkOperatorId : OperatorID.fromJobVertexID(this.getID());
	}

	public void setFormatDescription(String formatDescription) {
		this.formatDescription = formatDescription;
	}

	public String getFormatDescription() {
		return formatDescription;
	}

	@Override
	public void initializeOnMaster(ClassLoader loader) throws Exception {
		final TaskConfig cfg = new TaskConfig(getConfiguration());

		OutputFormatStub stub = new OutputFormatStub(cfg, loader, sinkOperatorId);

		FormatUtil.initializeOutputFormatsOnMaster(this, stub, Collections.singletonMap(sinkOperatorId, formatDescription));
	}

	@Override
	public void finalizeOnMaster(ClassLoader loader) throws Exception {
		final TaskConfig cfg = new TaskConfig(getConfiguration());

		OutputFormatStub stub = new OutputFormatStub(cfg, loader, sinkOperatorId);

		FormatUtil.finalizeOutputFormatsOnMaster(this, stub, Collections.singletonMap(sinkOperatorId, formatDescription));
	}
}
