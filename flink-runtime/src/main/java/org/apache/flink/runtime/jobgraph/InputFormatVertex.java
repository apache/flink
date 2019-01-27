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

import org.apache.flink.runtime.jobgraph.FormatUtil.InputFormatStub;
import org.apache.flink.runtime.operators.util.TaskConfig;

import javax.annotation.Nullable;

import java.util.Collections;

/**
 * A task vertex that run an initialization on the master, trying to deserialize an input format
 * and initializing it on master, if necessary.
 */
public class InputFormatVertex extends JobVertex {

	private static final long serialVersionUID = 1L;

	private String formatDescription;

	private OperatorID sourceOperatorId;

	/**
	 * Creates a new task vertex with the specified name.
	 *
	 * @param name The name of the task vertex.
	 */
	public InputFormatVertex(String name) {
		this(name, null);
	}

	public InputFormatVertex(String name, @Nullable OperatorID sourceOperatorId) {
		super(name);
		this.sourceOperatorId = (sourceOperatorId != null) ? sourceOperatorId : OperatorID.fromJobVertexID(this.getID());
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

		InputFormatStub stub = new InputFormatStub(cfg, loader, sourceOperatorId);

		FormatUtil.initializeInputFormatsOnMaster(this, stub, Collections.singletonMap(sourceOperatorId, formatDescription));
	}
}
