/**
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

import java.io.IOException;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.operators.util.TaskConfig;

public class JobInputVertex extends AbstractJobInputVertex {

	private InputFormat<?, ?> inputFormat;
	
	public JobInputVertex(String name, JobVertexID id, JobGraph jobGraph) {
		super(name, id, jobGraph);
	}
	
	/**
	 * Creates a new job file input vertex with the specified name.
	 * 
	 * @param name
	 *        The name of the new job file input vertex.
	 * @param jobGraph
	 *        The job graph this vertex belongs to.
	 */
	public JobInputVertex(String name, JobGraph jobGraph) {
		this(name, null, jobGraph);
	}

	/**
	 * Creates a new job file input vertex.
	 * 
	 * @param jobGraph
	 *        The job graph this vertex belongs to.
	 */
	public JobInputVertex(JobGraph jobGraph) {
		this(null, jobGraph);
	}
	
	public void setInputFormat(InputFormat<?, ?> format) {
		this.inputFormat = format;
	}
	
	public void initializeInputFormatFromTaskConfig(ClassLoader cl) {
		TaskConfig cfg = new TaskConfig(getConfiguration());
		
		UserCodeWrapper<InputFormat<?, ?>> wrapper = cfg.<InputFormat<?, ?>>getStubWrapper(cl);
		
		if (wrapper != null) {
			this.inputFormat = wrapper.getUserCodeObject(InputFormat.class, cl);
			this.inputFormat.configure(cfg.getStubParameters());
		}
	}

	/**
	 * Gets the input split type class
	 *
	 * @return Input split type class
	 */
	@Override
	public Class<? extends InputSplit> getInputSplitType() {
		if (inputFormat == null){
			return InputSplit.class;
		}

		return inputFormat.getInputSplitType();
	}

	/**
	 * Gets the input splits from the input format.
	 *
	 * @param minNumSplits Number of minimal input splits
	 * @return Array of input splits
	 * @throws IOException
	 */
	@Override
	public InputSplit[] getInputSplits(int minNumSplits) throws IOException {
		if (inputFormat == null){
			return null;
		}

		return inputFormat.createInputSplits(minNumSplits);
	}
}
