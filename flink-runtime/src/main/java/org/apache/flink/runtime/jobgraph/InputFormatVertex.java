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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.runtime.operators.util.TaskConfig;

public class InputFormatVertex extends AbstractJobVertex {

	private static final long serialVersionUID = 1L;
	
	/** Caches the output format associated to this output vertex. */
	private transient InputFormat<?, ?> inputFormat;
	
	
	public InputFormatVertex(String name) {
		super(name);
	}
	
	public InputFormatVertex(String name, JobVertexID id) {
		super(name, id);
	}
	
	
	@Override
	public void initializeOnMaster(ClassLoader loader) throws Exception {
		if (inputFormat == null) {
			TaskConfig cfg = new TaskConfig(getConfiguration());
			UserCodeWrapper<InputFormat<?, ?>> wrapper = cfg.<InputFormat<?, ?>>getStubWrapper(loader);
			
			if (wrapper == null) {
				throw new Exception("No input format present in InputFormatVertex's task configuration.");
			}
			
			inputFormat = wrapper.getUserCodeObject(InputFormat.class, loader);
			inputFormat.configure(cfg.getStubParameters());
		}
		
		setInputSplitSource(inputFormat);
	}
}
