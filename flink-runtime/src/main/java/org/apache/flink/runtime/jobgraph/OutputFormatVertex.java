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

import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.runtime.operators.util.TaskConfig;

/**
 * A task vertex that run an initialization on the master, trying to deserialize an output format
 * and initializing it on master, if necessary.
 */
public class OutputFormatVertex extends JobVertex {
	
	private static final long serialVersionUID = 1L;
	
	private String formatDescription;
	
	/**
	 * Creates a new task vertex with the specified name.
	 * 
	 * @param name The name of the task vertex.
	 */
	public OutputFormatVertex(String name) {
		super(name);
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
		
		UserCodeWrapper<OutputFormat<?>> wrapper;
		try {
			wrapper = cfg.<OutputFormat<?>>getStubWrapper(loader);
		}
		catch (Throwable t) {
			throw new Exception("Deserializing the OutputFormat (" + formatDescription + ") failed: " + t.getMessage(), t);
		}
		if (wrapper == null) {
			throw new Exception("No input format present in InputFormatVertex's task configuration.");
		}
		
		OutputFormat<?> outputFormat;
		try {
			outputFormat = wrapper.getUserCodeObject(OutputFormat.class, loader);
		}
		catch (Throwable t) {
			throw new Exception("Instantiating the OutputFormat (" + formatDescription + ") failed: " + t.getMessage(), t);
		}

		// set user classloader before calling user code
		final ClassLoader prevContextCl = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(loader);

		try {
			// configure output format
			try {
				outputFormat.configure(cfg.getStubParameters());
			} catch (Throwable t) {
				throw new Exception("Configuring the OutputFormat (" + formatDescription + ") failed: " + t.getMessage(), t);
			}
			if (outputFormat instanceof InitializeOnMaster) {
				((InitializeOnMaster) outputFormat).initializeGlobal(getParallelism());
			}
		} finally {
			// restore previous classloader
			Thread.currentThread().setContextClassLoader(prevContextCl);
		}
	}
	
	@Override
	public void finalizeOnMaster(ClassLoader loader) throws Exception {
		final TaskConfig cfg = new TaskConfig(getConfiguration());

		UserCodeWrapper<OutputFormat<?>> wrapper;
		try {
			wrapper = cfg.<OutputFormat<?>>getStubWrapper(loader);
		}
		catch (Throwable t) {
			throw new Exception("Deserializing the OutputFormat (" + formatDescription + ") failed: " + t.getMessage(), t);
		}
		if (wrapper == null) {
			throw new Exception("No input format present in InputFormatVertex's task configuration.");
		}

		OutputFormat<?> outputFormat;
		try {
			outputFormat = wrapper.getUserCodeObject(OutputFormat.class, loader);
		}
		catch (Throwable t) {
			throw new Exception("Instantiating the OutputFormat (" + formatDescription + ") failed: " + t.getMessage(), t);
		}

		// set user classloader before calling user code
		final ClassLoader prevContextCl = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(loader);

		try {
			// configure output format
			try {
				outputFormat.configure(cfg.getStubParameters());
			} catch (Throwable t) {
				throw new Exception("Configuring the OutputFormat (" + formatDescription + ") failed: " + t.getMessage(), t);
			}
			if (outputFormat instanceof FinalizeOnMaster) {
				((FinalizeOnMaster) outputFormat).finalizeGlobal(getParallelism());
			}
		} finally {
			// restore previous classloader
			Thread.currentThread().setContextClassLoader(prevContextCl);
		}
	}
}
