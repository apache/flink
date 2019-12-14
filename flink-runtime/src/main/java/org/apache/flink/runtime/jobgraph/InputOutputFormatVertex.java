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
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.runtime.operators.util.TaskConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A task vertex that runs an initialization and a finalization on the master. If necessary, it tries
 * to deserialize input and output formats, and initialize and finalize them on master.
 */
public class InputOutputFormatVertex extends JobVertex {

	private static final long serialVersionUID = 1L;

	private final Map<OperatorID, String> formatDescriptions = new HashMap<>();

	public InputOutputFormatVertex(String name) {
		super(name);
	}

	public InputOutputFormatVertex(
		String name,
		JobVertexID id,
		List<JobVertexID> alternativeIds,
		List<OperatorID> operatorIds,
		List<OperatorID> alternativeOperatorIds) {

		super(name, id, alternativeIds, operatorIds, alternativeOperatorIds);
	}

	@Override
	public void initializeOnMaster(ClassLoader loader) throws Exception {
		final InputOutputFormatContainer formatContainer = initInputOutputformatContainer(loader);

		final ClassLoader original = Thread.currentThread().getContextClassLoader();
		try {
			// set user classloader before calling user code
			Thread.currentThread().setContextClassLoader(loader);

			// configure the input format and setup input splits
			Map<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> inputFormats = formatContainer.getInputFormats();
			if (inputFormats.size() > 1) {
				throw new UnsupportedOperationException("Multiple input formats are not supported in a job vertex.");
			}
			for (Map.Entry<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> entry : inputFormats.entrySet()) {
				final InputFormat<?, ?> inputFormat;

				try {
					inputFormat = entry.getValue().getUserCodeObject();
					inputFormat.configure(formatContainer.getParameters(entry.getKey()));
				} catch (Throwable t) {
					throw new Exception("Configuring the input format (" + getFormatDescription(entry.getKey()) + ") failed: "
						+ t.getMessage(), t);
				}

				setInputSplitSource(inputFormat);
			}

			// configure input formats and invoke initializeGlobal()
			Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> outputFormats = formatContainer.getOutputFormats();
			for (Map.Entry<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> entry : outputFormats.entrySet()) {
				final OutputFormat<?> outputFormat;

				try {
					outputFormat = entry.getValue().getUserCodeObject();
					outputFormat.configure(formatContainer.getParameters(entry.getKey()));
				} catch (Throwable t) {
					throw new Exception("Configuring the output format (" + getFormatDescription(entry.getKey()) + ") failed: "
						+ t.getMessage(), t);
				}

				if (outputFormat instanceof InitializeOnMaster) {
					((InitializeOnMaster) outputFormat).initializeGlobal(getParallelism());
				}
			}

		} finally {
			// restore original classloader
			Thread.currentThread().setContextClassLoader(original);
		}
	}

	@Override
	public void finalizeOnMaster(ClassLoader loader) throws Exception {
		final InputOutputFormatContainer formatContainer = initInputOutputformatContainer(loader);

		final ClassLoader original = Thread.currentThread().getContextClassLoader();
		try {
			// set user classloader before calling user code
			Thread.currentThread().setContextClassLoader(loader);

			// configure input formats and invoke finalizeGlobal()
			Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> outputFormats = formatContainer.getOutputFormats();
			for (Map.Entry<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> entry : outputFormats.entrySet()) {
				final OutputFormat<?> outputFormat;

				try {
					outputFormat = entry.getValue().getUserCodeObject();
					outputFormat.configure(formatContainer.getParameters(entry.getKey()));
				} catch (Throwable t) {
					throw new Exception("Configuring the output format (" + getFormatDescription(entry.getKey()) + ") failed: "
						+ t.getMessage(), t);
				}

				if (outputFormat instanceof FinalizeOnMaster) {
					((FinalizeOnMaster) outputFormat).finalizeGlobal(getParallelism());
				}
			}

		} finally {
			// restore original classloader
			Thread.currentThread().setContextClassLoader(original);
		}
	}

	public String getFormatDescription(OperatorID operatorID) {
		return formatDescriptions.get(operatorID);
	}

	public void setFormatDescription(OperatorID operatorID, String formatDescription) {
		formatDescriptions.put(checkNotNull(operatorID), formatDescription);
	}

	private InputOutputFormatContainer initInputOutputformatContainer(ClassLoader classLoader) throws Exception {
		try {
			return new InputOutputFormatContainer(new TaskConfig(getConfiguration()), classLoader);
		} catch (Throwable t) {
			throw new Exception("Loading the input/output formats failed: "
				+ String.join(",", formatDescriptions.values()), t);
		}
	}
}
