/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.services.accumulators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.api.common.accumulators.Accumulator;
import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.util.StringUtils;

/**
 * This class encapsulates a map of accumulators for a single job. It is used
 * for the transfer from TaskManagers to the JobManager and from the JobManager
 * to the Client.
 */
public class AccumulatorEvent implements IOReadableWritable {

	private JobID jobID;

	private Map<String, Accumulator<?, ?>> accumulators = new HashMap<String, Accumulator<?, ?>>();

	private boolean useUserClassLoader = false;

	// Removing this causes an EOFException in the RPC service. The RPC should
	// be improved in this regard (error message is very unspecific).
	public AccumulatorEvent() {
	}

	public AccumulatorEvent(JobID jobID,
			Map<String, Accumulator<?, ?>> accumulators,
			boolean useUserClassLoader) {
		this.accumulators = accumulators;
		this.jobID = jobID;
		this.useUserClassLoader = useUserClassLoader;
	}

	public JobID getJobID() {
		return this.jobID;
	}

	public Map<String, Accumulator<?, ?>> getAccumulators() {
		return this.accumulators;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeBoolean(this.useUserClassLoader);
		jobID.write(out);
		out.writeInt(accumulators.size());
		for (Map.Entry<String, Accumulator<?, ?>> entry : this.accumulators
				.entrySet()) {
			out.writeUTF(entry.getKey());
			out.writeUTF(entry.getValue().getClass().getName());
			entry.getValue().write(out);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void read(DataInputView in) throws IOException {
		this.useUserClassLoader = in.readBoolean();
		jobID = new JobID();
		jobID.read(in);
		int numberOfMapEntries = in.readInt();
		this.accumulators = new HashMap<String, Accumulator<?, ?>>(
				numberOfMapEntries);

		// Get user class loader. This is required at the JobManager, but not at
		// the
		// client.
		ClassLoader classLoader = null;
		if (this.useUserClassLoader) {
			classLoader = LibraryCacheManager.getClassLoader(jobID);
		} else {
			classLoader = this.getClass().getClassLoader();
		}

		for (int i = 0; i < numberOfMapEntries; i++) {
			String key = in.readUTF();

			final String valueType = in.readUTF();
			Class<Accumulator<?, ?>> valueClass = null;
			try {
				valueClass = (Class<Accumulator<?, ?>>) Class.forName(
						valueType, true, classLoader);
			} catch (ClassNotFoundException e) {
				throw new IOException(StringUtils.stringifyException(e));
			}

			Accumulator<?, ?> value = null;
			try {
				value = valueClass.newInstance();
			} catch (Exception e) {
				throw new IOException(StringUtils.stringifyException(e));
			}
			value.read(in);

			this.accumulators.put(key, value);
		}
	}
}
