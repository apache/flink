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
package eu.stratosphere.sopremo.execution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.pact.SopremoUtil;

/**
 * Represents a request to a {@link MeteorExecutor} that encapsulates the query and optional settings.
 * 
 * @author Arvid Heise
 */
public class ExecutionRequest implements IOReadableWritable {
	private SopremoPlan query;

	private ExecutionMode mode = ExecutionMode.RUN;

	private transient byte[] planBuffer = null;

	private transient List<String> requiredPackages;

	/**
	 * Initializes ExecutionRequest with the given query.
	 * 
	 * @param query
	 *        the query to execute
	 */
	public ExecutionRequest(SopremoPlan query) {
		this.query = query;
	}

	/**
	 * Needed for deserialization.
	 */
	public ExecutionRequest() {
	}

	public ExecutionMode getMode() {
		return this.mode;
	}

	/**
	 * Returns the query.
	 * 
	 * @return the query
	 */
	public SopremoPlan getQuery() {
		if (this.query != null || this.planBuffer == null)
			return this.query;

		final JobID dummId = new JobID();
		try {
			LibraryCacheManager.register(dummId,
				this.requiredPackages.toArray(new String[this.requiredPackages.size()]));
			this.query = SopremoUtil.byteArrayToSerializable(this.planBuffer, SopremoPlan.class, 
				LibraryCacheManager.getClassLoader(dummId));
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				LibraryCacheManager.unregister(dummId);
			} catch (IOException e) {
			}
		}
		return this.query;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException {
		this.mode = ExecutionMode.values()[in.readInt()];

		this.requiredPackages = new ArrayList<String>();
		for (int count = in.readInt(); count > 0; count--)
			this.requiredPackages.add(in.readUTF());
		this.query = null;

		this.planBuffer = new byte[in.readInt()];
		in.readFully(this.planBuffer);
	}

	public void setMode(ExecutionMode mode) {
		if (mode == null)
			throw new NullPointerException("mode must not be null");

		this.mode = mode;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.mode.ordinal());

		final List<String> requiredPackages = this.query.getRequiredPackages();
		out.writeInt(requiredPackages.size());
		for (String packageName : requiredPackages)
			out.writeUTF(packageName);

		final byte[] planBuffer = SopremoUtil.serializableToByteArray(this.query);
		out.writeInt(planBuffer.length);
		out.write(planBuffer);
	}

	public enum ExecutionMode {
		RUN, RUN_WITH_STATISTICS;
	}

}
