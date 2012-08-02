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

import eu.stratosphere.nephele.io.IOReadableWritable;
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
		return this.query;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException {
		this.query = SopremoUtil.deserializeObject(in, SopremoPlan.class);
		this.mode = ExecutionMode.values()[in.readInt()];
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
		SopremoUtil.serializeObject(out, this.query);
		out.writeInt(this.mode.ordinal());
	}

	public enum ExecutionMode {
		RUN, RUN_WITH_STATISTICS;
	}

}
