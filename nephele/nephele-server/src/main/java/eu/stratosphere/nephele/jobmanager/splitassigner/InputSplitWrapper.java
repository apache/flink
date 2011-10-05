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

package eu.stratosphere.nephele.jobmanager.splitassigner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * An input split wrapper object wraps an input split for RPC calls. In particular, the input split wrapper ensures that
 * the right class loader is used to instantiate the wrapped input split object.
 * 
 * @author warneke
 */
public final class InputSplitWrapper implements IOReadableWritable {

	/**
	 * The ID of the job this input split belongs to.
	 */
	private JobID jobID;

	/**
	 * The wrapped input split.
	 */
	private InputSplit inputSplit = null;

	/**
	 * Constructs a new input split wrapper.
	 * 
	 * @param jobID
	 *        the ID of the job the input split belongs to
	 * @param inputSplit
	 *        the input split to be wrapped
	 */
	public InputSplitWrapper(final JobID jobID, final InputSplit inputSplit) {

		if (jobID == null) {
			throw new IllegalArgumentException("Argument jobID must not be null");
		}

		this.jobID = jobID;
		this.inputSplit = inputSplit;
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public InputSplitWrapper() {
		this.jobID = new JobID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		// Write the job ID
		this.jobID.write(out);

		if (this.inputSplit == null) {
			out.writeBoolean(false);
		} else {

			out.writeBoolean(true);

			// Write the name of the class
			StringRecord.writeString(out, this.inputSplit.getClass().getName());

			// Write out the input split itself
			this.inputSplit.write(out);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void read(final DataInput in) throws IOException {

		// Read the job ID
		this.jobID.read(in);

		if (in.readBoolean()) {

			// Find class loader for this job
			final ClassLoader cl = LibraryCacheManager.getClassLoader(this.jobID);
			if (cl == null) {
				throw new IOException("Cannot find class loader for job " + this.jobID);
			}

			// Read the name of the class
			final String className = StringRecord.readString(in);

			// Try to locate the class using the job's class loader
			Class<? extends InputSplit> splitClass = null;
			try {
				splitClass = (Class<? extends InputSplit>) Class.forName(className, true, cl);
			} catch (ClassNotFoundException e) {
				throw new IOException(StringUtils.stringifyException(e));
			}

			try {
				this.inputSplit = splitClass.newInstance();
			} catch (InstantiationException e) {
				throw new IOException(StringUtils.stringifyException(e));
			} catch (IllegalAccessException e) {
				throw new IOException(StringUtils.stringifyException(e));
			}

			// Read the input split itself
			this.inputSplit.read(in);
		} else {
			this.inputSplit = null;
		}
	}

	/**
	 * Returns the wrapped input split. The wrapped input split may also be <code>null</code> in case no more input
	 * splits shall be consumed by the requesting task.
	 * 
	 * @return the wrapped input split, possibly <code>null</code>
	 */
	public InputSplit getInputSplit() {

		return this.inputSplit;
	}
}
