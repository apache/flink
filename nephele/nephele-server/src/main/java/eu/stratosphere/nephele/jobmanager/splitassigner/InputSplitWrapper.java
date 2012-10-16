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

import java.io.IOException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.template.InputSplit;

/**
 * An input split wrapper object wraps an input split for RPC calls. In particular, the input split wrapper ensures that
 * the right class loader is used to instantiate the wrapped input split object.
 * 
 * @author warneke
 */
public final class InputSplitWrapper implements KryoSerializable {

	/**
	 * The ID of the job this input split belongs to.
	 */
	private JobID jobID;

	/**
	 * The wrapped input split.
	 */
	private InputSplit inputSplit;

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
		this.jobID = null;
		this.inputSplit = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final Kryo kryo, final Output output) {

		// Write the job ID
		kryo.writeObject(output, this.jobID);

		if (this.inputSplit == null) {
			output.writeBoolean(false);
		} else {

			output.writeBoolean(true);

			// Write the name of the class
			output.writeString(this.inputSplit.getClass().getName());
			kryo.writeObject(output, this.inputSplit);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void read(final Kryo kryo, final Input input) {

		// Read the job ID
		this.jobID = kryo.readObject(input, JobID.class);

		if (input.readBoolean()) {

			// Find class loader for this job
			ClassLoader cl = null;
			try {
				cl = LibraryCacheManager.getClassLoader(this.jobID);
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}

			// Read the name of the class
			final String className = input.readString();

			// Try to locate the class using the job's class loader
			Class<? extends InputSplit> splitClass = null;
			try {
				splitClass = (Class<? extends InputSplit>) Class.forName(className, true, cl);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}

			this.inputSplit = kryo.readObject(input, splitClass);
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
