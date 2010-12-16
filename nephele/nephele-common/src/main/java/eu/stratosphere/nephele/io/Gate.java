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

package eu.stratosphere.nephele.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * In Nephele a gate represents the connection between a user program and the processing framework. A gate
 * must be connected to exactly one record reader/writer and to at least one channel. The <code>Gate</code> class itself
 * is abstract. A gate automatically created for every record reader/writer in the user program. A gate can only be used
 * to transport one specific type of records.
 * 
 * @author warneke
 * @param <T>
 *        the record type to be transported from this gate
 */
public abstract class Gate<T extends Record> implements IOReadableWritable {

	/**
	 * The class of the record transported through this gate.
	 */
	protected RecordDeserializer<T> deserializer;

	/**
	 * The id of the job this gate belongs to (required to find the appropriate class loader).
	 */
	private JobID jobID;

	/**
	 * The index of the gate in the list of available input/output gates.
	 */
	protected int index;

	/**
	 * Sets the ID of the job this gate belongs to.
	 * 
	 * @param jobID
	 *        the ID of the job this gate belongs to
	 */
	public void setJobID(JobID jobID) {
		this.jobID = jobID;
	}

	/**
	 * Returns the ID of the job this gate belongs to.
	 * 
	 * @return the ID of the job this gate belongs to
	 */
	public JobID getJobID() {
		return this.jobID;
	}

	/**
	 * Returns the type of record that can be transported through this gate.
	 * 
	 * @return the type of record that can be transported through this gate
	 */
	public Class<T> getType() {
		return deserializer.getRecordType();
	}

	/**
	 * Returns the index that has been assigned to the gate upon initialization.
	 * 
	 * @return the index that has been assigned to the gate upon initialization.
	 */
	public int getIndex() {
		return this.index;
	}

	/**
	 * Sets the type class of the records that can be transported through this gate.
	 * 
	 * @param typeClass
	 *        the type class of the records that can be transported throught this gate.
	 */
	protected void setDeserializer(RecordDeserializer<T> deserializer) {
		this.deserializer = deserializer;
	}

	/**
	 * Checks if the gate is closed. The gate is closed if alls this associated channels are closed.
	 * 
	 * @return <code>true</code> if the gate is closed, <code>false</code> otherwise
	 * @throws IOException
	 *         thrown if any error occurred while closing the gate
	 */
	public abstract boolean isClosed() throws IOException;

	/**
	 * Checks if the considered gate is an input gate.
	 * 
	 * @return <code>true</code> if the considered gate is an input gate, <code>false</code> if it is an output gate
	 */
	public abstract boolean isInputGate();

	// TODO: See if type safety can be improved here
	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void read(DataInput in) throws IOException {

		// Read the job ID
		this.jobID = new JobID();
		this.jobID.read(in);

		// Now get the appropriate class loader for this task
		final ClassLoader cl = LibraryCacheManager.getClassLoader(this.jobID);

		// Read class
		final String deserializerClassName = StringRecord.readString(in);
		try {
			Class<RecordDeserializer<T>> deserializerClass = (Class<RecordDeserializer<T>>) Class.forName(
				deserializerClassName, true, cl);
			deserializer = deserializerClass.newInstance();
			// Important to set the correct class loader
			deserializer.setClassLoader(cl);
			deserializer.read(in);
		} catch (Exception e) {
			throw new IOException(StringUtils.stringifyException(e));
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		if (this.jobID == null) {
			throw new IOException("Job vertex ID is null for " + this);
		}

		// Write out the job vertex ID
		this.jobID.write(out);

		// Write class
		StringRecord.writeString(out, this.deserializer.getClass().getName());
		deserializer.write(out);
	}

	@Override
	public String toString() {
		return "Gate " + this.index;
	}

}
