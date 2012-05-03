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

package eu.stratosphere.pact.runtime.task.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import eu.stratosphere.nephele.io.AbstractRecordWriter;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * The OutputCollector collects {@link PactRecord}s, and emits the pair to a set of Nephele {@link RecordWriter}s.
 * The OutputCollector tracks to which writers a deep-copy must be given and which not.
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 */
public class OutputCollector implements Collector<PactRecord>
{	
	// list of writers
	protected AbstractRecordWriter<PactRecord>[] writers; 

	/**
	 * Initializes the output collector with no writers.
	 */
	public OutputCollector() {
	}
	
	/**
	 * Initializes the output collector with a set of writers. 
	 * To specify for a writer that it must be fed with a deep-copy, set the bit in the copy flag bit mask to 1 that 
	 * corresponds to the position of the writer within the {@link List}.
	 * 
	 * @param writers List of all writers.
	 */
	@SuppressWarnings("unchecked")
	public OutputCollector(List<RecordWriter<PactRecord>> writers) {
		
		this.writers = (RecordWriter<PactRecord>[]) writers.toArray(new RecordWriter[writers.size()]);
	}
	
	/**
	 * Adds a writer to the OutputCollector.
	 * 
	 * @param writer The writer to add.
	 */

	@SuppressWarnings("unchecked")
	public void addWriter(AbstractRecordWriter<PactRecord> writer)
	{
		// avoid using the array-list here to reduce one level of object indirection
		if (this.writers == null) {
			this.writers = new AbstractRecordWriter[] {writer};
		}
		else {
			AbstractRecordWriter<PactRecord>[] ws = new AbstractRecordWriter[this.writers.length + 1];
			System.arraycopy(this.writers, 0, ws, 0, this.writers.length);
			ws[this.writers.length] = writer;
			this.writers = ws;
		}
	}

	/**
	 * Collects a {@link PactRecord}, and emits it to all writers.
	 * Writers which require a deep-copy are fed with a copy.
	 */
	@Override
	public void collect(PactRecord record)
	{
		try {
			for (int i = 0; i < writers.length; i++) {
				this.writers[i].emit(record);	
			}
		}
		catch (IOException e) {
			throw new RuntimeException("Emitting the record caused an I/O exception: " + e.getMessage(), e);
		}
		catch (InterruptedException e) {
			throw new RuntimeException("Emitting the record was interrupted: " + e.getMessage(), e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stub.Collector#close()
	 */
	@Override
	public void close() {
	}

	/**
	 * List of writers that are associated with this output collector
	 * @return list of writers
	 */
	public List<AbstractRecordWriter<PactRecord>> getWriters() {
		return Collections.unmodifiableList(Arrays.asList(writers));
	}
}
