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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * The OutputCollector collects {@link PactRecord}s, and emits the pair to a set of Nephele {@link RecordWriter}s.
 * The OutputCollector tracks to which writers a deep-copy must be given and which not.
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 */
public class OutputCollector implements Collector
{	
	// list of writers
	protected final List<RecordWriter<PactRecord>> writers; 
	
	// bit mask for copy flags
	protected int fwdCopyFlags;

	/**
	 * Initializes the output collector with no writers.
	 */
	public OutputCollector() {
		this.writers = new ArrayList<RecordWriter<PactRecord>>();
		this.fwdCopyFlags = 0;
	}
	
	/**
	 * Initializes the output collector with a set of writers. 
	 * To specify for a writer that it must be fed with a deep-copy, set the bit in the copy flag bit mask to 1 that 
	 * corresponds to the position of the writer within the {@link List}.
	 * 
	 * @param writers List of all writers.
	 * @param fwdCopyFlags Bit mask that specifies which writer is fed with deep-copies.
	 */
	public OutputCollector(List<RecordWriter<PactRecord>> writers, int fwdCopyFlags) {
		
		this.writers = writers;
		this.fwdCopyFlags = fwdCopyFlags;		
	}
	
	/**
	 * Adds a writer to the OutputCollector.
	 * 
	 * @param writer The writer to add.
	 * @param fwdCopy Set true if writer requires a deep-copy. Set to false otherwise.
	 */
	public void addWriter(RecordWriter<PactRecord> writer, boolean fwdCopy) {
		this.writers.add(writer);
		if (fwdCopy) {
			this.fwdCopyFlags |= 0x1 << (this.writers.size() - 1);
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
			if (fwdCopyFlags == 0) {
				for (int i = 0; i < writers.size(); i++) {
					writers.get(i).emit(record);
				}
			}
			else {
				for (int i = 0; i < writers.size(); i++) {
					if (((fwdCopyFlags >> i) & 0x1) != 0) {
						PactRecord copy = record.createCopy();
						writers.get(i).emit(copy);
					}
					else {
						writers.get(i).emit(record);	
					}
				}
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
	public List<RecordWriter<PactRecord>> getWriters() {
		return Collections.unmodifiableList(writers);
	}
}
