/**
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

package org.apache.flink.runtime.accumulators;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.InputViewDataInputStreamWrapper;
import org.apache.flink.core.memory.OutputViewDataOutputStreamWrapper;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.util.InstantiationUtil;

/**
 * This class encapsulates a map of accumulators for a single job. It is used
 * for the transfer from TaskManagers to the JobManager and from the JobManager
 * to the Client.
 */
public class AccumulatorEvent implements IOReadableWritable {

	private JobID jobID;
	
	private Map<String, Accumulator<?, ?>> accumulators;
	
	// staging deserialized data until the classloader is available
	private String[] accNames;
	private String[] classNames;
	private byte[][] serializedData;


	// Removing this causes an EOFException in the RPC service. The RPC should
	// be improved in this regard (error message is very unspecific).
	public AccumulatorEvent() {
		this.accumulators = Collections.emptyMap();
	}

	public AccumulatorEvent(JobID jobID, Map<String, Accumulator<?, ?>> accumulators) {
		this.accumulators = accumulators;
		this.jobID = jobID;
	}

	public JobID getJobID() {
		return this.jobID;
	}

	public Map<String, Accumulator<?, ?>> getAccumulators(ClassLoader loader) {
		if (loader == null) {
			throw new NullPointerException();
		}
		
		if (this.accumulators == null) {
			// deserialize
			// we have read the binary data, but not yet turned into the objects
			final int num = accNames.length;
			this.accumulators = new HashMap<String, Accumulator<?,?>>(num);
			for (int i = 0; i < num; i++) {
				Accumulator<?, ?> acc;
				try {
					@SuppressWarnings("unchecked")
					Class<? extends Accumulator<?, ?>> valClass = (Class<? extends Accumulator<?, ?>>) Class.forName(classNames[i], true, loader);
					acc = InstantiationUtil.instantiate(valClass, Accumulator.class);
				}
				catch (ClassNotFoundException e) {
					throw new RuntimeException("Could not load user-defined class '" + classNames[i] + "'.", e);
				}
				catch (ClassCastException e) {
					throw new RuntimeException("User-defined accumulator class is not an Accumulator sublass.");
				}
				
				DataInputStream in = new DataInputStream(new ByteArrayInputStream(serializedData[i]));
				try {
					acc.read(new InputViewDataInputStreamWrapper(in));
					in.close();
				} catch (IOException e) {
					throw new RuntimeException("Error while deserializing the user-defined aggregate class.", e);
				}
				
				accumulators.put(accNames[i], acc);
			}
			
			// reset the serialized data
			this.accNames = null;
			this.classNames = null;
			this.serializedData = null;
		}
		
		return this.accumulators;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		jobID.write(out);
		out.writeInt(accumulators.size());
		
		if (accumulators.size() > 0) {
			ByteArrayOutputStream boas = new ByteArrayOutputStream();
			DataOutputStream bufferStream = new DataOutputStream(boas);
			
			for (Map.Entry<String, Accumulator<?, ?>> entry : this.accumulators.entrySet()) {
				
				// write accumulator name
				out.writeUTF(entry.getKey());
				
				// write type class
				out.writeUTF(entry.getValue().getClass().getName());
				
				entry.getValue().write(new OutputViewDataOutputStreamWrapper(bufferStream));
				bufferStream.flush();
				byte[] bytes = boas.toByteArray();
				out.writeInt(bytes.length);
				out.write(bytes);
				boas.reset();
			}
			bufferStream.close();
			boas.close();
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.accumulators = null; // this makes sure we deserialize
		
		jobID = new JobID();
		jobID.read(in);
		
		int numberOfMapEntries = in.readInt();
		this.accNames = new String[numberOfMapEntries];
		this.classNames = new String[numberOfMapEntries];
		this.serializedData = new byte[numberOfMapEntries][];

		for (int i = 0; i < numberOfMapEntries; i++) {
			this.accNames[i] = in.readUTF();
			this.classNames[i] = in.readUTF();
			
			int len = in.readInt();
			byte[] data = new byte[len];
			this.serializedData[i] = data;
			in.readFully(data);
		}
	}
}
