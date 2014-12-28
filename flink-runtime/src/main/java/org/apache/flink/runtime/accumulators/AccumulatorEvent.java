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

package org.apache.flink.runtime.accumulators;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.util.InstantiationUtil;

/**
 * This class encapsulates a map of accumulators for a single job. It is used
 * for the transfer from TaskManagers to the JobManager and from the JobManager
 * to the Client.
 */
public class AccumulatorEvent implements Serializable {

	private static final long serialVersionUID = 8965894516006882735L;

	private JobID jobID;
	
	private Map<String, Accumulator<?, ?>> accumulators;
	
	// staging deserialized data until the classloader is available
	private byte[] serializedData;


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
			ByteArrayInputStream bais = new ByteArrayInputStream(serializedData);
			ObjectInputStream ios = null;
			try {
				ios = new ObjectInputStream(bais);
			} catch (IOException e) {
				throw new RuntimeException("Error while creating the object input stream.");
			}

			int numAccumulators = 0;
			try {
				numAccumulators = ios.readInt();
			} catch (IOException e) {
				throw new RuntimeException("Error while reading the number of serialized " +
						"accumulators.");
			}

			this.accumulators = new HashMap<String, Accumulator<?, ?>>(numAccumulators);

			for (int i = 0; i < numAccumulators; i++) {
				String accumulatorName;
				try {
					accumulatorName = ios.readUTF();
				} catch (IOException e) {
					throw new RuntimeException("Error while reading the "+ i +"th accumulator " +
							"name.");
				}

				String className;
				try {
					className = ios.readUTF();
				} catch (IOException e) {
					throw new RuntimeException("Error while reading the "+ i +"th accumulator " +
							"class name.");
				}
				Accumulator<?, ?> accumulator;

				try {
					@SuppressWarnings("unchecked")
					Class<? extends Accumulator<?, ?>> valClass = (Class<? extends Accumulator<?,
							?>>) Class.forName(className, true, loader);
					accumulator = InstantiationUtil.instantiate(valClass, Accumulator.class);
				} catch (ClassNotFoundException e) {
					throw new RuntimeException("Could not load user-defined class '" +
							className + "'.", e);
				} catch (ClassCastException e) {
					throw new RuntimeException("User-defined accumulator class is not an Accumulator sublass.");
				}

				try {
					accumulator.read(ios);
				} catch (IOException e) {
					throw new RuntimeException("Error while deserializing the user-defined aggregate class.", e);
				}

				accumulators.put(accumulatorName, accumulator);

			}

			try {
				ios.close();
			} catch (IOException e) {
				throw new RuntimeException("Error while closing the InputObjectStream.");
			}

			try {
				bais.close();
			} catch (IOException e) {
				throw new RuntimeException("Error while closing the ByteArrayInputStream.");
			}

		}

		return accumulators;
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException{
		out.writeObject(jobID);

		byte[] buffer = null;

		if(accumulators != null) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);

			oos.writeInt(accumulators.size());

			for (Map.Entry<String, Accumulator<?, ?>> entry : this.accumulators.entrySet()) {
				oos.writeUTF(entry.getKey());
				oos.writeUTF(entry.getValue().getClass().getName());

				entry.getValue().write(oos);
			}

			oos.flush();
			oos.close();
			baos.close();

			buffer = baos.toByteArray();
		}else if(serializedData != null){
			buffer = serializedData;
		}else{
			throw new RuntimeException("The AccumulatorEvent's accumulator is null and there is " +
					"no serialized data attached to it.");
		}

		out.writeInt(buffer.length);
		out.write(buffer);
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException,
			ClassNotFoundException{
		this.accumulators = null; // this makes sure we deserialize

		jobID = (JobID) in.readObject();

		int bufferLength = in.readInt();

		serializedData = new byte[bufferLength];

		in.readFully(serializedData);
	}
}
