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

package eu.stratosphere.pact.runtime.iterative.event;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import eu.stratosphere.api.common.aggregators.Aggregator;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.core.memory.InputViewDataInputStreamWrapper;
import eu.stratosphere.core.memory.OutputViewDataOutputStreamWrapper;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.InstantiationUtil;

public abstract class IterationEventWithAggregators extends AbstractTaskEvent {
	
	protected static final String[] NO_STRINGS = new String[0];
	protected static final Value[] NO_VALUES = new Value[0];
	
	private String[] aggNames;
	
	private String[] classNames;
	private byte[][] serializedData;

	private Value[] aggregates;

	protected IterationEventWithAggregators() {
		this.aggNames = NO_STRINGS;
		this.aggregates = NO_VALUES;
	}

	protected IterationEventWithAggregators(String aggregatorName, Value aggregate) {
		if (aggregatorName == null || aggregate == null) {
			throw new NullPointerException();
		}
		
		this.aggNames = new String[] { aggregatorName };
		this.aggregates = new Value[] { aggregate };
	}
	
	protected IterationEventWithAggregators(Map<String, Aggregator<?>> aggregators) {
		int num = aggregators.size();
		if (num == 0) {
			this.aggNames = NO_STRINGS;
			this.aggregates = NO_VALUES;
		} else {
			this.aggNames = new String[num];
			this.aggregates = new Value[num];
			
			int i = 0;
			for (Map.Entry<String, Aggregator<?>> entry : aggregators.entrySet()) {
				this.aggNames[i] = entry.getKey();
				this.aggregates[i] = entry.getValue().getAggregate();
				i++;
			}
		}
	}
	
	public String[] getAggregatorNames() {
		return this.aggNames;
	}

	public Value[] getAggregates(ClassLoader classResolver) {
		if (aggregates == null) {
			// we have read the binary data, but not yet turned into the objects
			final int num = aggNames.length;
			aggregates = new Value[num];
			for (int i = 0; i < num; i++) {
				Value v;
				try {
					Class<? extends Value> valClass = Class.forName(classNames[i], true, classResolver).asSubclass(Value.class);
					v = InstantiationUtil.instantiate(valClass, Value.class);
				}
				catch (ClassNotFoundException e) {
					throw new RuntimeException("Could not load user-defined class '" + classNames[i] + "'.", e);
				}
				catch (ClassCastException e) {
					throw new RuntimeException("User-defined aggregator class is not a value sublass.");
				}
				
				DataInputStream in = new DataInputStream(new ByteArrayInputStream(serializedData[i]));
				try {
					v.read(new InputViewDataInputStreamWrapper(in));
					in.close();
				} catch (IOException e) {
					throw new RuntimeException("Error while deserializing the user-defined aggregate class.", e);
				}
				
				aggregates[i] = v;
			}
		}
		
		return this.aggregates;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		int num = this.aggNames.length;
		out.writeInt(num);
		
		ByteArrayOutputStream boas = new ByteArrayOutputStream();
		DataOutputStream bufferStream = new DataOutputStream(boas);
		
		for (int i = 0; i < num; i++) {
			// aggregator name and type
			out.writeUTF(this.aggNames[i]);
			out.writeUTF(this.aggregates[i].getClass().getName());
			
			// aggregator value indirect as a byte array
			this.aggregates[i].write(new OutputViewDataOutputStreamWrapper(bufferStream));
			bufferStream.flush();
			byte[] bytes = boas.toByteArray();
			out.writeInt(bytes.length);
			out.write(bytes);
			boas.reset();
		}
		bufferStream.close();
		boas.close();
	}

	@Override
	public void read(DataInputView in) throws IOException {
		int num = in.readInt();
		if (num == 0) {
			this.aggNames = NO_STRINGS;
			this.aggregates = NO_VALUES;
		} else {
			if (this.aggNames == null || num > this.aggNames.length) {
				this.aggNames = new String[num];
			}
			if (this.classNames == null || num > this.classNames.length) {
				this.classNames = new String[num];
			}
			if (this.serializedData == null || num > this.serializedData.length) {
				this.serializedData = new byte[num][];
			}

			for (int i = 0; i < num; i++) {
				this.aggNames[i] = in.readUTF();
				this.classNames[i] = in.readUTF();
				
				int len = in.readInt();
				byte[] data = new byte[len];
				this.serializedData[i] = data;
				in.readFully(data);
				
			}
			
			this.aggregates = null;
		}
	}
}
