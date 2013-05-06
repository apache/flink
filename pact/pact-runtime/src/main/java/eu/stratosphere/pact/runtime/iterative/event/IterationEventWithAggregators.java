/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.iterative.event;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.pact.common.stubs.aggregators.Aggregator;
import eu.stratosphere.pact.common.type.Value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public abstract class IterationEventWithAggregators extends AbstractTaskEvent {
	
	protected static final String[] NO_STRINGS = new String[0];
	protected static final Value[] NO_VALUES = new Value[0];
	
	private String[] aggNames;

	private Value[] aggregates;

	protected IterationEventWithAggregators() {
		this.aggNames = NO_STRINGS;
		this.aggregates = NO_VALUES;
	}

	protected IterationEventWithAggregators(String aggregatorName, Value aggregate) {
		if (aggregatorName == null || aggregate == null)
			throw new NullPointerException();
		
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

	public Value[] getAggregates() {
		return this.aggregates;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		int num = this.aggNames.length;
		out.writeInt(num);
		for (int i = 0; i < num; i++) {
			out.writeUTF(this.aggNames[i]);
			out.writeUTF(this.aggregates[i].getClass().getName());
			this.aggregates[i].write(out);
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		int num = in.readInt();
		if (num == 0) {
			this.aggNames = NO_STRINGS;
			this.aggregates = NO_VALUES;
		} else {
			if (this.aggNames == null || num > this.aggNames.length) {
				this.aggNames = new String[num];
			}
			if (this.aggregates == null || num > this.aggregates.length) {
				this.aggregates = new Value[num];
			}

			for (int i = 0; i < num; i++) {
				this.aggNames[i] = in.readUTF();
				String classname = in.readUTF();
				try {
					this.aggregates[i] = Class.forName(classname).asSubclass(Value.class).newInstance();
				} catch (Exception e) {
					throw new IOException(e);
				}
				aggregates[i].read(in);
			}
		}
	}
}
