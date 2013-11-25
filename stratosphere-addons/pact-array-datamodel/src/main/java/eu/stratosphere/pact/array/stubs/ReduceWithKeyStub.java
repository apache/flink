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

package eu.stratosphere.pact.array.stubs;

import java.lang.reflect.Method;
import java.util.Iterator;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.CopyableValue;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.generic.stub.GenericReducer;

/**
 * The ReduceStub must be extended to provide a reducer implementation which is called by a Reduce PACT.
 * By definition, the Reduce PACT calls the reduce implementation once for each distinct key and all records
 * that come with that key. For details on the Reduce PACT read the documentation of the PACT programming model.
 * <p>
 * The ReduceStub extension must be parameterized with the types of its input keys.
 * <p>
 * For a reduce implementation, the <code>reduce()</code> method must be implemented.
 */
public abstract class ReduceWithKeyStub extends AbstractArrayModelStub implements GenericReducer<Value[], Value[]> {
	
	public abstract void reduce(Value key, Iterator<Value[]> records, Collector<Value[]> out);
	
	public void combine(Value key, Iterator<Value[]> records, Collector<Value[]> out) {
		// to be implemented, if the reducer should use a combiner. Note that the combining method
		// is only used, if the stub class is further annotated with the annotation
		// @ReduceContract.Combinable
		reduce(key, records, out);
	}
	
	// --------------------------------------------------------------------------------------------
	
	private OneHeadIterator iter; 
	
	private Value keyVal;
	
	private int keyIndex;

	@Override
	@SuppressWarnings("unchecked")
	public final void reduce(Iterator<Value[]> records, Collector<Value[]> out) throws Exception {
		final Value[] first = records.next();
		final Value key = this.keyVal;
		((CopyableValue<Value>) first[this.keyIndex]).copyTo(key);
		this.iter.set(first, records);
		reduce(key, this.iter, out);
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public final void combine(Iterator<Value[]> records, Collector<Value[]> out) throws Exception {
		final Value[] first = records.next();
		final Value key = this.keyVal;
		((CopyableValue<Value>) first[this.keyIndex]).copyTo(key);
		this.iter.set(first, records);
		combine(key, this.iter, out);
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static final String KEY_INDEX_PARAM_KEY = "reduceWithKey.key-pos";
	
	public static final String KEY_TYPE_PARAM_KEY = "reduceWithKey.key-type";
	
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		this.keyIndex = parameters.getInteger(KEY_INDEX_PARAM_KEY, -1);
		if (this.keyIndex < 0)
			throw new Exception("Invalid setup for ReduceWithKey: Key position has not been encoded in the config.");
		
		final Class<? extends Value>[] types = getDataTypes(0);
		if (types == null) {
			throw new Exception("Data types of the function's input could not be determined.");
		}
		if (this.keyIndex >= types.length) {
			throw new Exception("The specified position of the key is out of the range for the data types.");
		}
		this.keyVal = InstantiationUtil.instantiate(types[this.keyIndex], Value.class);
		if (!(this.keyVal instanceof CopyableValue)) {
			throw new Exception("Invalid setup for ReduceWithKey: Key type must implement " + CopyableValue.class.getName());
		}
	}
	
	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.array.stubs.AbstractArrayModelStub#getUDFMethod()
	 */
	@Override
	public final Method getUDFMethod() {
		try {
			return getClass().getMethod("reduce", Value.class, Iterator.class, Collector.class);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final class OneHeadIterator implements Iterator<Value[]> {

		private Value[] head;
		
		private Iterator<Value[]> source;
		
		private void set(Value[] head, Iterator<Value[]> source) {
			this.head = head;
			this.source = source;
		}
		
		@Override
		public boolean hasNext() {
			return this.head != null || this.source.hasNext();
		}

		@Override
		public Value[] next() {
			if (this.head != null) {
				Value[] tmp = this.head;
				this.head = null;
				return tmp;
			} else {
				return this.source.next();
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
