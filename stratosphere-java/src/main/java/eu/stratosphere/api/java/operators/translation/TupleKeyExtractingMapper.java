/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.operators.translation;

import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple2;


public final class TupleKeyExtractingMapper<T, K> extends MapFunction<T, Tuple2<K, T>> {
	
	private static final long serialVersionUID = 1L;
	
	private final int pos;
	
	private final Tuple2<K, T> tuple = new Tuple2<K, T>();
	
	
	public TupleKeyExtractingMapper(int pos) {
		this.pos = pos;
	}
	
	
	@Override
	public Tuple2<K, T> map(T value) throws Exception {
		
		Tuple v = (Tuple) value;
		
		K key = v.getField(pos);
		tuple.f0 = key;
		tuple.f1 = value;
		
		return tuple;
	}
}
