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

package org.apache.flink.api.java.operators.translation;

import java.util.Iterator;

import org.apache.flink.api.common.functions.FlatCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.java.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.TypeInformation;
import org.apache.flink.util.Collector;

/**
 * A reduce operator that takes 2-tuples (key-value pairs), and applies the group reduce operation only
 * on the unwrapped values.
 */
public class PlanUnwrappingReduceGroupOperator<IN, OUT, K> extends GroupReduceOperatorBase<Tuple2<K, IN>, OUT, GroupReduceFunction<Tuple2<K, IN>,OUT>> {

	public PlanUnwrappingReduceGroupOperator(GroupReduceFunction<IN, OUT> udf, Keys.SelectorFunctionKeys<IN, K> key, String name,
			TypeInformation<OUT> outType, TypeInformation<Tuple2<K, IN>> typeInfoWithKey, boolean combinable)
	{
		super(combinable ? new TupleUnwrappingFlatCombinableGroupReducer<IN, OUT, K>((RichGroupReduceFunction) udf) : new TupleUnwrappingNonCombinableGroupReducer<IN, OUT, K>(udf),
				new UnaryOperatorInformation<Tuple2<K, IN>, OUT>(typeInfoWithKey, outType), key.computeLogicalKeyPositions(), name);
		
		super.setCombinable(combinable);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@RichGroupReduceFunction.Combinable
	public static final class TupleUnwrappingFlatCombinableGroupReducer<IN, OUT, K> extends WrappingFunction<RichGroupReduceFunction<IN, OUT>>
		implements GroupReduceFunction<Tuple2<K, IN>, OUT>, FlatCombineFunction<Tuple2<K, IN>>
	{

		private static final long serialVersionUID = 1L;
		
		private TupleUnwrappingIterator<IN, K> iter;
		private TupleWrappingCollector<IN, K> coll; 
		
		private TupleUnwrappingFlatCombinableGroupReducer(RichGroupReduceFunction<IN, OUT> wrapped) {
			super(wrapped);
			this.iter = new TupleUnwrappingIterator<IN, K>();
			this.coll = new TupleWrappingCollector<IN, K>(this.iter);
		}


		@Override
		public void reduce(Iterator<Tuple2<K, IN>> values, Collector<OUT> out) throws Exception {
			iter.set(values);
			this.wrappedFunction.reduce(iter, out);
		}

		@Override
		public void combine(Iterator<Tuple2<K, IN>> values, Collector<Tuple2<K, IN>> out) throws Exception {
				iter.set(values);
				coll.set(out);
				this.wrappedFunction.combine(iter, coll);
		}
		
		@Override
		public String toString() {
			return this.wrappedFunction.toString();
		}
	}
	
	public static final class TupleUnwrappingNonCombinableGroupReducer<IN, OUT, K> extends WrappingFunction<GroupReduceFunction<IN, OUT>>
		implements GroupReduceFunction<Tuple2<K, IN>, OUT>
	{
	
		private static final long serialVersionUID = 1L;
		
		private final TupleUnwrappingIterator<IN, K> iter; 
		
		private TupleUnwrappingNonCombinableGroupReducer(GroupReduceFunction<IN, OUT> wrapped) {
			super(wrapped);
			this.iter = new TupleUnwrappingIterator<IN, K>();
		}
	
	
		@Override
		public void reduce(Iterator<Tuple2<K, IN>> values, Collector<OUT> out) throws Exception {
			iter.set(values);
			this.wrappedFunction.reduce(iter, out);
		}
		
		@Override
		public String toString() {
			return this.wrappedFunction.toString();
		}
	}
}
