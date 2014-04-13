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

import java.util.Iterator;

import eu.stratosphere.api.common.functions.GenericCoGrouper;
import eu.stratosphere.api.common.operators.base.CoGroupOperatorBase;
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.operators.Keys;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.util.Collector;

public class PlanUnwrappingCoGroupOperator<I1, I2, OUT, K> 
	extends CoGroupOperatorBase<GenericCoGrouper<Tuple2<K, I1>, Tuple2<K, I2>, OUT>>
	implements BinaryJavaPlanNode<Tuple2<K, I1>, Tuple2<K, I2>, OUT>
{

	private final TypeInformation<Tuple2<K, I1>> inTypeWithKey1;
	
	private final TypeInformation<Tuple2<K, I2>> inTypeWithKey2;
	
	private final TypeInformation<OUT> outType;

	public PlanUnwrappingCoGroupOperator(CoGroupFunction<I1, I2, OUT> udf, 
			Keys.SelectorFunctionKeys<I1, K> key1, Keys.SelectorFunctionKeys<I2, K> key2, String name,
			TypeInformation<OUT> type, TypeInformation<Tuple2<K, I1>> typeInfoWithKey1, TypeInformation<Tuple2<K, I2>> typeInfoWithKey2)
	{
		super(new TupleUnwrappingCoGrouper<I1, I2, OUT, K>(udf), key1.computeLogicalKeyPositions(), key2.computeLogicalKeyPositions(), name);
		this.outType = type;
		
		this.inTypeWithKey1 = typeInfoWithKey1;
		this.inTypeWithKey2 = typeInfoWithKey2;
	}
	

	@Override
	public TypeInformation<OUT> getReturnType() {
		return outType;
	}

	@Override
	public TypeInformation<Tuple2<K, I1>> getInputType1() {
		return inTypeWithKey1;
	}


	@Override
	public TypeInformation<Tuple2<K, I2>> getInputType2() {
		return inTypeWithKey2;
	}


	
	// --------------------------------------------------------------------------------------------
	
	public static final class TupleUnwrappingCoGrouper<I1, I2, OUT, K> extends WrappingFunction<CoGroupFunction<I1, I2, OUT>>
		implements GenericCoGrouper<Tuple2<K, I1>, Tuple2<K, I2>, OUT>
	{

		private static final long serialVersionUID = 1L;
		
		private TupleUnwrappingCoGrouper(CoGroupFunction<I1, I2, OUT> wrapped) {
			super(wrapped);
		}


		@Override
		public void coGroup(Iterator<Tuple2<K, I1>> records1, Iterator<Tuple2<K, I2>> records2, Collector<OUT> out) throws Exception {
			this.wrappedFunction.coGroup(new UnwrappingKeyIterator<K, I1>(records1), new UnwrappingKeyIterator<K, I2>(records2), out);
		}


		@Override
		public Tuple2<K, I1> combineFirst(Iterator<Tuple2<K, I1>> records) throws Exception {
			throw new UnsupportedOperationException();
		}

		@Override
		public Tuple2<K, I2> combineSecond(Iterator<Tuple2<K, I2>> records) throws Exception {
			throw new UnsupportedOperationException();
		}
	}
	
	public static class UnwrappingKeyIterator<K, I1> implements Iterator<I1> {

		private Iterator<Tuple2<K, I1>> outerIterator;
		I1 firstValue;
		
		public UnwrappingKeyIterator(Iterator<Tuple2<K, I1>> records1) {
			this.outerIterator = records1;
			this.firstValue = null;
		}
		
		public UnwrappingKeyIterator(Iterator<Tuple2<K, I1>> records1, I1 firstValue ) {
			this.outerIterator = records1;
			this.firstValue = firstValue;
		}
		
		@Override
		public boolean hasNext() {
			return firstValue != null || outerIterator.hasNext();
		}

		@Override
		public I1 next() {
			if(firstValue != null) {
				firstValue = null;
				return firstValue;
			}
			return outerIterator.next().getField(1);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}

	
	public static class UnwrappingKeyCollector<K, I1> implements Collector<I1> {
		
		Collector<Tuple2<K, I1>> outerCollector;
		K key;

		public UnwrappingKeyCollector(Collector<Tuple2<K, I1>> outerCollector, K key) {
			this.outerCollector = outerCollector;
			this.key = key;
		}
		
		@Override
		public void collect(I1 record) {
			this.outerCollector.collect(new Tuple2<K, I1>(key, record));
		}

		@Override
		public void close() {
			this.outerCollector.close();
		}
	}
}
