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
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.Reference;

public class PlanCogroupOperator<IN1, IN2, OUT> 
	extends CoGroupOperatorBase<GenericCoGrouper<Reference<IN1>, Reference<IN2>, Reference<OUT>>>
	implements BinaryJavaPlanNode<IN1, IN2, OUT> {
	
	private final TypeInformation<IN1> inType1;
	private final TypeInformation<IN2> inType2;
	private final TypeInformation<OUT> outType;

	public PlanCogroupOperator(
			CoGroupFunction<IN1, IN2, OUT> udf,
			int[] keyPositions1, int[] keyPositions2, String name, TypeInformation<IN1> inType1, TypeInformation<IN2> inType2, TypeInformation<OUT> outType) {
		super(new ReferenceWrappingCogrouper<IN1, IN2, OUT>(udf), keyPositions1, keyPositions2, name);
		
		this.inType1 = inType1;
		this.inType2 = inType2;
		this.outType = outType;
	}
	
	public static final class ReferenceWrappingCogrouper<IN1, IN2, OUT> 
	extends WrappingFunction<CoGroupFunction<IN1, IN2, OUT>>
	implements GenericCoGrouper<Reference<IN1>, Reference<IN2>, Reference<OUT>>
	{
		
		private static final long serialVersionUID = 1L;
		
		protected ReferenceWrappingCogrouper(
				CoGroupFunction<IN1, IN2, OUT> wrappedFunction) {
			super(wrappedFunction);
		}
		
		@Override
		public void coGroup(final Iterator<Reference<IN1>> records1,
				final Iterator<Reference<IN2>> records2, final Collector<Reference<OUT>> out)
				throws Exception {
			
			this.wrappedFunction.coGroup(new UnwrappingIterator<IN1>(records1), new UnwrappingIterator<IN2>(records2), new UnwrappingCollector<OUT>(out));
			
		}


		@Override
		public void combineFirst(Iterator<Reference<IN1>> records,
				Collector<Reference<IN1>> out) throws Exception {
			
			this.wrappedFunction.combineFirst(new UnwrappingIterator<IN1>(records), new UnwrappingCollector<IN1>(out));
		}


		@Override
		public void combineSecond(Iterator<Reference<IN2>> records,
				Collector<Reference<IN2>> out) throws Exception {

			this.wrappedFunction.combineSecond(new UnwrappingIterator<IN2>(records), new UnwrappingCollector<IN2>(out));
		}
	}

	@Override
	public TypeInformation<OUT> getReturnType() {
		return this.outType;
	}


	@Override
	public TypeInformation<IN1> getInputType1() {
		return this.inType1;
	}


	@Override
	public TypeInformation<IN2> getInputType2() {
		return this.inType2;
	}
	
	public static class UnwrappingCollector<T> implements Collector<T> {
		
		Collector<Reference<T>> outerCollector;

		public UnwrappingCollector(Collector<Reference<T>> outerCollector) {
			this.outerCollector = outerCollector;
		}
		
		@Override
		public void collect(T record) {
			this.outerCollector.collect(new Reference<T>(record));
		}

		@Override
		public void close() {
			this.outerCollector.close();
		}
	}
	
	public static class UnwrappingIterator<T> implements Iterator<T> {

		private Iterator<? extends Reference<T>> outerIterator;
		
		public UnwrappingIterator(Iterator<? extends Reference<T>> outerIterator) {
			this.outerIterator = outerIterator;
		}
		
		@Override
		public boolean hasNext() {
			return outerIterator.hasNext();
		}

		@Override
		public T next() {
			return outerIterator.next().ref;
		}

		@Override
		public void remove() {
			this.outerIterator.remove();
		}
		
	}

}
