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

package org.apache.flink.api.java.operators;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.api.java.operators.translation.PlanExtractOperator;
import org.apache.flink.api.java.operators.translation.PlanProjectOperator;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.util.Arrays;

//CHECKSTYLE.OFF: AvoidStarImport - Needed for TupleGenerator
//CHECKSTYLE.ON: AvoidStarImport

/**
 * This operator represents the application of a projection operation on a data set, and the
 * result data set produced by the function.
 * 
 * @param <IN> The type of the data set projected by the operator.
 * @param <OUT> The type of data set that is the result of the projection.
 */
public class ExtractOperator<IN extends Tuple, OUT> 
	extends SingleInputOperator<IN, OUT, ExtractOperator<IN, OUT>> {
	
	protected final int field;
	
	private Extraction<IN> extr;
	
	public ExtractOperator(DataSet<IN> input, int field, TypeInformation<OUT> returnType) {
		super(input, returnType);
	
		this.field = field;
		extr = null;
	}
	
	public ExtractOperator(DataSet<IN> input, int field, TypeInformation<OUT> returnType, Extraction<IN> extr) {
		super(input, returnType);
	
		this.field = field;
		this.extr = extr;
	}

	@Override
	protected org.apache.flink.api.common.operators.base.MapOperatorBase<IN, OUT, MapFunction<IN,OUT>> translateToDataFlow(Operator<IN> input) {
		String name = getName() != null ? getName() : "Extraction " + field;
		// create operator
		PlanExtractOperator<IN, OUT> ppo = new PlanExtractOperator<IN, OUT>(field, name, getInputType(), getResultType());
		// set input
		ppo.setInput(input);
		// set dop
		ppo.setDegreeOfParallelism(this.getParallelism());
		
		//TODO: check this
		ppo.setSemanticProperties(SemanticPropUtil.createExtractionPropertiesSingle(field));

		return ppo;
	}

	/**
	 * Continues a Project transformation on a {@link org.apache.flink.api.java.tuple.Tuple} {@link org.apache.flink.api.java.DataSet}.<br/>
	 * <b>Note: Only Tuple DataSets can be projected using field indexes.</b></br>
	 * The transformation projects each Tuple of the DataSet onto a (sub)set of fields.</br>
	 * Additional fields can be added to the projection by calling {@link org.apache.flink.api.java.operators.ExtractOperator#project(int[])}.
	 *
	 * <b>Note: With the current implementation, the Project transformation looses type information.</b>
	 *
	 * @param fieldIndex The field indexes which are added to the Project transformation.
	 * 					   The order of fields in the output tuple corresponds to the order of field indexes.
	 * @return A ProjectOperator that represents the projected DataSet.
	 *
	 * @see org.apache.flink.api.java.tuple.Tuple
	 * @see org.apache.flink.api.java.DataSet
	 * @see org.apache.flink.api.java.operators.ExtractOperator
	 */
	@SuppressWarnings("hiding")
	public <OUT> ExtractOperator<? extends Tuple, OUT> extract(int fieldIndex) {
		extr.acceptIndex(fieldIndex);
		
		return extr.extractElementX();
	}

	public <OUT> ExtractOperator<? extends Tuple, OUT> type(Class<OUT> type) {
		return extr.extractElementX();
	}
	
	
	/**
	 * Deprecated method only kept for compatibility.
	 */
	@SuppressWarnings({ "unchecked", "hiding" })
	@Deprecated
	public <OUT extends Tuple> ExtractOperator<IN, OUT> types(Class<?>... types) {
		TupleTypeInfo<OUT> typeInfo = (TupleTypeInfo<OUT>)this.getResultType();

		if(types.length != typeInfo.getArity()) {
			throw new InvalidProgramException("Provided types do not match projection.");
		}
		for (int i=0; i<types.length; i++) {
			Class<?> typeClass = types[i];
			if (!typeClass.equals(typeInfo.getTypeAt(i).getTypeClass())) {
				throw new InvalidProgramException("Provided type "+typeClass.getSimpleName()+" at position "+i+" does not match projection");
			}
		}
		return (ExtractOperator<IN, OUT>) this;
	}
	
	public static class Extraction<T extends Tuple> {
		
		private final DataSet<T> ds;		
		private int fieldIndex;
		
		public Extraction(DataSet<T> ds, int fieldIndex, Class outputType) {
			
			if(!(ds.getType() instanceof TupleTypeInfo)) {
				throw new UnsupportedOperationException("extract() can only be applied to DataSets of Tuples.");
			}
			
			TupleTypeInfo tupleInfo = (TupleTypeInfo) ds.getType();
			
			if(!tupleInfo.getTypeAt(fieldIndex).equals(TypeExtractor.createTypeInfo(outputType))) {
				throw new IllegalArgumentException("The output class type has to be: " + tupleInfo.getTypeAt(fieldIndex).toString());
			}
			
			if(fieldIndex < 0) {
				throw new IllegalArgumentException("The index of extract() has to be positive!");
			} 
			if(fieldIndex > Tuple.MAX_ARITY) {
				throw new IllegalArgumentException("The index of extract() has to be smaller than the number of elements of the tuple!");
			}
			
			
			this.ds = ds;
			this.fieldIndex = fieldIndex;
		}

		private void acceptIndex(int fieldIndex) {

			if(fieldIndex < 0) {
				throw new IllegalArgumentException("The index of extract() has to be positive!");
			}
			if(fieldIndex > Tuple.MAX_ARITY) {
				throw new IllegalArgumentException("The index of extract() has to be smaller than the number of elements of the tuple!");
			}

			this.fieldIndex = fieldIndex;
		}
		
		
		
		// --------------------------------------------------------------------------------------------	
		// The following lines are generated.
		// --------------------------------------------------------------------------------------------	
		// BEGIN_OF_TUPLE_DEPENDENT_CODE	
	// GENERATED FROM org.apache.flink.api.java.tuple.TupleGenerator.

		/**
		 * Chooses a projectTupleX according to the length of
		 * {@link org.apache.flink.api.java.operators.ExtractOperator.Extraction#fieldIndex} 
		 * 
		 * @return The projected DataSet.
		 * 
		 * @see org.apache.flink.api.java.operators.ExtractOperator.Extraction
		 */
		@SuppressWarnings("unchecked")
		public <OUT> ExtractOperator<T, OUT> extractElementX() {
			ExtractOperator<T, OUT> projOperator = null;

			TupleTypeInfo tupleInfo = (TupleTypeInfo) ds.getType();
			TypeInformation<OUT> tType = tupleInfo.getTypeAt(fieldIndex);
			
			return new ExtractOperator<T, OUT>(this.ds, this.fieldIndex, tType, this);
		}

		/**
		 * Projects a {@link org.apache.flink.api.java.tuple.Tuple} {@link org.apache.flink.api.java.DataSet} to the previously selected fields. 
		 * 
		 * @return The projected DataSet.
		 * 
		 * @see org.apache.flink.api.java.tuple.Tuple
		 * @see org.apache.flink.api.java.DataSet
		 */
		/*
		public <T0> ExtractOperator<T, Tuple1<T0>> projectTuple1() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, ds.getType());
			TupleTypeInfo<Tuple1<T0>> tType = new TupleTypeInfo<Tuple1<T0>>(fTypes);

			return new ExtractOperator<T, Tuple1<T0>>(this.ds, this.fieldIndexes, tType, this);
		}
		*/

		
		
		
		private TypeInformation<?> extractFieldType(int field, TypeInformation<?> inType) {
			
			TupleTypeInfo<?> inTupleType = (TupleTypeInfo<?>) inType;
			return inTupleType.getTypeAt(field);
		}
		
	}
}
