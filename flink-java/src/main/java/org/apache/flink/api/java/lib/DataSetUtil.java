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

package org.apache.flink.api.java.lib;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.SingleInputUdfOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;


public class DataSetUtil {

	
	// --------------------------------------------------------------------------------------------
	//  Extraction of a single field
	// --------------------------------------------------------------------------------------------

	/**
	 * Applies a single field extraction on a {@link Tuple} {@link DataSet}.<br/>
	 * <b>Note: Can be only applied on Tuple DataSets using the corresponding field index.</b></br>
	 * The transformation extracts of each Tuple of the DataSet a given field.</br>
	 *
	 *
	 * @param ds The input DataSet.
	 * @param fieldIndex The field index of the input tuple which is extracted.
	 * @param outputType Class of the extracted field.   
	 * @return A SingleInputUdfOperator that represents the extracted field.
	 *
	 * @see Tuple
	 * @see DataSet
	 * @see org.apache.flink.api.java.operators.SingleInputUdfOperator
	 */
	public static <IN extends Tuple, OUT> SingleInputUdfOperator<IN, OUT, MapOperator<IN, OUT>> extractSingleField(DataSet<IN> ds, int fieldIndex, Class<OUT> outputType) {

		if(!ds.getType().isTupleType()) {
			throw new IllegalArgumentException("The DataSet has to contain a Tuple, not " + ds.getType().getTypeClass().getName());
		}
		
		TupleTypeInfo<IN> tupleInfo = (TupleTypeInfo) ds.getType();
		if(fieldIndex >= tupleInfo.getArity() || fieldIndex < 0) {
			throw new IndexOutOfBoundsException("The field index has to be between 0 and " + (tupleInfo.getArity()-1));
		}
		
		if(!tupleInfo.getTypeAt(fieldIndex).equals(TypeExtractor.createTypeInfo(outputType))) {
			throw new IllegalArgumentException("The output class type has to be: " + tupleInfo.getTypeAt(fieldIndex).toString());
		}
		
		return ds.map(new ExtractElement(fieldIndex)).returns(tupleInfo.getTypeAt(fieldIndex));
	}

	private static final class ExtractElement<T extends Tuple, R> implements MapFunction<T,R> {
		private int id;		

		public ExtractElement (int id){			
			this.id = id;		
		}

		@Override
		public R map(T value) {
			return (R) value.getField(id);

		}
	}	
	
}
