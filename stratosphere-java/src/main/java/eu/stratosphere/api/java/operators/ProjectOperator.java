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
package eu.stratosphere.api.java.operators;

import java.util.Arrays;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.operators.translation.PlanProjectOperator;
import eu.stratosphere.api.java.operators.translation.UnaryNodeTranslation;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple4;
import eu.stratosphere.api.java.tuple.Tuple5;
import eu.stratosphere.api.java.tuple.Tuple6;
import eu.stratosphere.api.java.tuple.Tuple7;
import eu.stratosphere.api.java.tuple.Tuple8;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeInformation;

/**
 *
 * @param <IN> The type of the data set projected by the operator.
 * @param <OUT> The type of data set that is the result of the projection.
 */
public class ProjectOperator<IN, OUT extends Tuple> 
	extends SingleInputOperator<IN, OUT, ProjectOperator<IN, OUT>> {
	
	protected final int[] fields;
	
	public ProjectOperator(DataSet<IN> input, int[] fields, TupleTypeInfo<OUT> returnType) {
		super(input, returnType);
	
		this.fields = fields;
	}

	@Override
	protected UnaryNodeTranslation translateToDataFlow() {
		String name = getName() != null ? getName() : "Projection "+Arrays.toString(fields);
		return new UnaryNodeTranslation(new PlanProjectOperator<IN, OUT>(fields, name, getInputType(), getResultType()));
	}

	
	public static class Projection<T> {
		
		private final DataSet<T> ds;
		private final int[] fields;
		
		public Projection(DataSet<T> ds, int[] fields) {
			
			this.ds = ds;
			this.fields = fields;
		}
		
		/**
		 * Projects a tuple data set to the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples.
		 * 
		 * @param type1 The class of the 1st field of the result tuples.
		 * @return The projected data set.
		 */
		public <T1> ProjectOperator<T, Tuple1<T1>> types(Class<T1> type1) {
			
			Class<?>[] types = {type1};
			if(types.length != this.fields.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fields, types, ds.getType());
			TupleTypeInfo<Tuple1<T1>> tType = new TupleTypeInfo<Tuple1<T1>>(fTypes);
			
			return new ProjectOperator<T, Tuple1<T1>>(this.ds, this.fields, tType);
		}
		
		/**
		 * Projects a tuple data set to the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples.
		 * 
		 * @param type1 The class of the 1st field of the result tuples.
		 * @param type2 The class of the 2nd field of the result tuples.
		 * @return The projected data set.
		 */
		public <T1, T2> ProjectOperator<T, Tuple2<T1, T2>> types(Class<T1> type1, Class<T2> type2) {
			
			Class<?>[] types = {type1, type2};
			if(types.length != this.fields.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fields, types, ds.getType());
			TupleTypeInfo<Tuple2<T1, T2>> tType = new TupleTypeInfo<Tuple2<T1, T2>>(fTypes);
			
			return new ProjectOperator<T, Tuple2<T1, T2>>(this.ds, this.fields, tType);
		}
		
		/**
		 * Projects a tuple data set to the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples.
		 * 
		 * @param type1 The class of the 1st field of the result tuples.
		 * @param type2 The class of the 2nd field of the result tuples.
		 * @param type3 The class of the 3rd field of the result tuples.
		 * @return The projected data set.
		 */
		public <T1, T2, T3> ProjectOperator<T, Tuple3<T1, T2, T3>> types(Class<T1> type1, Class<T2> type2, Class<T3> type3) {
			
			Class<?>[] types = {type1, type2, type3};
			if(types.length != this.fields.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fields, types, ds.getType());
			TupleTypeInfo<Tuple3<T1, T2, T3>> tType = new TupleTypeInfo<Tuple3<T1, T2, T3>>(fTypes);
			
			return new ProjectOperator<T, Tuple3<T1, T2, T3>>(this.ds, this.fields, tType);
		}
		
		/**
		 * Projects a tuple data set to the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples.
		 * 
		 * @param type1 The class of the 1st field of the result tuples.
		 * @param type2 The class of the 2nd field of the result tuples.
		 * @param type3 The class of the 3rd field of the result tuples.
		 * @param type4 The class of the 4th field of the result tuples.
		 * @return The projected data set.
		 */
		public <T1, T2, T3, T4> ProjectOperator<T, Tuple4<T1, T2, T3, T4>> types(Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4) {
			
			Class<?>[] types = {type1, type2, type3, type4};
			if(types.length != this.fields.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fields, types, ds.getType());
			TupleTypeInfo<Tuple4<T1, T2, T3, T4>> tType = new TupleTypeInfo<Tuple4<T1, T2, T3, T4>>(fTypes);
			
			return new ProjectOperator<T, Tuple4<T1, T2, T3, T4>>(this.ds, this.fields, tType);
		}
		
		/**
		 * Projects a tuple data set to the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples.
		 * 
		 * @param type1 The class of the 1st field of the result tuples.
		 * @param type2 The class of the 2nd field of the result tuples.
		 * @param type3 The class of the 3rd field of the result tuples.
		 * @param type4 The class of the 4th field of the result tuples.
		 * @param type5 The class of the 5th field of the result tuples.
		 * @return The projected data set.
		 */
		public <T1, T2, T3, T4, T5> ProjectOperator<T, Tuple5<T1, T2, T3, T4, T5>> types(Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5) {
			
			Class<?>[] types = {type1, type2, type3, type4, type5};
			if(types.length != this.fields.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fields, types, ds.getType());
			TupleTypeInfo<Tuple5<T1, T2, T3, T4, T5>> tType = new TupleTypeInfo<Tuple5<T1, T2, T3, T4, T5>>(fTypes);
			
			return new ProjectOperator<T, Tuple5<T1, T2, T3, T4, T5>>(this.ds, this.fields, tType);
		}
		
		/**
		 * Projects a tuple data set to the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples.
		 * 
		 * @param type1 The class of the 1st field of the result tuples.
		 * @param type2 The class of the 2nd field of the result tuples.
		 * @param type3 The class of the 3rd field of the result tuples.
		 * @param type4 The class of the 4th field of the result tuples.
		 * @param type5 The class of the 5th field of the result tuples.
		 * @param type6 The class of the 6th field of the result tuples.
		 * @return The projected data set.
		 */
		public <T1, T2, T3, T4, T5, T6> ProjectOperator<T, Tuple6<T1, T2, T3, T4, T5, T6>> types(
				Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6) {
			
			Class<?>[] types = {type1, type2, type3, type4, type5, type6};
			if(types.length != this.fields.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fields, types, ds.getType());
			TupleTypeInfo<Tuple6<T1, T2, T3, T4, T5, T6>> tType = new TupleTypeInfo<Tuple6<T1, T2, T3, T4, T5, T6>>(fTypes);
			
			return new ProjectOperator<T, Tuple6<T1, T2, T3, T4, T5, T6>>(this.ds, this.fields, tType);
		}
		
		/**
		 * Projects a tuple data set to the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples.
		 * 
		 * @param type1 The class of the 1st field of the result tuples.
		 * @param type2 The class of the 2nd field of the result tuples.
		 * @param type3 The class of the 3rd field of the result tuples.
		 * @param type4 The class of the 4th field of the result tuples.
		 * @param type5 The class of the 5th field of the result tuples.
		 * @param type6 The class of the 6th field of the result tuples.
		 * @param type7 The class of the 7th field of the result tuples.
		 * @return The projected data set.
		 */
		public <T1, T2, T3, T4, T5, T6, T7> ProjectOperator<T, Tuple7<T1, T2, T3, T4, T5, T6, T7>> types(
				Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7) {
			
			Class<?>[] types = {type1, type2, type3, type4, type5, type6, type7};
			if(types.length != this.fields.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fields, types, ds.getType());
			TupleTypeInfo<Tuple7<T1, T2, T3, T4, T5, T6, T7>> tType = new TupleTypeInfo<Tuple7<T1, T2, T3, T4, T5, T6, T7>>(fTypes);
			
			return new ProjectOperator<T, Tuple7<T1, T2, T3, T4, T5, T6, T7>>(this.ds, this.fields, tType);
		}
		
		/**
		 * Projects a tuple data set to the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples.
		 * 
		 * @param type1 The class of the 1st field of the result tuples.
		 * @param type2 The class of the 2nd field of the result tuples.
		 * @param type3 The class of the 3rd field of the result tuples.
		 * @param type4 The class of the 4th field of the result tuples.
		 * @param type5 The class of the 5th field of the result tuples.
		 * @param type6 The class of the 6th field of the result tuples.
		 * @param type7 The class of the 7th field of the result tuples.
		 * @param type8 The class of the 8th field of the result tuples.
		 * @return The projected data set.
		 */
		public <T1, T2, T3, T4, T5, T6, T7, T8> ProjectOperator<T, Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>> types(
				Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, 
				Class<T7> type7, Class<T8> type8) {
			
			Class<?>[] types = {type1, type2, type3, type4, type5, type6, type7, type8};
			if(types.length != this.fields.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fields, types, ds.getType());
			TupleTypeInfo<Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>> tType = new TupleTypeInfo<Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>>(fTypes);
			
			return new ProjectOperator<T, Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>>(this.ds, this.fields, tType);
		}
		
		// TODO: Do all 22 type methods
		
		// -----------------------------------------------------------------------------------------
		
			
		private TypeInformation<?>[] extractFieldTypes(int[] fields, Class<?>[] givenTypes, TypeInformation<?> inType) {
			
			TupleTypeInfo<?> inTupleType = (TupleTypeInfo<?>) inType;
			TypeInformation<?>[] fieldTypes = new TypeInformation[fields.length];
					
			for(int i=0; i<fields.length; i++) {
				
				if(inTupleType.getTypeAt(fields[i]).getTypeClass() != givenTypes[i]) {
					throw new IllegalArgumentException("Given types do not match types of input data set.");
				}
					
				fieldTypes[i] = inTupleType.getTypeAt(fields[i]);
			}
			
			return fieldTypes;
		}
		
	}
}
