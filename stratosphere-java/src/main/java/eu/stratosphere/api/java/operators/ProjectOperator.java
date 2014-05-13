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

import eu.stratosphere.api.common.functions.GenericMap;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.operators.translation.PlanProjectOperator;
//CHECKSTYLE.OFF: AvoidStarImport - Needed for TupleGenerator
import eu.stratosphere.api.java.tuple.*;
//CHECKSTYLE.ON: AvoidStarImport
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.types.TypeInformation;


/**
 * This operator represents the application of a projection operation on a data set, and the
 * result data set produced by the function.
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
	protected eu.stratosphere.api.common.operators.base.MapOperatorBase<IN, OUT, GenericMap<IN,OUT>> translateToDataFlow(Operator<IN> input) {		
		String name = getName() != null ? getName() : "Projection " + Arrays.toString(fields);
		// create operator
		PlanProjectOperator<IN, OUT> ppo = new PlanProjectOperator<IN, OUT>(fields, name, getInputType(), getResultType());
		// set input
		ppo.setInput(input);
		// set dop
		ppo.setDegreeOfParallelism(this.getParallelism());
		
		return ppo;
	}

	
	public static class Projection<T> {
		
		private final DataSet<T> ds;
		private final int[] fieldIndexes;
		
		public Projection(DataSet<T> ds, int[] fieldIndexes) {
			
			if(!(ds.getType() instanceof TupleTypeInfo)) {
				throw new UnsupportedOperationException("project() can only be applied to DataSets of Tuples.");
			}
			
			if(fieldIndexes.length == 0) {
				throw new IllegalArgumentException("project() needs to select at least one (1) field.");
			} else if(fieldIndexes.length > 22) {
				throw new IllegalArgumentException("project() may select only up to twenty-two (22) fields.");
			}
			
			int maxFieldIndex = ((TupleTypeInfo<?>)ds.getType()).getArity();
			for(int i=0; i<fieldIndexes.length; i++) {
				if(fieldIndexes[i] > maxFieldIndex - 1) {
					throw new IndexOutOfBoundsException("Provided field index is out of bounds of input tuple.");
				}
			}
			
			this.ds = ds;
			this.fieldIndexes = fieldIndexes;
		}
		
		// --------------------------------------------------------------------------------------------	
		// The following lines are generated.
		// --------------------------------------------------------------------------------------------	
		// BEGIN_OF_TUPLE_DEPENDENT_CODE	
	// GENERATED FROM eu.stratosphere.api.java.tuple.TupleGenerator.

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0> ProjectOperator<T, Tuple1<T0>> types(Class<T0> type0) {
			Class<?>[] types = {type0};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple1<T0>> tType = new TupleTypeInfo<Tuple1<T0>>(fTypes);

			return new ProjectOperator<T, Tuple1<T0>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1> ProjectOperator<T, Tuple2<T0, T1>> types(Class<T0> type0, Class<T1> type1) {
			Class<?>[] types = {type0, type1};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple2<T0, T1>> tType = new TupleTypeInfo<Tuple2<T0, T1>>(fTypes);

			return new ProjectOperator<T, Tuple2<T0, T1>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2> ProjectOperator<T, Tuple3<T0, T1, T2>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2) {
			Class<?>[] types = {type0, type1, type2};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple3<T0, T1, T2>> tType = new TupleTypeInfo<Tuple3<T0, T1, T2>>(fTypes);

			return new ProjectOperator<T, Tuple3<T0, T1, T2>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3> ProjectOperator<T, Tuple4<T0, T1, T2, T3>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3) {
			Class<?>[] types = {type0, type1, type2, type3};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple4<T0, T1, T2, T3>> tType = new TupleTypeInfo<Tuple4<T0, T1, T2, T3>>(fTypes);

			return new ProjectOperator<T, Tuple4<T0, T1, T2, T3>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4> ProjectOperator<T, Tuple5<T0, T1, T2, T3, T4>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4) {
			Class<?>[] types = {type0, type1, type2, type3, type4};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple5<T0, T1, T2, T3, T4>> tType = new TupleTypeInfo<Tuple5<T0, T1, T2, T3, T4>>(fTypes);

			return new ProjectOperator<T, Tuple5<T0, T1, T2, T3, T4>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5> ProjectOperator<T, Tuple6<T0, T1, T2, T3, T4, T5>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple6<T0, T1, T2, T3, T4, T5>> tType = new TupleTypeInfo<Tuple6<T0, T1, T2, T3, T4, T5>>(fTypes);

			return new ProjectOperator<T, Tuple6<T0, T1, T2, T3, T4, T5>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @param type6 The class of field '6' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6> ProjectOperator<T, Tuple7<T0, T1, T2, T3, T4, T5, T6>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple7<T0, T1, T2, T3, T4, T5, T6>> tType = new TupleTypeInfo<Tuple7<T0, T1, T2, T3, T4, T5, T6>>(fTypes);

			return new ProjectOperator<T, Tuple7<T0, T1, T2, T3, T4, T5, T6>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @param type6 The class of field '6' of the result Tuples.
		 * @param type7 The class of field '7' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7> ProjectOperator<T, Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> tType = new TupleTypeInfo<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(fTypes);

			return new ProjectOperator<T, Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @param type6 The class of field '6' of the result Tuples.
		 * @param type7 The class of field '7' of the result Tuples.
		 * @param type8 The class of field '8' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8> ProjectOperator<T, Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> tType = new TupleTypeInfo<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(fTypes);

			return new ProjectOperator<T, Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @param type6 The class of field '6' of the result Tuples.
		 * @param type7 The class of field '7' of the result Tuples.
		 * @param type8 The class of field '8' of the result Tuples.
		 * @param type9 The class of field '9' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9> ProjectOperator<T, Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> tType = new TupleTypeInfo<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(fTypes);

			return new ProjectOperator<T, Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @param type6 The class of field '6' of the result Tuples.
		 * @param type7 The class of field '7' of the result Tuples.
		 * @param type8 The class of field '8' of the result Tuples.
		 * @param type9 The class of field '9' of the result Tuples.
		 * @param type10 The class of field '10' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> ProjectOperator<T, Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> tType = new TupleTypeInfo<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(fTypes);

			return new ProjectOperator<T, Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @param type6 The class of field '6' of the result Tuples.
		 * @param type7 The class of field '7' of the result Tuples.
		 * @param type8 The class of field '8' of the result Tuples.
		 * @param type9 The class of field '9' of the result Tuples.
		 * @param type10 The class of field '10' of the result Tuples.
		 * @param type11 The class of field '11' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> ProjectOperator<T, Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> tType = new TupleTypeInfo<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(fTypes);

			return new ProjectOperator<T, Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @param type6 The class of field '6' of the result Tuples.
		 * @param type7 The class of field '7' of the result Tuples.
		 * @param type8 The class of field '8' of the result Tuples.
		 * @param type9 The class of field '9' of the result Tuples.
		 * @param type10 The class of field '10' of the result Tuples.
		 * @param type11 The class of field '11' of the result Tuples.
		 * @param type12 The class of field '12' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> ProjectOperator<T, Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> tType = new TupleTypeInfo<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(fTypes);

			return new ProjectOperator<T, Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @param type6 The class of field '6' of the result Tuples.
		 * @param type7 The class of field '7' of the result Tuples.
		 * @param type8 The class of field '8' of the result Tuples.
		 * @param type9 The class of field '9' of the result Tuples.
		 * @param type10 The class of field '10' of the result Tuples.
		 * @param type11 The class of field '11' of the result Tuples.
		 * @param type12 The class of field '12' of the result Tuples.
		 * @param type13 The class of field '13' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13> ProjectOperator<T, Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> tType = new TupleTypeInfo<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(fTypes);

			return new ProjectOperator<T, Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @param type6 The class of field '6' of the result Tuples.
		 * @param type7 The class of field '7' of the result Tuples.
		 * @param type8 The class of field '8' of the result Tuples.
		 * @param type9 The class of field '9' of the result Tuples.
		 * @param type10 The class of field '10' of the result Tuples.
		 * @param type11 The class of field '11' of the result Tuples.
		 * @param type12 The class of field '12' of the result Tuples.
		 * @param type13 The class of field '13' of the result Tuples.
		 * @param type14 The class of field '14' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14> ProjectOperator<T, Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> tType = new TupleTypeInfo<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(fTypes);

			return new ProjectOperator<T, Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @param type6 The class of field '6' of the result Tuples.
		 * @param type7 The class of field '7' of the result Tuples.
		 * @param type8 The class of field '8' of the result Tuples.
		 * @param type9 The class of field '9' of the result Tuples.
		 * @param type10 The class of field '10' of the result Tuples.
		 * @param type11 The class of field '11' of the result Tuples.
		 * @param type12 The class of field '12' of the result Tuples.
		 * @param type13 The class of field '13' of the result Tuples.
		 * @param type14 The class of field '14' of the result Tuples.
		 * @param type15 The class of field '15' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15> ProjectOperator<T, Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> tType = new TupleTypeInfo<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(fTypes);

			return new ProjectOperator<T, Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @param type6 The class of field '6' of the result Tuples.
		 * @param type7 The class of field '7' of the result Tuples.
		 * @param type8 The class of field '8' of the result Tuples.
		 * @param type9 The class of field '9' of the result Tuples.
		 * @param type10 The class of field '10' of the result Tuples.
		 * @param type11 The class of field '11' of the result Tuples.
		 * @param type12 The class of field '12' of the result Tuples.
		 * @param type13 The class of field '13' of the result Tuples.
		 * @param type14 The class of field '14' of the result Tuples.
		 * @param type15 The class of field '15' of the result Tuples.
		 * @param type16 The class of field '16' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16> ProjectOperator<T, Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> tType = new TupleTypeInfo<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>(fTypes);

			return new ProjectOperator<T, Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @param type6 The class of field '6' of the result Tuples.
		 * @param type7 The class of field '7' of the result Tuples.
		 * @param type8 The class of field '8' of the result Tuples.
		 * @param type9 The class of field '9' of the result Tuples.
		 * @param type10 The class of field '10' of the result Tuples.
		 * @param type11 The class of field '11' of the result Tuples.
		 * @param type12 The class of field '12' of the result Tuples.
		 * @param type13 The class of field '13' of the result Tuples.
		 * @param type14 The class of field '14' of the result Tuples.
		 * @param type15 The class of field '15' of the result Tuples.
		 * @param type16 The class of field '16' of the result Tuples.
		 * @param type17 The class of field '17' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17> ProjectOperator<T, Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> tType = new TupleTypeInfo<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>(fTypes);

			return new ProjectOperator<T, Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @param type6 The class of field '6' of the result Tuples.
		 * @param type7 The class of field '7' of the result Tuples.
		 * @param type8 The class of field '8' of the result Tuples.
		 * @param type9 The class of field '9' of the result Tuples.
		 * @param type10 The class of field '10' of the result Tuples.
		 * @param type11 The class of field '11' of the result Tuples.
		 * @param type12 The class of field '12' of the result Tuples.
		 * @param type13 The class of field '13' of the result Tuples.
		 * @param type14 The class of field '14' of the result Tuples.
		 * @param type15 The class of field '15' of the result Tuples.
		 * @param type16 The class of field '16' of the result Tuples.
		 * @param type17 The class of field '17' of the result Tuples.
		 * @param type18 The class of field '18' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18> ProjectOperator<T, Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> tType = new TupleTypeInfo<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>(fTypes);

			return new ProjectOperator<T, Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @param type6 The class of field '6' of the result Tuples.
		 * @param type7 The class of field '7' of the result Tuples.
		 * @param type8 The class of field '8' of the result Tuples.
		 * @param type9 The class of field '9' of the result Tuples.
		 * @param type10 The class of field '10' of the result Tuples.
		 * @param type11 The class of field '11' of the result Tuples.
		 * @param type12 The class of field '12' of the result Tuples.
		 * @param type13 The class of field '13' of the result Tuples.
		 * @param type14 The class of field '14' of the result Tuples.
		 * @param type15 The class of field '15' of the result Tuples.
		 * @param type16 The class of field '16' of the result Tuples.
		 * @param type17 The class of field '17' of the result Tuples.
		 * @param type18 The class of field '18' of the result Tuples.
		 * @param type19 The class of field '19' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19> ProjectOperator<T, Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> tType = new TupleTypeInfo<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>(fTypes);

			return new ProjectOperator<T, Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @param type6 The class of field '6' of the result Tuples.
		 * @param type7 The class of field '7' of the result Tuples.
		 * @param type8 The class of field '8' of the result Tuples.
		 * @param type9 The class of field '9' of the result Tuples.
		 * @param type10 The class of field '10' of the result Tuples.
		 * @param type11 The class of field '11' of the result Tuples.
		 * @param type12 The class of field '12' of the result Tuples.
		 * @param type13 The class of field '13' of the result Tuples.
		 * @param type14 The class of field '14' of the result Tuples.
		 * @param type15 The class of field '15' of the result Tuples.
		 * @param type16 The class of field '16' of the result Tuples.
		 * @param type17 The class of field '17' of the result Tuples.
		 * @param type18 The class of field '18' of the result Tuples.
		 * @param type19 The class of field '19' of the result Tuples.
		 * @param type20 The class of field '20' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20> ProjectOperator<T, Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19, Class<T20> type20) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> tType = new TupleTypeInfo<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>(fTypes);

			return new ProjectOperator<T, Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @param type6 The class of field '6' of the result Tuples.
		 * @param type7 The class of field '7' of the result Tuples.
		 * @param type8 The class of field '8' of the result Tuples.
		 * @param type9 The class of field '9' of the result Tuples.
		 * @param type10 The class of field '10' of the result Tuples.
		 * @param type11 The class of field '11' of the result Tuples.
		 * @param type12 The class of field '12' of the result Tuples.
		 * @param type13 The class of field '13' of the result Tuples.
		 * @param type14 The class of field '14' of the result Tuples.
		 * @param type15 The class of field '15' of the result Tuples.
		 * @param type16 The class of field '16' of the result Tuples.
		 * @param type17 The class of field '17' of the result Tuples.
		 * @param type18 The class of field '18' of the result Tuples.
		 * @param type19 The class of field '19' of the result Tuples.
		 * @param type20 The class of field '20' of the result Tuples.
		 * @param type21 The class of field '21' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21> ProjectOperator<T, Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19, Class<T20> type20, Class<T21> type21) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20, type21};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> tType = new TupleTypeInfo<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>(fTypes);

			return new ProjectOperator<T, Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @param type6 The class of field '6' of the result Tuples.
		 * @param type7 The class of field '7' of the result Tuples.
		 * @param type8 The class of field '8' of the result Tuples.
		 * @param type9 The class of field '9' of the result Tuples.
		 * @param type10 The class of field '10' of the result Tuples.
		 * @param type11 The class of field '11' of the result Tuples.
		 * @param type12 The class of field '12' of the result Tuples.
		 * @param type13 The class of field '13' of the result Tuples.
		 * @param type14 The class of field '14' of the result Tuples.
		 * @param type15 The class of field '15' of the result Tuples.
		 * @param type16 The class of field '16' of the result Tuples.
		 * @param type17 The class of field '17' of the result Tuples.
		 * @param type18 The class of field '18' of the result Tuples.
		 * @param type19 The class of field '19' of the result Tuples.
		 * @param type20 The class of field '20' of the result Tuples.
		 * @param type21 The class of field '21' of the result Tuples.
		 * @param type22 The class of field '22' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22> ProjectOperator<T, Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19, Class<T20> type20, Class<T21> type21, Class<T22> type22) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20, type21, type22};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>> tType = new TupleTypeInfo<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>>(fTypes);

			return new ProjectOperator<T, Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @param type6 The class of field '6' of the result Tuples.
		 * @param type7 The class of field '7' of the result Tuples.
		 * @param type8 The class of field '8' of the result Tuples.
		 * @param type9 The class of field '9' of the result Tuples.
		 * @param type10 The class of field '10' of the result Tuples.
		 * @param type11 The class of field '11' of the result Tuples.
		 * @param type12 The class of field '12' of the result Tuples.
		 * @param type13 The class of field '13' of the result Tuples.
		 * @param type14 The class of field '14' of the result Tuples.
		 * @param type15 The class of field '15' of the result Tuples.
		 * @param type16 The class of field '16' of the result Tuples.
		 * @param type17 The class of field '17' of the result Tuples.
		 * @param type18 The class of field '18' of the result Tuples.
		 * @param type19 The class of field '19' of the result Tuples.
		 * @param type20 The class of field '20' of the result Tuples.
		 * @param type21 The class of field '21' of the result Tuples.
		 * @param type22 The class of field '22' of the result Tuples.
		 * @param type23 The class of field '23' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23> ProjectOperator<T, Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19, Class<T20> type20, Class<T21> type21, Class<T22> type22, Class<T23> type23) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20, type21, type22, type23};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>> tType = new TupleTypeInfo<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>>(fTypes);

			return new ProjectOperator<T, Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>>(this.ds, this.fieldIndexes, tType);
		}

		/**
		 * Projects a {@link Tuple} {@link DataSet} to the previously selected fields. 
		 * Requires the classes of the fields of the resulting Tuples. 
		 * 
		 * @param type0 The class of field '0' of the result Tuples.
		 * @param type1 The class of field '1' of the result Tuples.
		 * @param type2 The class of field '2' of the result Tuples.
		 * @param type3 The class of field '3' of the result Tuples.
		 * @param type4 The class of field '4' of the result Tuples.
		 * @param type5 The class of field '5' of the result Tuples.
		 * @param type6 The class of field '6' of the result Tuples.
		 * @param type7 The class of field '7' of the result Tuples.
		 * @param type8 The class of field '8' of the result Tuples.
		 * @param type9 The class of field '9' of the result Tuples.
		 * @param type10 The class of field '10' of the result Tuples.
		 * @param type11 The class of field '11' of the result Tuples.
		 * @param type12 The class of field '12' of the result Tuples.
		 * @param type13 The class of field '13' of the result Tuples.
		 * @param type14 The class of field '14' of the result Tuples.
		 * @param type15 The class of field '15' of the result Tuples.
		 * @param type16 The class of field '16' of the result Tuples.
		 * @param type17 The class of field '17' of the result Tuples.
		 * @param type18 The class of field '18' of the result Tuples.
		 * @param type19 The class of field '19' of the result Tuples.
		 * @param type20 The class of field '20' of the result Tuples.
		 * @param type21 The class of field '21' of the result Tuples.
		 * @param type22 The class of field '22' of the result Tuples.
		 * @param type23 The class of field '23' of the result Tuples.
		 * @param type24 The class of field '24' of the result Tuples.
		 * @return The projected DataSet.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24> ProjectOperator<T, Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19, Class<T20> type20, Class<T21> type21, Class<T22> type22, Class<T23> type23, Class<T24> type24) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20, type21, type22, type23, type24};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types, ds.getType());
			TupleTypeInfo<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>> tType = new TupleTypeInfo<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>>(fTypes);

			return new ProjectOperator<T, Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>>(this.ds, this.fieldIndexes, tType);
		}

		// END_OF_TUPLE_DEPENDENT_CODE
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
