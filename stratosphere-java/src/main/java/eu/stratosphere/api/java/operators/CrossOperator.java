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

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.functions.CrossFunction;
import eu.stratosphere.api.java.operators.translation.PlanCrossOperator;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple4;
import eu.stratosphere.api.java.tuple.Tuple5;
import eu.stratosphere.api.java.tuple.Tuple6;
import eu.stratosphere.api.java.tuple.Tuple7;
import eu.stratosphere.api.java.tuple.Tuple8;
import eu.stratosphere.api.java.tuple.Tuple9;
import eu.stratosphere.api.java.tuple.Tuple10;
import eu.stratosphere.api.java.tuple.Tuple11;
import eu.stratosphere.api.java.tuple.Tuple12;
import eu.stratosphere.api.java.tuple.Tuple13;
import eu.stratosphere.api.java.tuple.Tuple14;
import eu.stratosphere.api.java.tuple.Tuple15;
import eu.stratosphere.api.java.tuple.Tuple16;
import eu.stratosphere.api.java.tuple.Tuple17;
import eu.stratosphere.api.java.tuple.Tuple18;
import eu.stratosphere.api.java.tuple.Tuple19;
import eu.stratosphere.api.java.tuple.Tuple20;
import eu.stratosphere.api.java.tuple.Tuple21;
import eu.stratosphere.api.java.tuple.Tuple22;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.api.java.typeutils.TypeInformation;

import java.util.Arrays;

/**
 * A {@link DataSet} that is the result of a Cross transformation.
 *
 * @param <I1> The type of the first input DataSet of the Cross transformation.
 * @param <I2> The type of the second input DataSet of the Cross transformation.
 * @param <OUT> The type of the result of the Cross transformation.
 *
 * @see DataSet
 */
public class CrossOperator<I1, I2, OUT> 
	extends TwoInputUdfOperator<I1, I2, OUT, CrossOperator<I1, I2, OUT>> {
	
	private final CrossFunction<I1, I2, OUT> function;

	protected CrossOperator(DataSet<I1> input1, DataSet<I2> input2,
							CrossFunction<I1, I2, OUT> function,
							TypeInformation<OUT> returnType)
	{
		super(input1, input2, returnType);

		this.function = function;
	}
	
	@Override
	protected Operator translateToDataFlow(Operator input1, Operator input2) {
		
		String name = getName() != null ? getName() : function.getClass().getName();
		// create operator
		PlanCrossOperator<I1, I2, OUT> po = new PlanCrossOperator<I1, I2, OUT>(function, name, getInput1Type(), getInput2Type(), getResultType());
		// set inputs
		po.setFirstInput(input1);
		po.setSecondInput(input2);
		// set dop
		po.setDegreeOfParallelism(this.getParallelism());
		
		return po;
	}
	

	// --------------------------------------------------------------------------------------------
	// Builder classes for incremental construction
	// --------------------------------------------------------------------------------------------
	
	public static final class CrossOperatorSets<I1, I2> {
		
		private final DataSet<I1> input1;
		private final DataSet<I2> input2;
		
		public CrossOperatorSets(DataSet<I1> input1, DataSet<I2> input2) {
			if (input1 == null || input2 == null)
				throw new NullPointerException();
			
			this.input1 = input1;
			this.input2 = input2;
		}

		public <R> CrossOperator<I1, I2, R> with(CrossFunction<I1, I2, R> function) {
			TypeInformation<R> returnType = TypeExtractor.getCrossReturnTypes(function, input1.getType(), input2.getType());
			return new CrossOperator<I1, I2, R>(input1, input2, function, returnType);
		}



		/**
		 * Initiates a ProjectCross transformation and projects the first cross input<br/>
		 * If the first cross input is a {@link Tuple} {@link DataSet}, fields can be selected by their index.
		 * If the first cross input is not a Tuple DataSet, no parameters should be passed.<br/>
		 *
		 * Fields of the first and second input can be added by chaining the method calls of
		 * {@link CrossProjection#projectFirst(int...)} and {@link CrossProjection#projectSecond(int...)}.
		 *
		 * @param fieldIndexes If the first input is a Tuple DataSet, the indexes of the selected fields.
		 * 					   For a non-Tuple DataSet, do not provide parameters.
		 * 					   The order of fields in the output tuple is defined by to the order of field indexes.
		 * @return A CrossProjection that needs to be converted into a {@link ProjectOperator} to complete the
		 *           ProjectCross transformation by calling {@link CrossProjection#types()}.
		 *
		 * @see Tuple
		 * @see DataSet
		 * @see CrossProjection
		 * @see ProjectCross
		 */
		public CrossProjection<I1, I2> projectFirst(int... firstFieldIndexes) {
			return new CrossProjection<I1, I2>(input1, input2, firstFieldIndexes, null);
		}

		/**
		 * Initiates a ProjectCross transformation and projects the second cross input<br/>
		 * If the second cross input is a {@link Tuple} {@link DataSet}, fields can be selected by their index.
		 * If the second cross input is not a Tuple DataSet, no parameters should be passed.<br/>
		 *
		 * Fields of the first and second input can be added by chaining the method calls of
		 * {@link CrossProjection#projectFirst(int...)} and {@link CrossProjection#projectSecond(int...)}.
		 *
		 * @param fieldIndexes If the second input is a Tuple DataSet, the indexes of the selected fields.
		 * 					   For a non-Tuple DataSet, do not provide parameters.
		 * 					   The order of fields in the output tuple is defined by to the order of field indexes.
		 * @return A CrossProjection that needs to be converted into a {@link ProjectOperator} to complete the
		 *           ProjectCross transformation by calling {@link CrossProjection#types()}.
		 *
		 * @see Tuple
		 * @see DataSet
		 * @see CrossProjection
		 * @see ProjectCross
		 */
		public CrossProjection<I1, I2> projectSecond(int... secondFieldIndexes) {
			return new CrossProjection<I1, I2>(input1, input2, null, secondFieldIndexes);
		}
	}

	/**
	 * A Cross transformation that projects crossing elements or fields of crossing {@link Tuple Tuples}
	 * into result {@link Tuple Tuples}. <br/>
	 * It also represents the {@link DataSet} that is the result of a Cross transformation.
	 *
	 * @param <I1> The type of the first input DataSet of the Cross transformation.
	 * @param <I2> The type of the second input DataSet of the Cross transformation.
	 * @param <OUT> The type of the result of the Cross transformation.
	 *
	 * @see Tuple
	 * @see DataSet
	 */
	private static final class ProjectCross<I1, I2, OUT extends Tuple> extends CrossOperator<I1, I2, OUT> {

		protected ProjectCross(DataSet<I1> input1, DataSet<I2> input2, int[] fields, boolean[] isFromFirst, TupleTypeInfo<OUT> returnType) {
			super(input1, input2,
				new ProjectCrossFunction<I1, I2, OUT>(fields, isFromFirst, returnType.createSerializer().createInstance()),
				returnType);
		}
	}

	public static final class ProjectCrossFunction<T1, T2, R extends Tuple> extends CrossFunction<T1, T2, R> {

		private static final long serialVersionUID = 1L;

		private final int[] fields;
		private final boolean[] isFromFirst;
		private final R outTuple;

		private ProjectCrossFunction(int[] fields, boolean[] isFromFirst, R outTupleInstance) {
			this.fields = fields;
			this.isFromFirst = isFromFirst;
			this.outTuple = outTupleInstance;
		}

		public R cross(T1 in1, T2 in2) {
			for(int i=0; i<fields.length; i++) {
				if(isFromFirst[i]) {
					if(fields[i] >= 0) {
						outTuple.setField(((Tuple)in1).getField(fields[i]), i);
					} else {
						outTuple.setField(in1, i);
					}
				} else {
					if(fields[i] >= 0) {
						outTuple.setField(((Tuple)in2).getField(fields[i]), i);
					} else {
						outTuple.setField(in2, i);
					}
				}
			}
			return outTuple;
		}
	}

	public static final class CrossProjection<I1, I2> {
		private final DataSet<I1> ds1;
		private final DataSet<I2> ds2;


		private int[] fieldIndexes;
		private boolean[] isFieldInFirst;

		private final int numFieldsDs1;
		private final int numFieldsDs2;

		public CrossProjection(DataSet<I1> ds1, DataSet<I2> ds2, int[] firstFieldIndexes, int[] secondFieldIndexes) {

			boolean isFirstTuple;
			boolean isSecondTuple;

			if(ds1.getType() instanceof TupleTypeInfo) {
				numFieldsDs1 = ((TupleTypeInfo<?>)ds1.getType()).getArity();
				isFirstTuple = true;
			} else {
				numFieldsDs1 = 1;
				isFirstTuple = false;
			}
			if(ds2.getType() instanceof TupleTypeInfo) {
				numFieldsDs2 = ((TupleTypeInfo<?>)ds2.getType()).getArity();
				isSecondTuple = true;
			} else {
				numFieldsDs2 = 1;
				isSecondTuple = false;
			}

			boolean isTuple;
			boolean firstInput;

			if(firstFieldIndexes != null && secondFieldIndexes == null) {
				// index array for first input is provided
				firstInput = true;
				isTuple = isFirstTuple;
				this.fieldIndexes = firstFieldIndexes;

				if(this.fieldIndexes.length == 0) {
					// no indexes provided, treat tuple as regular object
					isTuple = false;
				}
			} else if (firstFieldIndexes == null && secondFieldIndexes != null) {
				// index array for second input is provided
				firstInput = false;
				isTuple = isSecondTuple;
				this.fieldIndexes = secondFieldIndexes;

				if(this.fieldIndexes.length == 0) {
					// no indexes provided, treat tuple as regular object
					isTuple = false;
				}
			} else if (firstFieldIndexes == null && secondFieldIndexes == null) {
				throw new IllegalArgumentException("You must provide at least one field index array.");
			} else {
				throw new IllegalArgumentException("You must provide at most one field index array.");
			}

			if(!isTuple && this.fieldIndexes.length != 0) {
				// field index provided for non-Tuple input
				throw new IllegalArgumentException("Input is not a Tuple. Call projectSecond without arguments to include it.");
			} else if(this.fieldIndexes.length > 22) {
				throw new IllegalArgumentException("You may select only up to twenty-two (22) fields.");
			}

			isFieldInFirst = new boolean[this.fieldIndexes.length];

			if(isTuple) {
				// check field indexes and adapt to position in tuple
				int maxFieldIndex = firstInput ? numFieldsDs1 : numFieldsDs2;
				for(int i=0; i<this.fieldIndexes.length; i++) {
					if(this.fieldIndexes[i] > maxFieldIndex - 1) {
						throw new IndexOutOfBoundsException("Provided field index is out of bounds of input tuple.");
					}
					if(firstInput) {
						this.isFieldInFirst[i] = true;
					} else {
						this.isFieldInFirst[i] = false;
					}
				}
			} else {
				this.isFieldInFirst = new boolean[]{firstInput};
				this.fieldIndexes = new int[]{-1};
			}

			this.ds1 = ds1;
			this.ds2 = ds2;
		}

		/**
		 * Continues a ProjectCross transformation and adds fields of the first cross input.<br/>
		 * If the first cross input is a {@link Tuple} {@link DataSet}, fields can be selected by their index.
		 * If the first cross input is not a Tuple DataSet, no parameters should be passed.<br/>
		 *
		 * Fields of the first and second input can be added by chaining the method calls of
		 * {@link CrossProjection#projectFirst(int...)} and {@link CrossProjection#projectSecond(int...)}.
		 *
		 * @param fieldIndexes If the first input is a Tuple DataSet, the indexes of the selected fields.
		 * 					   For a non-Tuple DataSet, do not provide parameters.
		 * 					   The order of fields in the output tuple is defined by to the order of field indexes.
		 * @return A CrossProjection that needs to be converted into a {@link ProjectOperator} to complete the
		 *           ProjectCross transformation by calling {@link CrossProjection#types()}.
		 *
		 * @see Tuple
		 * @see DataSet
		 * @see CrossProjection
		 * @see ProjectCross
		 */
		public CrossProjection<I1, I2> projectFirst(int... firstFieldIndexes) {

			boolean isFirstTuple;

			if(ds1.getType() instanceof TupleTypeInfo && firstFieldIndexes.length > 0) {
				isFirstTuple = true;
			} else {
				isFirstTuple = false;
			}

			if(!isFirstTuple && firstFieldIndexes.length != 0) {
				// field index provided for non-Tuple input
				throw new IllegalArgumentException("Input is not a Tuple. Call projectSecond without arguments to include it.");
			} else if(firstFieldIndexes.length > (22 - this.fieldIndexes.length)) {
				// to many field indexes provided
				throw new IllegalArgumentException("You may select only up to twenty-two (22) fields in total.");
			}

			int offset = this.fieldIndexes.length;

			if(isFirstTuple) {
				// extend index and flag arrays
				this.fieldIndexes = Arrays.copyOf(this.fieldIndexes, this.fieldIndexes.length + firstFieldIndexes.length);
				this.isFieldInFirst = Arrays.copyOf(this.isFieldInFirst, this.isFieldInFirst.length + firstFieldIndexes.length);

				// copy field indexes
				int maxFieldIndex = numFieldsDs1;
				for(int i = 0; i < firstFieldIndexes.length; i++) {
					// check if indexes in range
					if(firstFieldIndexes[i] > maxFieldIndex - 1) {
						throw new IndexOutOfBoundsException("Provided field index is out of bounds of input tuple.");
					}
					this.isFieldInFirst[offset + i] = true;
					this.fieldIndexes[offset + i] = firstFieldIndexes[i];
				}
			} else {
				// extend index and flag arrays
				this.fieldIndexes = Arrays.copyOf(this.fieldIndexes, this.fieldIndexes.length + 1);
				this.isFieldInFirst = Arrays.copyOf(this.isFieldInFirst, this.isFieldInFirst.length + 1);

				// add input object to output tuple
				this.isFieldInFirst[offset] = true;
				this.fieldIndexes[offset] = -1;
			}

			return this;
		}

		/**
		 * Continues a ProjectCross transformation and adds fields of the second cross input.<br/>
		 * If the second cross input is a {@link Tuple} {@link DataSet}, fields can be selected by their index.
		 * If the second cross input is not a Tuple DataSet, no parameters should be passed.<br/>
		 *
		 * Fields of the first and second input can be added by chaining the method calls of
		 * {@link CrossProjection#projectFirst(int...)} and {@link CrossProjection#projectSecond(int...)}.
		 *
		 * @param fieldIndexes If the second input is a Tuple DataSet, the indexes of the selected fields.
		 * 					   For a non-Tuple DataSet, do not provide parameters.
		 * 					   The order of fields in the output tuple is defined by to the order of field indexes.
		 * @return A CrossProjection that needs to be converted into a {@link ProjectOperator} to complete the
		 *           ProjectCross transformation by calling {@link CrossProjection#types()}.
		 *
		 * @see Tuple
		 * @see DataSet
		 * @see CrossProjection
		 * @see ProjectCross
		 */
		public CrossProjection<I1, I2> projectSecond(int... secondFieldIndexes) {

			boolean isSecondTuple;

			if(ds2.getType() instanceof TupleTypeInfo && secondFieldIndexes.length > 0) {
				isSecondTuple = true;
			} else {
				isSecondTuple = false;
			}

			if(!isSecondTuple && secondFieldIndexes.length != 0) {
				// field index provided for non-Tuple input
				throw new IllegalArgumentException("Input is not a Tuple. Call projectSecond without arguments to include it.");
			} else if(secondFieldIndexes.length > (22 - this.fieldIndexes.length)) {
				// to many field indexes provided
				throw new IllegalArgumentException("You may select only up to twenty-two (22) fields in total.");
			}

			int offset = this.fieldIndexes.length;

			if(isSecondTuple) {
				// extend index and flag arrays
				this.fieldIndexes = Arrays.copyOf(this.fieldIndexes, this.fieldIndexes.length + secondFieldIndexes.length);
				this.isFieldInFirst = Arrays.copyOf(this.isFieldInFirst, this.isFieldInFirst.length + secondFieldIndexes.length);

				// copy field indexes
				int maxFieldIndex = numFieldsDs2;
				for(int i = 0; i < secondFieldIndexes.length; i++) {
					// check if indexes in range
					if(secondFieldIndexes[i] > maxFieldIndex - 1) {
						throw new IndexOutOfBoundsException("Provided field index is out of bounds of input tuple.");
					}
					this.isFieldInFirst[offset + i] = false;
					this.fieldIndexes[offset + i] = secondFieldIndexes[i];
				}
			} else {
				// extend index and flag arrays
				this.fieldIndexes = Arrays.copyOf(this.fieldIndexes, this.fieldIndexes.length + 1);
				this.isFieldInFirst = Arrays.copyOf(this.isFieldInFirst, this.isFieldInFirst.length + 1);

				// add input object to output tuple
				this.isFieldInFirst[offset] = false;
				this.fieldIndexes[offset] = -1;
			}

			return this;
		}

		// --------------------------------------------------------------------------------------------
		// The following lines are generated.
		// --------------------------------------------------------------------------------------------
		// BEGIN_OF_TUPLE_DEPENDENT_CODE
		// GENERATED FROM eu.stratosphere.api.java.tuple.TupleGenerator.

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0> ProjectCross<I1, I2, Tuple1<T0>> types(Class<T0> type0) {
			Class<?>[] types = {type0};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple1<T0>> tType = new TupleTypeInfo<Tuple1<T0>>(fTypes);

			return new ProjectCross<I1, I2, Tuple1<T0>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1> ProjectCross<I1, I2, Tuple2<T0, T1>> types(Class<T0> type0, Class<T1> type1) {
			Class<?>[] types = {type0, type1};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple2<T0, T1>> tType = new TupleTypeInfo<Tuple2<T0, T1>>(fTypes);

			return new ProjectCross<I1, I2, Tuple2<T0, T1>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2> ProjectCross<I1, I2, Tuple3<T0, T1, T2>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2) {
			Class<?>[] types = {type0, type1, type2};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple3<T0, T1, T2>> tType = new TupleTypeInfo<Tuple3<T0, T1, T2>>(fTypes);

			return new ProjectCross<I1, I2, Tuple3<T0, T1, T2>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @param type3 The class of field '3' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3> ProjectCross<I1, I2, Tuple4<T0, T1, T2, T3>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3) {
			Class<?>[] types = {type0, type1, type2, type3};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple4<T0, T1, T2, T3>> tType = new TupleTypeInfo<Tuple4<T0, T1, T2, T3>>(fTypes);

			return new ProjectCross<I1, I2, Tuple4<T0, T1, T2, T3>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @param type3 The class of field '3' of the result tuples.
		 * @param type4 The class of field '4' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4> ProjectCross<I1, I2, Tuple5<T0, T1, T2, T3, T4>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4) {
			Class<?>[] types = {type0, type1, type2, type3, type4};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple5<T0, T1, T2, T3, T4>> tType = new TupleTypeInfo<Tuple5<T0, T1, T2, T3, T4>>(fTypes);

			return new ProjectCross<I1, I2, Tuple5<T0, T1, T2, T3, T4>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @param type3 The class of field '3' of the result tuples.
		 * @param type4 The class of field '4' of the result tuples.
		 * @param type5 The class of field '5' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5> ProjectCross<I1, I2, Tuple6<T0, T1, T2, T3, T4, T5>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple6<T0, T1, T2, T3, T4, T5>> tType = new TupleTypeInfo<Tuple6<T0, T1, T2, T3, T4, T5>>(fTypes);

			return new ProjectCross<I1, I2, Tuple6<T0, T1, T2, T3, T4, T5>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @param type3 The class of field '3' of the result tuples.
		 * @param type4 The class of field '4' of the result tuples.
		 * @param type5 The class of field '5' of the result tuples.
		 * @param type6 The class of field '6' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6> ProjectCross<I1, I2, Tuple7<T0, T1, T2, T3, T4, T5, T6>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple7<T0, T1, T2, T3, T4, T5, T6>> tType = new TupleTypeInfo<Tuple7<T0, T1, T2, T3, T4, T5, T6>>(fTypes);

			return new ProjectCross<I1, I2, Tuple7<T0, T1, T2, T3, T4, T5, T6>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @param type3 The class of field '3' of the result tuples.
		 * @param type4 The class of field '4' of the result tuples.
		 * @param type5 The class of field '5' of the result tuples.
		 * @param type6 The class of field '6' of the result tuples.
		 * @param type7 The class of field '7' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7> ProjectCross<I1, I2, Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> tType = new TupleTypeInfo<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(fTypes);

			return new ProjectCross<I1, I2, Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @param type3 The class of field '3' of the result tuples.
		 * @param type4 The class of field '4' of the result tuples.
		 * @param type5 The class of field '5' of the result tuples.
		 * @param type6 The class of field '6' of the result tuples.
		 * @param type7 The class of field '7' of the result tuples.
		 * @param type8 The class of field '8' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8> ProjectCross<I1, I2, Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> tType = new TupleTypeInfo<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(fTypes);

			return new ProjectCross<I1, I2, Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @param type3 The class of field '3' of the result tuples.
		 * @param type4 The class of field '4' of the result tuples.
		 * @param type5 The class of field '5' of the result tuples.
		 * @param type6 The class of field '6' of the result tuples.
		 * @param type7 The class of field '7' of the result tuples.
		 * @param type8 The class of field '8' of the result tuples.
		 * @param type9 The class of field '9' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9> ProjectCross<I1, I2, Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> tType = new TupleTypeInfo<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(fTypes);

			return new ProjectCross<I1, I2, Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @param type3 The class of field '3' of the result tuples.
		 * @param type4 The class of field '4' of the result tuples.
		 * @param type5 The class of field '5' of the result tuples.
		 * @param type6 The class of field '6' of the result tuples.
		 * @param type7 The class of field '7' of the result tuples.
		 * @param type8 The class of field '8' of the result tuples.
		 * @param type9 The class of field '9' of the result tuples.
		 * @param type10 The class of field '10' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> ProjectCross<I1, I2, Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> tType = new TupleTypeInfo<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(fTypes);

			return new ProjectCross<I1, I2, Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @param type3 The class of field '3' of the result tuples.
		 * @param type4 The class of field '4' of the result tuples.
		 * @param type5 The class of field '5' of the result tuples.
		 * @param type6 The class of field '6' of the result tuples.
		 * @param type7 The class of field '7' of the result tuples.
		 * @param type8 The class of field '8' of the result tuples.
		 * @param type9 The class of field '9' of the result tuples.
		 * @param type10 The class of field '10' of the result tuples.
		 * @param type11 The class of field '11' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> ProjectCross<I1, I2, Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> tType = new TupleTypeInfo<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(fTypes);

			return new ProjectCross<I1, I2, Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @param type3 The class of field '3' of the result tuples.
		 * @param type4 The class of field '4' of the result tuples.
		 * @param type5 The class of field '5' of the result tuples.
		 * @param type6 The class of field '6' of the result tuples.
		 * @param type7 The class of field '7' of the result tuples.
		 * @param type8 The class of field '8' of the result tuples.
		 * @param type9 The class of field '9' of the result tuples.
		 * @param type10 The class of field '10' of the result tuples.
		 * @param type11 The class of field '11' of the result tuples.
		 * @param type12 The class of field '12' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> ProjectCross<I1, I2, Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> tType = new TupleTypeInfo<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(fTypes);

			return new ProjectCross<I1, I2, Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @param type3 The class of field '3' of the result tuples.
		 * @param type4 The class of field '4' of the result tuples.
		 * @param type5 The class of field '5' of the result tuples.
		 * @param type6 The class of field '6' of the result tuples.
		 * @param type7 The class of field '7' of the result tuples.
		 * @param type8 The class of field '8' of the result tuples.
		 * @param type9 The class of field '9' of the result tuples.
		 * @param type10 The class of field '10' of the result tuples.
		 * @param type11 The class of field '11' of the result tuples.
		 * @param type12 The class of field '12' of the result tuples.
		 * @param type13 The class of field '13' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13> ProjectCross<I1, I2, Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> tType = new TupleTypeInfo<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(fTypes);

			return new ProjectCross<I1, I2, Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @param type3 The class of field '3' of the result tuples.
		 * @param type4 The class of field '4' of the result tuples.
		 * @param type5 The class of field '5' of the result tuples.
		 * @param type6 The class of field '6' of the result tuples.
		 * @param type7 The class of field '7' of the result tuples.
		 * @param type8 The class of field '8' of the result tuples.
		 * @param type9 The class of field '9' of the result tuples.
		 * @param type10 The class of field '10' of the result tuples.
		 * @param type11 The class of field '11' of the result tuples.
		 * @param type12 The class of field '12' of the result tuples.
		 * @param type13 The class of field '13' of the result tuples.
		 * @param type14 The class of field '14' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14> ProjectCross<I1, I2, Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> tType = new TupleTypeInfo<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(fTypes);

			return new ProjectCross<I1, I2, Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @param type3 The class of field '3' of the result tuples.
		 * @param type4 The class of field '4' of the result tuples.
		 * @param type5 The class of field '5' of the result tuples.
		 * @param type6 The class of field '6' of the result tuples.
		 * @param type7 The class of field '7' of the result tuples.
		 * @param type8 The class of field '8' of the result tuples.
		 * @param type9 The class of field '9' of the result tuples.
		 * @param type10 The class of field '10' of the result tuples.
		 * @param type11 The class of field '11' of the result tuples.
		 * @param type12 The class of field '12' of the result tuples.
		 * @param type13 The class of field '13' of the result tuples.
		 * @param type14 The class of field '14' of the result tuples.
		 * @param type15 The class of field '15' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15> ProjectCross<I1, I2, Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> tType = new TupleTypeInfo<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(fTypes);

			return new ProjectCross<I1, I2, Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @param type3 The class of field '3' of the result tuples.
		 * @param type4 The class of field '4' of the result tuples.
		 * @param type5 The class of field '5' of the result tuples.
		 * @param type6 The class of field '6' of the result tuples.
		 * @param type7 The class of field '7' of the result tuples.
		 * @param type8 The class of field '8' of the result tuples.
		 * @param type9 The class of field '9' of the result tuples.
		 * @param type10 The class of field '10' of the result tuples.
		 * @param type11 The class of field '11' of the result tuples.
		 * @param type12 The class of field '12' of the result tuples.
		 * @param type13 The class of field '13' of the result tuples.
		 * @param type14 The class of field '14' of the result tuples.
		 * @param type15 The class of field '15' of the result tuples.
		 * @param type16 The class of field '16' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16> ProjectCross<I1, I2, Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> tType = new TupleTypeInfo<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>(fTypes);

			return new ProjectCross<I1, I2, Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @param type3 The class of field '3' of the result tuples.
		 * @param type4 The class of field '4' of the result tuples.
		 * @param type5 The class of field '5' of the result tuples.
		 * @param type6 The class of field '6' of the result tuples.
		 * @param type7 The class of field '7' of the result tuples.
		 * @param type8 The class of field '8' of the result tuples.
		 * @param type9 The class of field '9' of the result tuples.
		 * @param type10 The class of field '10' of the result tuples.
		 * @param type11 The class of field '11' of the result tuples.
		 * @param type12 The class of field '12' of the result tuples.
		 * @param type13 The class of field '13' of the result tuples.
		 * @param type14 The class of field '14' of the result tuples.
		 * @param type15 The class of field '15' of the result tuples.
		 * @param type16 The class of field '16' of the result tuples.
		 * @param type17 The class of field '17' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17> ProjectCross<I1, I2, Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> tType = new TupleTypeInfo<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>(fTypes);

			return new ProjectCross<I1, I2, Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @param type3 The class of field '3' of the result tuples.
		 * @param type4 The class of field '4' of the result tuples.
		 * @param type5 The class of field '5' of the result tuples.
		 * @param type6 The class of field '6' of the result tuples.
		 * @param type7 The class of field '7' of the result tuples.
		 * @param type8 The class of field '8' of the result tuples.
		 * @param type9 The class of field '9' of the result tuples.
		 * @param type10 The class of field '10' of the result tuples.
		 * @param type11 The class of field '11' of the result tuples.
		 * @param type12 The class of field '12' of the result tuples.
		 * @param type13 The class of field '13' of the result tuples.
		 * @param type14 The class of field '14' of the result tuples.
		 * @param type15 The class of field '15' of the result tuples.
		 * @param type16 The class of field '16' of the result tuples.
		 * @param type17 The class of field '17' of the result tuples.
		 * @param type18 The class of field '18' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18> ProjectCross<I1, I2, Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> tType = new TupleTypeInfo<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>(fTypes);

			return new ProjectCross<I1, I2, Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @param type3 The class of field '3' of the result tuples.
		 * @param type4 The class of field '4' of the result tuples.
		 * @param type5 The class of field '5' of the result tuples.
		 * @param type6 The class of field '6' of the result tuples.
		 * @param type7 The class of field '7' of the result tuples.
		 * @param type8 The class of field '8' of the result tuples.
		 * @param type9 The class of field '9' of the result tuples.
		 * @param type10 The class of field '10' of the result tuples.
		 * @param type11 The class of field '11' of the result tuples.
		 * @param type12 The class of field '12' of the result tuples.
		 * @param type13 The class of field '13' of the result tuples.
		 * @param type14 The class of field '14' of the result tuples.
		 * @param type15 The class of field '15' of the result tuples.
		 * @param type16 The class of field '16' of the result tuples.
		 * @param type17 The class of field '17' of the result tuples.
		 * @param type18 The class of field '18' of the result tuples.
		 * @param type19 The class of field '19' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19> ProjectCross<I1, I2, Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> tType = new TupleTypeInfo<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>(fTypes);

			return new ProjectCross<I1, I2, Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @param type3 The class of field '3' of the result tuples.
		 * @param type4 The class of field '4' of the result tuples.
		 * @param type5 The class of field '5' of the result tuples.
		 * @param type6 The class of field '6' of the result tuples.
		 * @param type7 The class of field '7' of the result tuples.
		 * @param type8 The class of field '8' of the result tuples.
		 * @param type9 The class of field '9' of the result tuples.
		 * @param type10 The class of field '10' of the result tuples.
		 * @param type11 The class of field '11' of the result tuples.
		 * @param type12 The class of field '12' of the result tuples.
		 * @param type13 The class of field '13' of the result tuples.
		 * @param type14 The class of field '14' of the result tuples.
		 * @param type15 The class of field '15' of the result tuples.
		 * @param type16 The class of field '16' of the result tuples.
		 * @param type17 The class of field '17' of the result tuples.
		 * @param type18 The class of field '18' of the result tuples.
		 * @param type19 The class of field '19' of the result tuples.
		 * @param type20 The class of field '20' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20> ProjectCross<I1, I2, Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19, Class<T20> type20) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> tType = new TupleTypeInfo<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>(fTypes);

			return new ProjectCross<I1, I2, Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @param type2 The class of field '2' of the result tuples.
		 * @param type3 The class of field '3' of the result tuples.
		 * @param type4 The class of field '4' of the result tuples.
		 * @param type5 The class of field '5' of the result tuples.
		 * @param type6 The class of field '6' of the result tuples.
		 * @param type7 The class of field '7' of the result tuples.
		 * @param type8 The class of field '8' of the result tuples.
		 * @param type9 The class of field '9' of the result tuples.
		 * @param type10 The class of field '10' of the result tuples.
		 * @param type11 The class of field '11' of the result tuples.
		 * @param type12 The class of field '12' of the result tuples.
		 * @param type13 The class of field '13' of the result tuples.
		 * @param type14 The class of field '14' of the result tuples.
		 * @param type15 The class of field '15' of the result tuples.
		 * @param type16 The class of field '16' of the result tuples.
		 * @param type17 The class of field '17' of the result tuples.
		 * @param type18 The class of field '18' of the result tuples.
		 * @param type19 The class of field '19' of the result tuples.
		 * @param type20 The class of field '20' of the result tuples.
		 * @param type21 The class of field '21' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21> ProjectCross<I1, I2, Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19, Class<T20> type20, Class<T21> type21) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20, type21};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> tType = new TupleTypeInfo<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>(fTypes);

			return new ProjectCross<I1, I2, Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		// END_OF_TUPLE_DEPENDENT_CODE
		// -----------------------------------------------------------------------------------------

		private TypeInformation<?>[] extractFieldTypes(int[] fields, Class<?>[] givenTypes) {

			TypeInformation<?>[] fieldTypes = new TypeInformation[fields.length];

			for(int i=0; i<fields.length; i++) {

				TypeInformation<?> typeInfo;
				if(isFieldInFirst[i]) {
					if(fields[i] >= 0) {
						typeInfo = ((TupleTypeInfo<?>)ds1.getType()).getTypeAt(fields[i]);
					} else {
						typeInfo = ds1.getType();
					}
				} else {
					if(fields[i] >= 0) {
						typeInfo = ((TupleTypeInfo<?>)ds2.getType()).getTypeAt(fields[i]);
					} else {
						typeInfo = ds2.getType();
					}
				}

				if(typeInfo.getTypeClass() != givenTypes[i]) {
					throw new IllegalArgumentException("Given types do not match types of input data set.");
				}

				fieldTypes[i] = typeInfo;
			}

			return fieldTypes;
		}
	}
}
