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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.base.CrossOperatorBase;
import org.apache.flink.api.common.operators.base.CrossOperatorBase.CrossHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple13;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.api.java.tuple.Tuple17;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.tuple.Tuple19;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple20;
import org.apache.flink.api.java.tuple.Tuple21;
import org.apache.flink.api.java.tuple.Tuple22;
import org.apache.flink.api.java.tuple.Tuple23;
import org.apache.flink.api.java.tuple.Tuple24;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Preconditions;

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
@Public
public class CrossOperator<I1, I2, OUT> extends TwoInputUdfOperator<I1, I2, OUT, CrossOperator<I1, I2, OUT>> {

	private final CrossFunction<I1, I2, OUT> function;

	private final String defaultName;

	private final CrossHint hint;

	public CrossOperator(DataSet<I1> input1, DataSet<I2> input2,
							CrossFunction<I1, I2, OUT> function,
							TypeInformation<OUT> returnType,
							CrossHint hint,
							String defaultName) {
		super(input1, input2, returnType);

		this.function = function;
		this.defaultName = defaultName;
		this.hint = hint;
	}

	@Override
	protected CrossFunction<I1, I2, OUT> getFunction() {
		return function;
	}

	@Internal
	public CrossHint getCrossHint() {
		return hint;
	}

	private String getDefaultName() {
		return defaultName;
	}

	@Override
	protected CrossOperatorBase<I1, I2, OUT, CrossFunction<I1, I2, OUT>> translateToDataFlow(Operator<I1> input1, Operator<I2> input2) {

		String name = getName() != null ? getName() : "Cross at " + defaultName;
		// create operator
		CrossOperatorBase<I1, I2, OUT, CrossFunction<I1, I2, OUT>> po =
				new CrossOperatorBase<I1, I2, OUT, CrossFunction<I1, I2, OUT>>(function,
						new BinaryOperatorInformation<I1, I2, OUT>(getInput1Type(), getInput2Type(), getResultType()),
						name);

		po.setFirstInput(input1);
		po.setSecondInput(input2);
		po.setParallelism(getParallelism());
		po.setCrossHint(hint);

		return po;
	}

	// --------------------------------------------------------------------------------------------
	// Builder classes for incremental construction
	// --------------------------------------------------------------------------------------------

	/**
	 * A Cross transformation that wraps pairs of crossed elements into {@link Tuple2}.
	 *
	 * <p>It also represents the {@link DataSet} that is the result of a Cross transformation.
	 *
	 * @param <I1> The type of the first input DataSet of the Cross transformation.
	 * @param <I2> The type of the second input DataSet of the Cross transformation.
	 *
	 * @see Tuple2
	 * @see DataSet
	 */
	@Public
	public static final class DefaultCross<I1, I2> extends CrossOperator<I1, I2, Tuple2<I1, I2>>  {

		public DefaultCross(DataSet<I1> input1, DataSet<I2> input2, CrossHint hint, String defaultName) {
			super(input1, input2, new DefaultCrossFunction<I1, I2>(),
				new TupleTypeInfo<Tuple2<I1, I2>>(
					Preconditions.checkNotNull(input1, "input1 is null").getType(),
					Preconditions.checkNotNull(input2, "input2 is null").getType()),
				hint, defaultName);
		}

		/**
		 * Finalizes a Cross transformation by applying a {@link CrossFunction} to each pair of crossed elements.
		 *
		 * <p>Each CrossFunction call returns exactly one element.
		 *
		 * @param function The CrossFunction that is called for each pair of crossed elements.
		 * @return An CrossOperator that represents the crossed result DataSet
		 *
		 * @see CrossFunction
		 * @see DataSet
		 */
		public <R> CrossOperator<I1, I2, R> with(CrossFunction<I1, I2, R> function) {
			if (function == null) {
				throw new NullPointerException("Cross function must not be null.");
			}
			TypeInformation<R> returnType = TypeExtractor.getCrossReturnTypes(function, getInput1().getType(), getInput2().getType(),
					super.getDefaultName(), true);
			return new CrossOperator<I1, I2, R>(getInput1(), getInput2(), clean(function), returnType,
					getCrossHint(), Utils.getCallLocationName());
		}

		/**
		 * Initiates a ProjectCross transformation and projects the first cross input.
		 *
		 * <p>If the first cross input is a {@link Tuple} {@link DataSet}, fields can be selected by their index.
		 * If the first cross input is not a Tuple DataSet, no parameters should be passed.
		 *
		 * <p>Fields of the first and second input can be added by chaining the method calls of
		 * {@link org.apache.flink.api.java.operators.CrossOperator.ProjectCross#projectFirst(int...)} and
		 * {@link org.apache.flink.api.java.operators.CrossOperator.ProjectCross#projectSecond(int...)}.
		 *
 		 * <b>Note: With the current implementation, the Project transformation looses type information.</b>
		 *
		 * @param firstFieldIndexes If the first input is a Tuple DataSet, the indexes of the selected fields.
		 * 					   For a non-Tuple DataSet, do not provide parameters.
		 * 					   The order of fields in the output tuple is defined by to the order of field indexes.
		 * @return A ProjectCross which represents the projected cross result.
		 *
		 * @see Tuple
		 * @see DataSet
		 * @see org.apache.flink.api.java.operators.CrossOperator.ProjectCross
		 */
		public <OUT extends Tuple> ProjectCross<I1, I2, OUT> projectFirst(int... firstFieldIndexes) {
			return new CrossProjection<I1, I2>(getInput1(), getInput2(), firstFieldIndexes, null, getCrossHint())
						.projectTupleX();
		}

		/**
		 * Initiates a ProjectCross transformation and projects the second cross input.
		 *
		 * <p>If the second cross input is a {@link Tuple} {@link DataSet}, fields can be selected by their index.
		 * If the second cross input is not a Tuple DataSet, no parameters should be passed.
		 *
		 * <p>Fields of the first and second input can be added by chaining the method calls of
		 * {@link org.apache.flink.api.java.operators.CrossOperator.ProjectCross#projectFirst(int...)} and
		 * {@link org.apache.flink.api.java.operators.CrossOperator.ProjectCross#projectSecond(int...)}.
		 *
		 * <b>Note: With the current implementation, the Project transformation looses type information.</b>
		 *
		 * @param secondFieldIndexes If the second input is a Tuple DataSet, the indexes of the selected fields.
		 * 					   For a non-Tuple DataSet, do not provide parameters.
		 * 					   The order of fields in the output tuple is defined by to the order of field indexes.
		 * @return A ProjectCross which represents the projected cross result.
		 *
		 * @see Tuple
		 * @see DataSet
		 * @see org.apache.flink.api.java.operators.CrossOperator.ProjectCross
		 */
		public <OUT extends Tuple> ProjectCross<I1, I2, OUT> projectSecond(int... secondFieldIndexes) {
			return new CrossProjection<I1, I2>(getInput1(), getInput2(), null, secondFieldIndexes, getCrossHint())
						.projectTupleX();
		}

	}

	/**
	 * A Cross transformation that projects crossing elements or fields of crossing {@link Tuple Tuples}
	 * into result {@link Tuple Tuples}.
	 *
	 * <p>It also represents the {@link DataSet} that is the result of a Cross transformation.
	 *
	 * @param <I1> The type of the first input DataSet of the Cross transformation.
	 * @param <I2> The type of the second input DataSet of the Cross transformation.
	 * @param <OUT> The type of the result of the Cross transformation.
	 *
	 * @see Tuple
	 * @see DataSet
	 */
	@Public
	public static final class ProjectCross<I1, I2, OUT extends Tuple> extends CrossOperator<I1, I2, OUT> {

		private CrossProjection<I1, I2> crossProjection;

		protected ProjectCross(DataSet<I1> input1, DataSet<I2> input2, int[] fields, boolean[] isFromFirst,
				TupleTypeInfo<OUT> returnType, CrossHint hint) {
			super(input1, input2,
					new ProjectCrossFunction<I1, I2, OUT>(fields, isFromFirst, returnType.createSerializer(input1.getExecutionEnvironment().getConfig()).createInstance()),
					returnType, hint, "unknown");

			crossProjection = null;
		}

		protected ProjectCross(DataSet<I1> input1, DataSet<I2> input2, int[] fields, boolean[] isFromFirst,
				TupleTypeInfo<OUT> returnType, CrossProjection<I1, I2> crossProjection, CrossHint hint) {
			super(input1, input2,
				new ProjectCrossFunction<I1, I2, OUT>(fields, isFromFirst, returnType.createSerializer(input1.getExecutionEnvironment().getConfig()).createInstance()),
				returnType, hint, "unknown");

			this.crossProjection = crossProjection;
		}

		@Override
		protected ProjectCrossFunction<I1, I2, OUT> getFunction() {
			return (ProjectCrossFunction<I1, I2, OUT>) super.getFunction();
		}

		/**
		 * Continues a ProjectCross transformation and adds fields of the first cross input to the projection.
		 *
		 * <p>If the first cross input is a {@link Tuple} {@link DataSet}, fields can be selected by their index.
		 * If the first cross input is not a Tuple DataSet, no parameters should be passed.
		 *
		 * <p>Additional fields of the first and second input can be added by chaining the method calls of
		 * {@link org.apache.flink.api.java.operators.CrossOperator.ProjectCross#projectFirst(int...)} and
		 * {@link org.apache.flink.api.java.operators.CrossOperator.ProjectCross#projectSecond(int...)}.
		 *
		 * <p><b>Note: With the current implementation, the Project transformation looses type information.</b>
		 *
		 * @param firstFieldIndexes If the first input is a Tuple DataSet, the indexes of the selected fields.
		 * 					   For a non-Tuple DataSet, do not provide parameters.
		 * 					   The order of fields in the output tuple is defined by to the order of field indexes.
		 * @return A ProjectCross which represents the projected cross result.
		 *
		 * @see Tuple
		 * @see DataSet
		 * @see org.apache.flink.api.java.operators.CrossOperator.ProjectCross
		 */
		@SuppressWarnings("hiding")
		public <OUT extends Tuple> ProjectCross<I1, I2, OUT> projectFirst(int... firstFieldIndexes) {
			crossProjection = crossProjection.projectFirst(firstFieldIndexes);

			return crossProjection.projectTupleX();
		}

		/**
		 * Continues a ProjectCross transformation and adds fields of the second cross input to the projection.
		 *
		 * <p>If the second cross input is a {@link Tuple} {@link DataSet}, fields can be selected by their index.
		 * If the second cross input is not a Tuple DataSet, no parameters should be passed.
		 *
		 * <p>Additional fields of the first and second input can be added by chaining the method calls of
		 * {@link org.apache.flink.api.java.operators.CrossOperator.ProjectCross#projectFirst(int...)} and
		 * {@link org.apache.flink.api.java.operators.CrossOperator.ProjectCross#projectSecond(int...)}.
		 *
		 * <b>Note: With the current implementation, the Project transformation looses type information.</b>
		 *
		 * @param secondFieldIndexes If the second input is a Tuple DataSet, the indexes of the selected fields.
		 * 					   For a non-Tuple DataSet, do not provide parameters.
		 * 					   The order of fields in the output tuple is defined by to the order of field indexes.
		 * @return A ProjectCross which represents the projected cross result.
		 *
		 * @see Tuple
		 * @see DataSet
		 * @see org.apache.flink.api.java.operators.CrossOperator.ProjectCross
		 */
		@SuppressWarnings("hiding")
		public <OUT extends Tuple> ProjectCross<I1, I2, OUT> projectSecond(int... secondFieldIndexes) {
			crossProjection = crossProjection.projectSecond(secondFieldIndexes);

			return crossProjection.projectTupleX();
		}

		/**
		 * @deprecated Deprecated method only kept for compatibility.
		 */
		@SuppressWarnings({ "hiding", "unchecked" })
		@Deprecated
		@PublicEvolving
		public <OUT extends Tuple> CrossOperator<I1, I2, OUT> types(Class<?>... types) {
			TupleTypeInfo<OUT> typeInfo = (TupleTypeInfo<OUT>) this.getResultType();

			if (types.length != typeInfo.getArity()) {
				throw new InvalidProgramException("Provided types do not match projection.");
			}
			for (int i = 0; i < types.length; i++) {
				Class<?> typeClass = types[i];
				if (!typeClass.equals(typeInfo.getTypeAt(i).getTypeClass())) {
					throw new InvalidProgramException("Provided type " + typeClass.getSimpleName() + " at position " + i + " does not match projection");
				}
			}
			return (CrossOperator<I1, I2, OUT>) this;
		}

		@Override
		public CrossOperator<I1, I2, OUT> withForwardedFieldsFirst(String... forwardedFieldsFirst) {
			throw new InvalidProgramException("The semantic properties (forwarded fields) are automatically calculated.");
		}

		@Override
		public CrossOperator<I1, I2, OUT> withForwardedFieldsSecond(String... forwardedFieldsSecond) {
			throw new InvalidProgramException("The semantic properties (forwarded fields) are automatically calculated.");
		}

		@Override
		protected DualInputSemanticProperties extractSemanticAnnotationsFromUdf(Class<?> udfClass) {
			// we do not extract anything, but construct the properties from the projection
			return SemanticPropUtil.createProjectionPropertiesDual(getFunction().getFields(), getFunction().getIsFromFirst(),
					getInput1Type(), getInput2Type());
		}
	}

	/**
	 * @see ProjectCross
	 * @param <T1>
	 * @param <T2>
	 * @param <R>
	 */
	@Internal
	public static final class ProjectCrossFunction<T1, T2, R extends Tuple> implements CrossFunction<T1, T2, R> {

		private static final long serialVersionUID = 1L;

		private final int[] fields;
		private final boolean[] isFromFirst;

		private final R outTuple;

		/**
		 * Instantiates and configures a ProjectCrossFunction.
		 * Creates output tuples by copying fields of crossed input tuples (or a full input object) into an output tuple.
		 *
		 * @param fields List of indexes fields that should be copied to the output tuple.
		 * 					If the full input object should be copied (for example in case of a non-tuple input) the index should be -1.
		 * @param isFromFirst List of flags indicating whether the field should be copied from the first (true) or the second (false) input.
		 * @param outTupleInstance An instance of an output tuple.
		 */
		private ProjectCrossFunction(int[] fields, boolean[] isFromFirst, R outTupleInstance) {

			if (fields.length != isFromFirst.length) {
				throw new IllegalArgumentException("Fields and isFromFirst arrays must have same length!");
			}
			this.fields = fields;
			this.isFromFirst = isFromFirst;
			this.outTuple = outTupleInstance;
		}

		public R cross(T1 in1, T2 in2) {
			for (int i = 0; i < fields.length; i++) {
				if (isFromFirst[i]) {
					if (fields[i] >= 0) {
						outTuple.setField(((Tuple) in1).getField(fields[i]), i);
					} else {
						outTuple.setField(in1, i);
					}
				} else {
					if (fields[i] >= 0) {
						outTuple.setField(((Tuple) in2).getField(fields[i]), i);
					} else {
						outTuple.setField(in2, i);
					}
				}
			}
			return outTuple;
		}

		protected int[] getFields() {
			return fields;
		}

		protected boolean[] getIsFromFirst() {
			return isFromFirst;
		}

	}

	/**
	 * @see ProjectCross
	 * @param <I1>
	 * @param <I2>
	 */
	@Internal
	public static final class CrossProjection<I1, I2> {

		private final DataSet<I1> ds1;
		private final DataSet<I2> ds2;

		private int[] fieldIndexes;
		private boolean[] isFieldInFirst;

		private final int numFieldsDs1;
		private final int numFieldsDs2;

		private final CrossHint hint;

		public CrossProjection(DataSet<I1> ds1, DataSet<I2> ds2, int[] firstFieldIndexes, int[] secondFieldIndexes, CrossHint hint) {

			this.ds1 = ds1;
			this.ds2 = ds2;
			this.hint = hint;

			boolean isFirstTuple;
			boolean isSecondTuple;

			if (ds1.getType() instanceof TupleTypeInfo) {
				numFieldsDs1 = ((TupleTypeInfo<?>) ds1.getType()).getArity();
				isFirstTuple = true;
			} else {
				numFieldsDs1 = 1;
				isFirstTuple = false;
			}
			if (ds2.getType() instanceof TupleTypeInfo) {
				numFieldsDs2 = ((TupleTypeInfo<?>) ds2.getType()).getArity();
				isSecondTuple = true;
			} else {
				numFieldsDs2 = 1;
				isSecondTuple = false;
			}

			boolean isTuple;
			boolean firstInput;

			if (firstFieldIndexes != null && secondFieldIndexes == null) {
				// index array for first input is provided
				firstInput = true;
				isTuple = isFirstTuple;
				this.fieldIndexes = firstFieldIndexes;

				if (this.fieldIndexes.length == 0) {
					// no indexes provided, treat tuple as regular object
					isTuple = false;
				}
			} else if (firstFieldIndexes == null && secondFieldIndexes != null) {
				// index array for second input is provided
				firstInput = false;
				isTuple = isSecondTuple;
				this.fieldIndexes = secondFieldIndexes;

				if (this.fieldIndexes.length == 0) {
					// no indexes provided, treat tuple as regular object
					isTuple = false;
				}
			} else if (firstFieldIndexes == null && secondFieldIndexes == null) {
				throw new IllegalArgumentException("You must provide at least one field index array.");
			} else {
				throw new IllegalArgumentException("You must provide at most one field index array.");
			}

			if (!isTuple && this.fieldIndexes.length != 0) {
				// field index provided for non-Tuple input
				throw new IllegalArgumentException("Input is not a Tuple. Call projectFirst() (or projectSecond()) without arguments to include it.");
			} else if (this.fieldIndexes.length > 22) {
				throw new IllegalArgumentException("You may select only up to twenty-two (22) fields.");
			}

			if (isTuple) {
				this.isFieldInFirst = new boolean[this.fieldIndexes.length];

				// check field indexes and adapt to position in tuple
				int maxFieldIndex = firstInput ? numFieldsDs1 : numFieldsDs2;
				for (int i = 0; i < this.fieldIndexes.length; i++) {
					Preconditions.checkElementIndex(this.fieldIndexes[i], maxFieldIndex);

					if (firstInput) {
						this.isFieldInFirst[i] = true;
					} else {
						this.isFieldInFirst[i] = false;
					}
				}
			} else {
				this.isFieldInFirst = new boolean[]{firstInput};
				this.fieldIndexes = new int[]{-1};
			}

		}

		/**
		 * Continues a ProjectCross transformation and adds fields of the first cross input.
		 *
		 * <p>If the first cross input is a {@link Tuple} {@link DataSet}, fields can be selected by their index.
		 * If the first cross input is not a Tuple DataSet, no parameters should be passed.
		 *
		 * <p>Fields of the first and second input can be added by chaining the method calls of
		 * {@link org.apache.flink.api.java.operators.CrossOperator.CrossProjection#projectFirst(int...)} and
		 * {@link org.apache.flink.api.java.operators.CrossOperator.CrossProjection#projectSecond(int...)}.
		 *
		 * @param firstFieldIndexes If the first input is a Tuple DataSet, the indexes of the selected fields.
		 * 					   For a non-Tuple DataSet, do not provide parameters.
		 * 					   The order of fields in the output tuple is defined by to the order of field indexes.
		 * @return An extended CrossProjection.
		 *
		 * @see Tuple
		 * @see DataSet
		 * @see org.apache.flink.api.java.operators.CrossOperator.CrossProjection
		 * @see org.apache.flink.api.java.operators.CrossOperator.ProjectCross
		 */
		protected CrossProjection<I1, I2> projectFirst(int... firstFieldIndexes) {

			boolean isFirstTuple;

			if (ds1.getType() instanceof TupleTypeInfo && firstFieldIndexes.length > 0) {
				isFirstTuple = true;
			} else {
				isFirstTuple = false;
			}

			if (!isFirstTuple && firstFieldIndexes.length != 0) {
				// field index provided for non-Tuple input
				throw new IllegalArgumentException("Input is not a Tuple. Call projectFirst() without arguments to include it.");
			} else if (firstFieldIndexes.length > (22 - this.fieldIndexes.length)) {
				// to many field indexes provided
				throw new IllegalArgumentException("You may select only up to twenty-two (22) fields in total.");
			}

			int offset = this.fieldIndexes.length;

			if (isFirstTuple) {
				// extend index and flag arrays
				this.fieldIndexes = Arrays.copyOf(this.fieldIndexes, this.fieldIndexes.length + firstFieldIndexes.length);
				this.isFieldInFirst = Arrays.copyOf(this.isFieldInFirst, this.isFieldInFirst.length + firstFieldIndexes.length);

				// copy field indexes
				int maxFieldIndex = numFieldsDs1;
				for (int i = 0; i < firstFieldIndexes.length; i++) {
					// check if indexes in range
					Preconditions.checkElementIndex(firstFieldIndexes[i], maxFieldIndex);

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
		 * Continues a ProjectCross transformation and adds fields of the second cross input.
		 *
		 * <p>If the second cross input is a {@link Tuple} {@link DataSet}, fields can be selected by their index.
		 * If the second cross input is not a Tuple DataSet, no parameters should be passed.
		 *
		 * <p>Fields of the first and second input can be added by chaining the method calls of
		 * {@link org.apache.flink.api.java.operators.CrossOperator.CrossProjection#projectFirst(int...)} and
		 * {@link org.apache.flink.api.java.operators.CrossOperator.CrossProjection#projectSecond(int...)}.
		 *
		 * @param secondFieldIndexes If the second input is a Tuple DataSet, the indexes of the selected fields.
		 * 					   For a non-Tuple DataSet, do not provide parameters.
		 * 					   The order of fields in the output tuple is defined by to the order of field indexes.
		 * @return An extended CrossProjection.
		 *
		 * @see Tuple
		 * @see DataSet
		 * @see org.apache.flink.api.java.operators.CrossOperator.CrossProjection
		 * @see org.apache.flink.api.java.operators.CrossOperator.ProjectCross
		 */
		protected CrossProjection<I1, I2> projectSecond(int... secondFieldIndexes) {

			boolean isSecondTuple;

			if (ds2.getType() instanceof TupleTypeInfo && secondFieldIndexes.length > 0) {
				isSecondTuple = true;
			} else {
				isSecondTuple = false;
			}

			if (!isSecondTuple && secondFieldIndexes.length != 0) {
				// field index provided for non-Tuple input
				throw new IllegalArgumentException("Input is not a Tuple. Call projectSecond() without arguments to include it.");
			} else if (secondFieldIndexes.length > (22 - this.fieldIndexes.length)) {
				// to many field indexes provided
				throw new IllegalArgumentException("You may select only up to twenty-two (22) fields in total.");
			}

			int offset = this.fieldIndexes.length;

			if (isSecondTuple) {
				// extend index and flag arrays
				this.fieldIndexes = Arrays.copyOf(this.fieldIndexes, this.fieldIndexes.length + secondFieldIndexes.length);
				this.isFieldInFirst = Arrays.copyOf(this.isFieldInFirst, this.isFieldInFirst.length + secondFieldIndexes.length);

				// copy field indexes
				int maxFieldIndex = numFieldsDs2;
				for (int i = 0; i < secondFieldIndexes.length; i++) {
					// check if indexes in range
					Preconditions.checkElementIndex(secondFieldIndexes[i], maxFieldIndex);

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
	// GENERATED FROM org.apache.flink.api.java.tuple.TupleGenerator.

		/**
		 * Chooses a projectTupleX according to the length of
		 * {@link org.apache.flink.api.java.operators.CrossOperator.CrossProjection#fieldIndexes}.
		 *
		 * @return The projected DataSet.
		 */
		@SuppressWarnings("unchecked")
		public <OUT extends Tuple> ProjectCross<I1, I2, OUT> projectTupleX() {
			ProjectCross<I1, I2, OUT> projectionCross = null;

			switch (fieldIndexes.length) {
			case 1: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple1(); break;
			case 2: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple2(); break;
			case 3: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple3(); break;
			case 4: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple4(); break;
			case 5: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple5(); break;
			case 6: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple6(); break;
			case 7: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple7(); break;
			case 8: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple8(); break;
			case 9: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple9(); break;
			case 10: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple10(); break;
			case 11: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple11(); break;
			case 12: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple12(); break;
			case 13: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple13(); break;
			case 14: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple14(); break;
			case 15: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple15(); break;
			case 16: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple16(); break;
			case 17: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple17(); break;
			case 18: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple18(); break;
			case 19: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple19(); break;
			case 20: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple20(); break;
			case 21: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple21(); break;
			case 22: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple22(); break;
			case 23: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple23(); break;
			case 24: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple24(); break;
			case 25: projectionCross = (ProjectCross<I1, I2, OUT>) projectTuple25(); break;
			default: throw new IllegalStateException("Excessive arity in tuple.");
			}

			return projectionCross;
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0> ProjectCross<I1, I2, Tuple1<T0>> projectTuple1() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple1<T0>> tType = new TupleTypeInfo<Tuple1<T0>>(fTypes);

			return new ProjectCross<I1, I2, Tuple1<T0>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1> ProjectCross<I1, I2, Tuple2<T0, T1>> projectTuple2() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple2<T0, T1>> tType = new TupleTypeInfo<Tuple2<T0, T1>>(fTypes);

			return new ProjectCross<I1, I2, Tuple2<T0, T1>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2> ProjectCross<I1, I2, Tuple3<T0, T1, T2>> projectTuple3() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple3<T0, T1, T2>> tType = new TupleTypeInfo<Tuple3<T0, T1, T2>>(fTypes);

			return new ProjectCross<I1, I2, Tuple3<T0, T1, T2>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3> ProjectCross<I1, I2, Tuple4<T0, T1, T2, T3>> projectTuple4() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple4<T0, T1, T2, T3>> tType = new TupleTypeInfo<Tuple4<T0, T1, T2, T3>>(fTypes);

			return new ProjectCross<I1, I2, Tuple4<T0, T1, T2, T3>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4> ProjectCross<I1, I2, Tuple5<T0, T1, T2, T3, T4>> projectTuple5() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple5<T0, T1, T2, T3, T4>> tType = new TupleTypeInfo<Tuple5<T0, T1, T2, T3, T4>>(fTypes);

			return new ProjectCross<I1, I2, Tuple5<T0, T1, T2, T3, T4>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5> ProjectCross<I1, I2, Tuple6<T0, T1, T2, T3, T4, T5>> projectTuple6() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple6<T0, T1, T2, T3, T4, T5>> tType = new TupleTypeInfo<Tuple6<T0, T1, T2, T3, T4, T5>>(fTypes);

			return new ProjectCross<I1, I2, Tuple6<T0, T1, T2, T3, T4, T5>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6> ProjectCross<I1, I2, Tuple7<T0, T1, T2, T3, T4, T5, T6>> projectTuple7() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple7<T0, T1, T2, T3, T4, T5, T6>> tType = new TupleTypeInfo<Tuple7<T0, T1, T2, T3, T4, T5, T6>>(fTypes);

			return new ProjectCross<I1, I2, Tuple7<T0, T1, T2, T3, T4, T5, T6>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7> ProjectCross<I1, I2, Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> projectTuple8() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> tType = new TupleTypeInfo<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(fTypes);

			return new ProjectCross<I1, I2, Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8> ProjectCross<I1, I2, Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> projectTuple9() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> tType = new TupleTypeInfo<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(fTypes);

			return new ProjectCross<I1, I2, Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9> ProjectCross<I1, I2, Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> projectTuple10() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> tType = new TupleTypeInfo<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(fTypes);

			return new ProjectCross<I1, I2, Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> ProjectCross<I1, I2, Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> projectTuple11() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> tType = new TupleTypeInfo<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(fTypes);

			return new ProjectCross<I1, I2, Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> ProjectCross<I1, I2, Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> projectTuple12() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> tType = new TupleTypeInfo<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(fTypes);

			return new ProjectCross<I1, I2, Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> ProjectCross<I1, I2, Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> projectTuple13() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> tType = new TupleTypeInfo<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(fTypes);

			return new ProjectCross<I1, I2, Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13> ProjectCross<I1, I2, Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> projectTuple14() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> tType = new TupleTypeInfo<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(fTypes);

			return new ProjectCross<I1, I2, Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14> ProjectCross<I1, I2, Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> projectTuple15() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> tType = new TupleTypeInfo<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(fTypes);

			return new ProjectCross<I1, I2, Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15> ProjectCross<I1, I2, Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> projectTuple16() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> tType = new TupleTypeInfo<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(fTypes);

			return new ProjectCross<I1, I2, Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16> ProjectCross<I1, I2, Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> projectTuple17() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> tType = new TupleTypeInfo<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>(fTypes);

			return new ProjectCross<I1, I2, Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17> ProjectCross<I1, I2, Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> projectTuple18() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> tType = new TupleTypeInfo<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>(fTypes);

			return new ProjectCross<I1, I2, Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18> ProjectCross<I1, I2, Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> projectTuple19() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> tType = new TupleTypeInfo<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>(fTypes);

			return new ProjectCross<I1, I2, Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19> ProjectCross<I1, I2, Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> projectTuple20() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> tType = new TupleTypeInfo<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>(fTypes);

			return new ProjectCross<I1, I2, Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20> ProjectCross<I1, I2, Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> projectTuple21() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> tType = new TupleTypeInfo<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>(fTypes);

			return new ProjectCross<I1, I2, Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21> ProjectCross<I1, I2, Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> projectTuple22() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> tType = new TupleTypeInfo<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>(fTypes);

			return new ProjectCross<I1, I2, Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22> ProjectCross<I1, I2, Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>> projectTuple23() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>> tType = new TupleTypeInfo<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>>(fTypes);

			return new ProjectCross<I1, I2, Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23> ProjectCross<I1, I2, Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>> projectTuple24() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>> tType = new TupleTypeInfo<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>>(fTypes);

			return new ProjectCross<I1, I2, Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		/**
		 * Projects a pair of crossed elements to a {@link Tuple} with the previously selected fields.
		 *
		 * @return The projected data set.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24> ProjectCross<I1, I2, Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>> projectTuple25() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>> tType = new TupleTypeInfo<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>>(fTypes);

			return new ProjectCross<I1, I2, Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>>(this.ds1, this.ds2, this.fieldIndexes, this.isFieldInFirst, tType, this, hint);
		}

		// END_OF_TUPLE_DEPENDENT_CODE
		// -----------------------------------------------------------------------------------------

		private TypeInformation<?>[] extractFieldTypes(int[] fields) {

			TypeInformation<?>[] fieldTypes = new TypeInformation[fields.length];

			for (int i = 0; i < fields.length; i++) {

				TypeInformation<?> typeInfo;
				if (isFieldInFirst[i]) {
					if (fields[i] >= 0) {
						typeInfo = ((TupleTypeInfo<?>) ds1.getType()).getTypeAt(fields[i]);
					} else {
						typeInfo = ds1.getType();
					}
				} else {
					if (fields[i] >= 0) {
						typeInfo = ((TupleTypeInfo<?>) ds2.getType()).getTypeAt(fields[i]);
					} else {
						typeInfo = ds2.getType();
					}
				}

				fieldTypes[i] = typeInfo;
			}

			return fieldTypes;
		}
	}

	// --------------------------------------------------------------------------------------------
	//  default join functions
	// --------------------------------------------------------------------------------------------

	@Internal
	private static final class DefaultCrossFunction<T1, T2> implements CrossFunction<T1, T2, Tuple2<T1, T2>> {

		private static final long serialVersionUID = 1L;

		private final Tuple2<T1, T2> outTuple = new Tuple2<T1, T2>();

		@Override
		public Tuple2<T1, T2> cross(T1 first, T2 second) throws Exception {
			outTuple.f0 = first;
			outTuple.f1 = second;
			return outTuple;
		}
	}
}
