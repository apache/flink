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

import java.security.InvalidParameterException;
import java.util.Arrays;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.api.java.operators.DeltaIteration.SolutionSetPlaceHolder;
import org.apache.flink.api.java.operators.Keys.ExpressionKeys;
import org.apache.flink.api.java.operators.Keys.IncompatibleKeysException;
import org.apache.flink.api.java.operators.translation.KeyExtractingMapper;
import org.apache.flink.api.java.operators.translation.PlanBothUnwrappingJoinOperator;
import org.apache.flink.api.java.operators.translation.PlanLeftUnwrappingJoinOperator;
import org.apache.flink.api.java.operators.translation.PlanRightUnwrappingJoinOperator;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;

//CHECKSTYLE.OFF: AvoidStarImport - Needed for TupleGenerator
import org.apache.flink.api.java.tuple.*;
//CHECKSTYLE.ON: AvoidStarImport

import com.google.common.base.Preconditions;

/**
 * A {@link DataSet} that is the result of a Join transformation. 
 * 
 * @param <I1> The type of the first input DataSet of the Join transformation.
 * @param <I2> The type of the second input DataSet of the Join transformation.
 * @param <OUT> The type of the result of the Join transformation.
 * 
 * @see DataSet
 */
public abstract class JoinOperator<I1, I2, OUT> extends TwoInputUdfOperator<I1, I2, OUT, JoinOperator<I1, I2, OUT>> {
	
	protected final Keys<I1> keys1;
	protected final Keys<I2> keys2;
	
	private final JoinHint joinHint;
	
	private Partitioner<?> customPartitioner;
	
	
	protected JoinOperator(DataSet<I1> input1, DataSet<I2> input2, 
			Keys<I1> keys1, Keys<I2> keys2,
			TypeInformation<OUT> returnType, JoinHint hint)
	{
		super(input1, input2, returnType);
		
		Preconditions.checkNotNull(keys1);
		Preconditions.checkNotNull(keys2);
		
		try {
			if (!keys1.areCompatible(keys2)) {
				throw new InvalidProgramException("The types of the key fields do not match.");
			}
		}
		catch (IncompatibleKeysException ike) {
			throw new InvalidProgramException("The types of the key fields do not match: " + ike.getMessage(), ike);
		}

		// sanity check solution set key mismatches
		if (input1 instanceof SolutionSetPlaceHolder) {
			if (keys1 instanceof ExpressionKeys) {
				int[] positions = ((ExpressionKeys<?>) keys1).computeLogicalKeyPositions();
				((SolutionSetPlaceHolder<?>) input1).checkJoinKeyFields(positions);
			} else {
				throw new InvalidProgramException("Currently, the solution set may only be joined with using tuple field positions.");
			}
		}
		if (input2 instanceof SolutionSetPlaceHolder) {
			if (keys2 instanceof ExpressionKeys) {
				int[] positions = ((ExpressionKeys<?>) keys2).computeLogicalKeyPositions();
				((SolutionSetPlaceHolder<?>) input2).checkJoinKeyFields(positions);
			} else {
				throw new InvalidProgramException("Currently, the solution set may only be joined with using tuple field positions.");
			}
		}

		this.keys1 = keys1;
		this.keys2 = keys2;
		this.joinHint = hint == null ? JoinHint.OPTIMIZER_CHOOSES : hint;
	}
	
	protected Keys<I1> getKeys1() {
		return this.keys1;
	}
	
	protected Keys<I2> getKeys2() {
		return this.keys2;
	}
	
	/**
	 * Gets the JoinHint that describes how the join is executed.
	 * 
	 * @return The JoinHint.
	 */
	public JoinHint getJoinHint() {
		return this.joinHint;
	}
	
	/**
	 * Sets a custom partitioner for this join. The partitioner will be called on the join keys to determine
	 * the partition a key should be assigned to. The partitioner is evaluated on both join inputs in the
	 * same way.
	 * <p>
	 * NOTE: A custom partitioner can only be used with single-field join keys, not with composite join keys.
	 * 
	 * @param partitioner The custom partitioner to be used.
	 * @return This join operator, to allow for function chaining.
	 */
	public JoinOperator<I1, I2, OUT> withPartitioner(Partitioner<?> partitioner) {
		if (partitioner != null) {
			keys1.validateCustomPartitioner(partitioner, null);
			keys2.validateCustomPartitioner(partitioner, null);
		}
		this.customPartitioner = partitioner;
		return this;
	}
	
	/**
	 * Gets the custom partitioner used by this join, or {@code null}, if none is set.
	 * 
	 * @return The custom partitioner used by this join;
	 */
	public Partitioner<?> getPartitioner() {
		return customPartitioner;
	}
	
	// --------------------------------------------------------------------------------------------
	// special join types
	// --------------------------------------------------------------------------------------------
	
	/**
	 * A Join transformation that applies a {@link JoinFunction} on each pair of joining elements.<br/>
	 * It also represents the {@link DataSet} that is the result of a Join transformation. 
	 * 
	 * @param <I1> The type of the first input DataSet of the Join transformation.
	 * @param <I2> The type of the second input DataSet of the Join transformation.
	 * @param <OUT> The type of the result of the Join transformation.
	 * 
	 * @see org.apache.flink.api.common.functions.RichFlatJoinFunction
	 * @see DataSet
	 */
	public static class EquiJoin<I1, I2, OUT> extends JoinOperator<I1, I2, OUT> {
		
		private final FlatJoinFunction<I1, I2, OUT> function;
		
		@SuppressWarnings("unused")
		private boolean preserve1;
		@SuppressWarnings("unused")
		private boolean preserve2;
		
		private final String joinLocationName;
		
		public EquiJoin(DataSet<I1> input1, DataSet<I2> input2,
				Keys<I1> keys1, Keys<I2> keys2, FlatJoinFunction<I1, I2, OUT> function,
				TypeInformation<OUT> returnType, JoinHint hint, String joinLocationName)
		{
			super(input1, input2, keys1, keys2, returnType, hint);
			
			if (function == null) {
				throw new NullPointerException();
			}
			
			this.function = function;
			this.joinLocationName = joinLocationName;

			if (!(function instanceof ProjectFlatJoinFunction)) {
				extractSemanticAnnotationsFromUdf(function.getClass());
			} else {
				generateProjectionProperties(((ProjectFlatJoinFunction<?, ?, ?>) function));
			}
		}

		public EquiJoin(DataSet<I1> input1, DataSet<I2> input2,
				Keys<I1> keys1, Keys<I2> keys2, FlatJoinFunction<I1, I2, OUT> generatedFunction, JoinFunction<I1, I2, OUT> function,
				TypeInformation<OUT> returnType, JoinHint hint, String joinLocationName)
		{
			super(input1, input2, keys1, keys2, returnType, hint);
			
			this.joinLocationName = joinLocationName;

			if (function == null) {
				throw new NullPointerException();
			}

			this.function = generatedFunction;

			if (!(generatedFunction instanceof ProjectFlatJoinFunction)) {
				extractSemanticAnnotationsFromUdf(function.getClass());
			} else {
				generateProjectionProperties(((ProjectFlatJoinFunction<?, ?, ?>) generatedFunction));
			}
		}

		public void generateProjectionProperties(ProjectFlatJoinFunction<?, ?, ?> pjf) {
			DualInputSemanticProperties props = SemanticPropUtil.createProjectionPropertiesDual(pjf.getFields(), pjf.getIsFromFirst());
			setSemanticProperties(props);
		}

		// TODO
//		public EquiJoin<I1, I2, OUT> leftOuter() {
//			this.preserve1 = true;
//			return this;
//		}

		// TODO
//		public EquiJoin<I1, I2, OUT> rightOuter() {
//			this.preserve2 = true;
//			return this;
//		}
		
		// TODO
//		public EquiJoin<I1, I2, OUT> fullOuter() {
//			this.preserve1 = true;
//			this.preserve2 = true;
//			return this;
//		}
		
		@Override
		protected JoinOperatorBase<?, ?, OUT, ?> translateToDataFlow(Operator<I1> input1, Operator<I2> input2) {

			String name = getName() != null ? getName() : "Join at "+joinLocationName;

			final JoinOperatorBase<?, ?, OUT, ?> translated;
			
			if (keys1 instanceof Keys.SelectorFunctionKeys && keys2 instanceof Keys.SelectorFunctionKeys) {
				// Both join sides have a key selector function, so we need to do the
				// tuple wrapping/unwrapping on both sides.

				@SuppressWarnings("unchecked")
				Keys.SelectorFunctionKeys<I1, ?> selectorKeys1 = (Keys.SelectorFunctionKeys<I1, ?>) keys1;
				@SuppressWarnings("unchecked")
				Keys.SelectorFunctionKeys<I2, ?> selectorKeys2 = (Keys.SelectorFunctionKeys<I2, ?>) keys2;
				
				PlanBothUnwrappingJoinOperator<I1, I2, OUT, ?> po =
						translateSelectorFunctionJoin(selectorKeys1, selectorKeys2, function, 
						getInput1Type(), getInput2Type(), getResultType(), name, input1, input2);
				
				// set dop
				po.setDegreeOfParallelism(this.getParallelism());
				
				translated = po;
			}
			else if (keys2 instanceof Keys.SelectorFunctionKeys) {
				// The right side of the join needs the tuple wrapping/unwrapping

				int[] logicalKeyPositions1 = keys1.computeLogicalKeyPositions();

				@SuppressWarnings("unchecked")
				Keys.SelectorFunctionKeys<I2, ?> selectorKeys2 =
						(Keys.SelectorFunctionKeys<I2, ?>) keys2;

				PlanRightUnwrappingJoinOperator<I1, I2, OUT, ?> po =
						translateSelectorFunctionJoinRight(logicalKeyPositions1, selectorKeys2,
								function, getInput1Type(), getInput2Type(), getResultType(), name,
								input1, input2);

				// set dop
				po.setDegreeOfParallelism(this.getParallelism());

				translated = po;
			}
			else if (keys1 instanceof Keys.SelectorFunctionKeys) {
				// The left side of the join needs the tuple wrapping/unwrapping


				@SuppressWarnings("unchecked")
				Keys.SelectorFunctionKeys<I1, ?> selectorKeys1 =
						(Keys.SelectorFunctionKeys<I1, ?>) keys1;

				int[] logicalKeyPositions2 = keys2.computeLogicalKeyPositions();

				PlanLeftUnwrappingJoinOperator<I1, I2, OUT, ?> po =
						translateSelectorFunctionJoinLeft(selectorKeys1, logicalKeyPositions2, function,
								getInput1Type(), getInput2Type(), getResultType(), name, input1, input2);

				// set dop
				po.setDegreeOfParallelism(this.getParallelism());

				translated = po;
			}
			else if (super.keys1 instanceof Keys.ExpressionKeys && super.keys2 instanceof Keys.ExpressionKeys)
			{
				// Neither side needs the tuple wrapping/unwrapping

				int[] logicalKeyPositions1 = super.keys1.computeLogicalKeyPositions();
				int[] logicalKeyPositions2 = super.keys2.computeLogicalKeyPositions();
				
				JoinOperatorBase<I1, I2, OUT, FlatJoinFunction<I1, I2, OUT>> po =
						new JoinOperatorBase<I1, I2, OUT, FlatJoinFunction<I1, I2, OUT>>(function,
								new BinaryOperatorInformation<I1, I2, OUT>(getInput1Type(), getInput2Type(), getResultType()),
								logicalKeyPositions1, logicalKeyPositions2,
								name);
				
				// set inputs
				po.setFirstInput(input1);
				po.setSecondInput(input2);
				// set dop
				po.setDegreeOfParallelism(this.getParallelism());
				
				translated = po;
			}
			else {
				throw new UnsupportedOperationException("Unrecognized or incompatible key types.");
			}
			
			translated.setJoinHint(getJoinHint());
			translated.setCustomPartitioner(getPartitioner());
			
			return translated;
		}
		
		private static <I1, I2, K, OUT> PlanBothUnwrappingJoinOperator<I1, I2, OUT, K> translateSelectorFunctionJoin(
				Keys.SelectorFunctionKeys<I1, ?> rawKeys1, Keys.SelectorFunctionKeys<I2, ?> rawKeys2, 
				FlatJoinFunction<I1, I2, OUT> function,
				TypeInformation<I1> inputType1, TypeInformation<I2> inputType2, TypeInformation<OUT> outputType, String name,
				Operator<I1> input1, Operator<I2> input2)
		{
			@SuppressWarnings("unchecked")
			final Keys.SelectorFunctionKeys<I1, K> keys1 = (Keys.SelectorFunctionKeys<I1, K>) rawKeys1;
			@SuppressWarnings("unchecked")
			final Keys.SelectorFunctionKeys<I2, K> keys2 = (Keys.SelectorFunctionKeys<I2, K>) rawKeys2;
			
			final TypeInformation<Tuple2<K, I1>> typeInfoWithKey1 = new TupleTypeInfo<Tuple2<K, I1>>(keys1.getKeyType(), inputType1);
			final TypeInformation<Tuple2<K, I2>> typeInfoWithKey2 = new TupleTypeInfo<Tuple2<K, I2>>(keys2.getKeyType(), inputType2);
			
			final KeyExtractingMapper<I1, K> extractor1 = new KeyExtractingMapper<I1, K>(keys1.getKeyExtractor());
			final KeyExtractingMapper<I2, K> extractor2 = new KeyExtractingMapper<I2, K>(keys2.getKeyExtractor());

			final MapOperatorBase<I1, Tuple2<K, I1>, MapFunction<I1, Tuple2<K, I1>>> keyMapper1 =
					new MapOperatorBase<I1, Tuple2<K, I1>, MapFunction<I1, Tuple2<K, I1>>>(extractor1, new UnaryOperatorInformation<I1, Tuple2<K, I1>>(inputType1, typeInfoWithKey1), "Key Extractor 1");
			final MapOperatorBase<I2, Tuple2<K, I2>, MapFunction<I2, Tuple2<K, I2>>> keyMapper2 =
					new MapOperatorBase<I2, Tuple2<K, I2>, MapFunction<I2, Tuple2<K, I2>>>(extractor2, new UnaryOperatorInformation<I2, Tuple2<K, I2>>(inputType2, typeInfoWithKey2), "Key Extractor 2");
			final PlanBothUnwrappingJoinOperator<I1, I2, OUT, K> join = new PlanBothUnwrappingJoinOperator<I1, I2, OUT, K>(function, keys1, keys2, name, outputType, typeInfoWithKey1, typeInfoWithKey2);
			
			join.setFirstInput(keyMapper1);
			join.setSecondInput(keyMapper2);
			
			keyMapper1.setInput(input1);
			keyMapper2.setInput(input2);
			// set dop
			keyMapper1.setDegreeOfParallelism(input1.getDegreeOfParallelism());
			keyMapper2.setDegreeOfParallelism(input2.getDegreeOfParallelism());
			
			return join;
		}
		
		private static <I1, I2, K, OUT> PlanRightUnwrappingJoinOperator<I1, I2, OUT, K> translateSelectorFunctionJoinRight(
				int[] logicalKeyPositions1,
				Keys.SelectorFunctionKeys<I2, ?> rawKeys2,
				FlatJoinFunction<I1, I2, OUT> function,
				TypeInformation<I1> inputType1,
				TypeInformation<I2> inputType2,
				TypeInformation<OUT> outputType,
				String name,
				Operator<I1> input1,
				Operator<I2> input2) {

			if(!inputType1.isTupleType()) {
				throw new InvalidParameterException("Should not happen.");
			}
			
			@SuppressWarnings("unchecked")
			final Keys.SelectorFunctionKeys<I2, K> keys2 =
					(Keys.SelectorFunctionKeys<I2, K>) rawKeys2;
			
			final TypeInformation<Tuple2<K, I2>> typeInfoWithKey2 =
					new TupleTypeInfo<Tuple2<K, I2>>(keys2.getKeyType(), inputType2);
			
			final KeyExtractingMapper<I2, K> extractor2 =
					new KeyExtractingMapper<I2, K>(keys2.getKeyExtractor());

			final MapOperatorBase<I2, Tuple2<K, I2>, MapFunction<I2, Tuple2<K, I2>>> keyMapper2 =
					new MapOperatorBase<I2, Tuple2<K, I2>, MapFunction<I2, Tuple2<K, I2>>>(
							extractor2,
							new UnaryOperatorInformation<I2,Tuple2<K, I2>>(inputType2, typeInfoWithKey2),
							"Key Extractor 2");
			
			final PlanRightUnwrappingJoinOperator<I1, I2, OUT, K> join =
					new PlanRightUnwrappingJoinOperator<I1, I2, OUT, K>(
							function,
							logicalKeyPositions1,
							keys2,
							name,
							outputType,
							inputType1,
							typeInfoWithKey2);
			
			join.setFirstInput(input1);
			join.setSecondInput(keyMapper2);
			
			keyMapper2.setInput(input2);
			// set dop
			keyMapper2.setDegreeOfParallelism(input2.getDegreeOfParallelism());
			
			return join;
		}
		
		private static <I1, I2, K, OUT> PlanLeftUnwrappingJoinOperator<I1, I2, OUT, K> translateSelectorFunctionJoinLeft(
				Keys.SelectorFunctionKeys<I1, ?> rawKeys1,
				int[] logicalKeyPositions2,
				FlatJoinFunction<I1, I2, OUT> function,
				TypeInformation<I1> inputType1,
				TypeInformation<I2> inputType2,
				TypeInformation<OUT> outputType,
				String name,
				Operator<I1> input1,
				Operator<I2> input2) {

			if(!inputType2.isTupleType()) {
				throw new InvalidParameterException("Should not happen.");
			}
			
			@SuppressWarnings("unchecked")
			final Keys.SelectorFunctionKeys<I1, K> keys1 = (Keys.SelectorFunctionKeys<I1, K>) rawKeys1;
			
			final TypeInformation<Tuple2<K, I1>> typeInfoWithKey1 =
					new TupleTypeInfo<Tuple2<K, I1>>(keys1.getKeyType(), inputType1);

			final KeyExtractingMapper<I1, K> extractor1 =
					new KeyExtractingMapper<I1, K>(keys1.getKeyExtractor());

			final MapOperatorBase<I1, Tuple2<K, I1>, MapFunction<I1, Tuple2<K, I1>>> keyMapper1 =
					new MapOperatorBase<I1, Tuple2<K, I1>, MapFunction<I1, Tuple2<K, I1>>>(
							extractor1,
							new UnaryOperatorInformation<I1, Tuple2<K, I1>>(inputType1, typeInfoWithKey1),
							"Key Extractor 1");

			final PlanLeftUnwrappingJoinOperator<I1, I2, OUT, K> join =
					new PlanLeftUnwrappingJoinOperator<I1, I2, OUT, K>(
							function,
							keys1,
							logicalKeyPositions2,
							name,
							outputType,
							typeInfoWithKey1,
							inputType2);
			
			join.setFirstInput(keyMapper1);
			join.setSecondInput(input2);
			
			keyMapper1.setInput(input1);
			// set dop
			keyMapper1.setDegreeOfParallelism(input1.getDegreeOfParallelism());

			return join;
		}
	}
	
	/**
	 * A Join transformation that wraps pairs of joining elements into {@link Tuple2}.<br/>
	 * It also represents the {@link DataSet} that is the result of a Join transformation. 
	 * 
	 * @param <I1> The type of the first input DataSet of the Join transformation.
	 * @param <I2> The type of the second input DataSet of the Join transformation.
	 *
	 * @see Tuple2
	 * @see DataSet
	 */
	public static final class DefaultJoin<I1, I2> extends EquiJoin<I1, I2, Tuple2<I1, I2>> {

		protected DefaultJoin(DataSet<I1> input1, DataSet<I2> input2, 
				Keys<I1> keys1, Keys<I2> keys2, JoinHint hint, String joinLocationName)
		{
			super(input1, input2, keys1, keys2, 
				(RichFlatJoinFunction<I1, I2, Tuple2<I1, I2>>) new DefaultFlatJoinFunction<I1, I2>(),
				new TupleTypeInfo<Tuple2<I1, I2>>(input1.getType(), input2.getType()), hint, joinLocationName);
		}
		
		/**
		 * Finalizes a Join transformation by applying a {@link org.apache.flink.api.common.functions.RichFlatJoinFunction} to each pair of joined elements.<br/>
		 * Each JoinFunction call returns exactly one element. 
		 * 
		 * @param function The JoinFunction that is called for each pair of joined elements.
		 * @return An EquiJoin that represents the joined result DataSet
		 * 
		 * @see org.apache.flink.api.common.functions.RichFlatJoinFunction
		 * @see org.apache.flink.api.java.operators.JoinOperator.EquiJoin
		 * @see DataSet
		 */
		public <R> EquiJoin<I1, I2, R> with(FlatJoinFunction<I1, I2, R> function) {
			if (function == null) {
				throw new NullPointerException("Join function must not be null.");
			}
			TypeInformation<R> returnType = TypeExtractor.getFlatJoinReturnTypes(function, getInput1Type(), getInput2Type());
			return new EquiJoin<I1, I2, R>(getInput1(), getInput2(), getKeys1(), getKeys2(), function, returnType, getJoinHint(), Utils.getCallLocationName());
		}

		public <R> EquiJoin<I1, I2, R> with (JoinFunction<I1, I2, R> function) {
			if (function == null) {
				throw new NullPointerException("Join function must not be null.");
			}
			FlatJoinFunction<I1, I2, R> generatedFunction = new WrappingFlatJoinFunction<I1, I2, R>(function);
			TypeInformation<R> returnType = TypeExtractor.getJoinReturnTypes(function, getInput1Type(), getInput2Type());
			return new EquiJoin<I1, I2, R>(getInput1(), getInput2(), getKeys1(), getKeys2(), generatedFunction, function, returnType, getJoinHint(), Utils.getCallLocationName());
		}

		public static class WrappingFlatJoinFunction<IN1, IN2, OUT> extends WrappingFunction<JoinFunction<IN1,IN2,OUT>> implements FlatJoinFunction<IN1, IN2, OUT> {

			private static final long serialVersionUID = 1L;

			public WrappingFlatJoinFunction(JoinFunction<IN1, IN2, OUT> wrappedFunction) {
				super(wrappedFunction);
			}

			@Override
			public void join(IN1 left, IN2 right, Collector<OUT> out) throws Exception {
				out.collect (this.wrappedFunction.join(left, right));
			}
		}

		/**
		 * Initiates a ProjectJoin transformation and projects the first join input<br/>
		 * If the first join input is a {@link Tuple} {@link DataSet}, fields can be selected by their index.
		 * If the first join input is not a Tuple DataSet, no parameters should be passed.<br/>
		 * 
		 * Fields of the first and second input can be added by chaining the method calls of
		 * {@link org.apache.flink.api.java.operators.JoinOperator.ProjectJoin#projectFirst(int...)} and
		 * {@link org.apache.flink.api.java.operators.JoinOperator.ProjectJoin#projectSecond(int...)}.
		 * 
		 * @param firstFieldIndexes If the first input is a Tuple DataSet, the indexes of the selected fields.
		 * 					   For a non-Tuple DataSet, do not provide parameters.
		 * 					   The order of fields in the output tuple is defined by to the order of field indexes.
		 * @return A ProjectJoin to complete the Join transformation.
		 * 
		 * @see Tuple
		 * @see DataSet
		 * @see org.apache.flink.api.java.operators.JoinOperator.JoinProjection
		 * @see org.apache.flink.api.java.operators.JoinOperator.ProjectJoin
		 */
		public <OUT extends Tuple> ProjectJoin<I1, I2, OUT> projectFirst(int... firstFieldIndexes) {
			JoinProjection<I1, I2> joinProjection = new JoinProjection<I1, I2>(getInput1(), getInput2(), getKeys1(), getKeys2(), getJoinHint(), firstFieldIndexes, null);
			
			return joinProjection.projectTupleX();
		}
		
		/**
		 * Initiates a ProjectJoin transformation and projects the second join input<br/>
		 * If the second join input is a {@link Tuple} {@link DataSet}, fields can be selected by their index.
		 * If the second join input is not a Tuple DataSet, no parameters should be passed.<br/>
		 * 
		 * Fields of the first and second input can be added by chaining the method calls of
		 * {@link org.apache.flink.api.java.operators.JoinOperator.ProjectJoin#projectionFirst(int...)} and
		 * {@link org.apache.flink.api.java.operators.JoinOperator.ProjectJoin#projectionSecond(int...)}.
		 * 
		 * @param secondFieldIndexes If the second input is a Tuple DataSet, the indexes of the selected fields. 
		 * 					   For a non-Tuple DataSet, do not provide parameters.
		 * 					   The order of fields in the output tuple is defined by to the order of field indexes.
		 * @return A ProjectJoin to complete the Join transformation.
		 * 
		 * @see Tuple
		 * @see DataSet
		 * @see org.apache.flink.api.java.operators.JoinOperator.JoinProjection
		 * @see org.apache.flink.api.java.operators.JoinOperator.ProjectJoin
		 */
		public <OUT extends Tuple> ProjectJoin<I1, I2, OUT> projectSecond(int... secondFieldIndexes) {
			JoinProjection<I1, I2> joinProjection = new JoinProjection<I1, I2>(getInput1(), getInput2(), getKeys1(), getKeys2(), getJoinHint(), null, secondFieldIndexes);
			
			return joinProjection.projectTupleX();
		}
		
//		public JoinOperator<I1, I2, I1> leftSemiJoin() {
//			return new LeftSemiJoin<I1, I2>(getInput1(), getInput2(), getKeys1(), getKeys2(), getJoinHint());
//		}
		
//		public JoinOperator<I1, I2, I2> rightSemiJoin() {
//			return new RightSemiJoin<I1, I2>(getInput1(), getInput2(), getKeys1(), getKeys2(), getJoinHint());
//		}
		
//		public JoinOperator<I1, I2, I1> leftAntiJoin() {
//			return new LeftAntiJoin<I1, I2>(getInput1(), getInput2(), getKeys1(), getKeys2(), getJoinHint());
//		}
		
//		public JoinOperator<I1, I2, I2> rightAntiJoin() {
//			return new RightAntiJoin<I1, I2>(getInput1(), getInput2(), getKeys1(), getKeys2(), getJoinHint());
//		}
	}
	
	/**
	 * A Join transformation that projects joining elements or fields of joining {@link Tuple Tuples} 
	 * into result {@link Tuple Tuples}. <br/>
	 * It also represents the {@link DataSet} that is the result of a Join transformation. 
	 * 
	 * @param <I1> The type of the first input DataSet of the Join transformation.
	 * @param <I2> The type of the second input DataSet of the Join transformation.
	 * @param <OUT> The type of the result of the Join transformation.
	 * 
	 * @see Tuple
	 * @see DataSet
	 */
	public static class ProjectJoin<I1, I2, OUT extends Tuple> extends EquiJoin<I1, I2, OUT> {
		
		private JoinProjection<I1, I2> joinProj;
		
		protected ProjectJoin(DataSet<I1> input1, DataSet<I2> input2, Keys<I1> keys1, Keys<I2> keys2, JoinHint hint, int[] fields, boolean[] isFromFirst, TupleTypeInfo<OUT> returnType) {
			super(input1, input2, keys1, keys2, 
					new ProjectFlatJoinFunction<I1, I2, OUT>(fields, isFromFirst, returnType.createSerializer().createInstance()),
					returnType, hint, Utils.getCallLocationName(4)); // We need to use the 4th element in the stack because the call comes through .types().

			
			joinProj = null;
		}
		
		protected ProjectJoin(DataSet<I1> input1, DataSet<I2> input2, Keys<I1> keys1, Keys<I2> keys2, JoinHint hint, int[] fields, boolean[] isFromFirst, TupleTypeInfo<OUT> returnType, JoinProjection<I1, I2> joinProj) {
			super(input1, input2, keys1, keys2, 
					new ProjectFlatJoinFunction<I1, I2, OUT>(fields, isFromFirst, returnType.createSerializer().createInstance()),
					returnType, hint, Utils.getCallLocationName(4));
			
			this.joinProj = joinProj;
		}
		
		@SuppressWarnings("hiding")
		public <OUT extends Tuple> ProjectJoin<I1, I2, OUT> projectFirst(int... firstFieldIndexes) {	
			joinProj = joinProj.projectFirst(firstFieldIndexes);
			
			return joinProj.projectTupleX();
		}
		
		@SuppressWarnings("hiding")
		public <OUT extends Tuple> ProjectJoin<I1, I2, OUT> projectSecond(int... secondFieldIndexes) {
			joinProj = joinProj.projectSecond(secondFieldIndexes);
			
			return joinProj.projectTupleX();
		}

		@SuppressWarnings({ "unchecked", "hiding" })
		@Deprecated
		public <OUT extends Tuple> ProjectJoin<I1, I2, OUT> types(Class<?>... types) {
			return (ProjectJoin<I1, I2, OUT>) this;
		}
		
		@Override
		public JoinOperator<I1, I2, OUT> withConstantSetFirst(String... constantSetFirst) {
			throw new InvalidProgramException("The semantic properties (constant fields and forwarded fields) are automatically calculated.");
		}

		@Override
		public JoinOperator<I1, I2, OUT> withConstantSetSecond(String... constantSetSecond) {
			throw new InvalidProgramException("The semantic properties (constant fields and forwarded fields) are automatically calculated.");
		}
	}
	
//	@SuppressWarnings("unused")
//	private static final class LeftAntiJoin<I1, I2> extends JoinOperator<I1, I2, I1> {
//		
//		protected LeftAntiJoin(DataSet<I1> input1, DataSet<I2> input2, Keys<I1> keys1, Keys<I2> keys2, JoinHint hint) {
//			super(input1, input2, keys1, keys2, input1.getType(), hint);
//		}
//		
//		@Override
//		protected Operator<I1> translateToDataFlow(Operator<I1> input1, Operator<I2> input2) {
//			throw new UnsupportedOperationException("LeftAntiJoin operator currently not supported.");
//		}
//	}
	
//	@SuppressWarnings("unused")
//	private static final class RightAntiJoin<I1, I2> extends JoinOperator<I1, I2, I2> {
//		
//		protected RightAntiJoin(DataSet<I1> input1, DataSet<I2> input2, Keys<I1> keys1, Keys<I2> keys2, JoinHint hint) {
//			super(input1, input2, keys1, keys2, input2.getType(), hint);
//		}
//		
//		@Override
//		protected Operator<I2> translateToDataFlow(Operator<I1> input1, Operator<I2> input2) {
//			throw new UnsupportedOperationException("RightAntiJoin operator currently not supported.");
//		}
//	}
	
//	@SuppressWarnings("unused")
//	private static final class LeftSemiJoin<I1, I2> extends EquiJoin<I1, I2, I1> {
//		
//		protected LeftSemiJoin(DataSet<I1> input1, DataSet<I2> input2, Keys<I1> keys1, Keys<I2> keys2, JoinHint hint) {
//			super(input1, input2, keys1, keys2, new LeftSemiJoinFunction<I1, I2>(), input1.getType(), hint);
//		}
//		
//		@Override
//		protected Operator<I1> translateToDataFlow(Operator<I1> input1, Operator<I2> input2) {
//			// TODO: Runtime support required. Each left tuple may be returned only once.
//			// 	     Special exec strategy (runtime + optimizer) based on hash join required. 
//			// 		 Either no duplicates of right side in HT or left tuples removed from HT after first match.
//			throw new UnsupportedOperationException("LeftSemiJoin operator currently not supported.");
//		}
//	}
	
//	@SuppressWarnings("unused")
//	private static final class RightSemiJoin<I1, I2> extends EquiJoin<I1, I2, I2> {
//		
//		protected RightSemiJoin(DataSet<I1> input1, DataSet<I2> input2, Keys<I1> keys1, Keys<I2> keys2, JoinHint hint) {
//			super(input1, input2, keys1, keys2, new RightSemiJoinFunction<I1, I2>(), input2.getType(), hint);
//		}
//		
//		@Override
//		protected Operator<I2> translateToDataFlow(Operator<I1> input1, Operator<I2> input2) {
//			// TODO: Runtime support required. Each right tuple may be returned only once.
//			// 	     Special exec strategy (runtime + optimizer) based on hash join required. 
//			// 		 Either no duplicates of left side in HT or right tuples removed from HT after first match.
//			throw new UnsupportedOperationException("RightSemiJoin operator currently not supported.");
//		}
//	}
	
	// --------------------------------------------------------------------------------------------
	// Builder classes for incremental construction
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Intermediate step of a Join transformation. <br/>
	 * To continue the Join transformation, select the join key of the first input {@link DataSet} by calling 
	 * {@link org.apache.flink.api.java.operators.JoinOperator.JoinOperatorSets#where(int...)} or
	 * {@link org.apache.flink.api.java.operators.JoinOperator.JoinOperatorSets#where(KeySelector)}.
	 *
	 * @param <I1> The type of the first input DataSet of the Join transformation.
	 * @param <I2> The type of the second input DataSet of the Join transformation.
	 */
	public static final class JoinOperatorSets<I1, I2> {
		
		private final DataSet<I1> input1;
		private final DataSet<I2> input2;
		
		private final JoinHint joinHint;
		
		public JoinOperatorSets(DataSet<I1> input1, DataSet<I2> input2) {
			this(input1, input2, JoinHint.OPTIMIZER_CHOOSES);
		}
		
		public JoinOperatorSets(DataSet<I1> input1, DataSet<I2> input2, JoinHint hint) {
			if (input1 == null || input2 == null) {
				throw new NullPointerException();
			}
			
			this.input1 = input1;
			this.input2 = input2;
			this.joinHint = hint;
		}
		
		/**
		 * Continues a Join transformation. <br/>
		 * Defines the {@link Tuple} fields of the first join {@link DataSet} that should be used as join keys.<br/>
		 * <b>Note: Fields can only be selected as join keys on Tuple DataSets.</b><br/>
		 *
		 * @param fields The indexes of the other Tuple fields of the first join DataSets that should be used as keys.
		 * @return An incomplete Join transformation. 
		 *           Call {@link org.apache.flink.api.java.operators.JoinOperator.JoinOperatorSets.JoinOperatorSetsPredicate#equalTo(int...)} or
		 *           {@link org.apache.flink.api.java.operators.JoinOperator.JoinOperatorSets.JoinOperatorSetsPredicate#equalTo(KeySelector)}
		 *           to continue the Join. 
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public JoinOperatorSetsPredicate where(int... fields) {
			return new JoinOperatorSetsPredicate(new Keys.ExpressionKeys<I1>(fields, input1.getType()));
		}

		/**
		 * Continues a Join transformation. <br/>
		 * Defines the fields of the first join {@link DataSet} that should be used as grouping keys. Fields
		 * are the names of member fields of the underlying type of the data set.
		 *
		 * @param fields The  fields of the first join DataSets that should be used as keys.
		 * @return An incomplete Join transformation.
		 *           Call {@link org.apache.flink.api.java.operators.JoinOperator.JoinOperatorSets.JoinOperatorSetsPredicate#equalTo(int...)} or
		 *           {@link org.apache.flink.api.java.operators.JoinOperator.JoinOperatorSets.JoinOperatorSetsPredicate#equalTo(KeySelector)}
		 *           to continue the Join.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public JoinOperatorSetsPredicate where(String... fields) {
			return new JoinOperatorSetsPredicate(new Keys.ExpressionKeys<I1>(fields, input1.getType()));
		}
		
		/**
		 * Continues a Join transformation and defines a {@link KeySelector} function for the first join {@link DataSet}.</br>
		 * The KeySelector function is called for each element of the first DataSet and extracts a single 
		 * key value on which the DataSet is joined. </br>
		 * 
		 * @param keySelector The KeySelector function which extracts the key values from the DataSet on which it is joined.
		 * @return An incomplete Join transformation. 
		 *           Call {@link org.apache.flink.api.java.operators.JoinOperator.JoinOperatorSets.JoinOperatorSetsPredicate#equalTo(int...)} or
		 *           {@link org.apache.flink.api.java.operators.JoinOperator.JoinOperatorSets.JoinOperatorSetsPredicate#equalTo(KeySelector)}
		 *           to continue the Join. 
		 * 
		 * @see KeySelector
		 * @see DataSet
		 */
		public <K> JoinOperatorSetsPredicate where(KeySelector<I1, K> keySelector) {
			TypeInformation<K> keyType = TypeExtractor.getKeySelectorTypes(keySelector, input1.getType());
			return new JoinOperatorSetsPredicate(new Keys.SelectorFunctionKeys<I1, K>(keySelector, input1.getType(), keyType));
		}
		
		// ----------------------------------------------------------------------------------------
		
		/**
		 * Intermediate step of a Join transformation. <br/>
		 * To continue the Join transformation, select the join key of the second input {@link DataSet} by calling 
		 * {@link org.apache.flink.api.java.operators.JoinOperator.JoinOperatorSets.JoinOperatorSetsPredicate#equalTo(int...)} or
		 * {@link org.apache.flink.api.java.operators.JoinOperator.JoinOperatorSets.JoinOperatorSetsPredicate#equalTo(KeySelector)}.
		 *
		 */
		public class JoinOperatorSetsPredicate {
			
			private final Keys<I1> keys1;
			
			private JoinOperatorSetsPredicate(Keys<I1> keys1) {
				if (keys1 == null) {
					throw new NullPointerException();
				}
				
				if (keys1.isEmpty()) {
					throw new InvalidProgramException("The join keys must not be empty.");
				}
				
				this.keys1 = keys1;
			}
			
			/**
			 * Continues a Join transformation and defines the {@link Tuple} fields of the second join 
			 * {@link DataSet} that should be used as join keys.<br/>
			 * <b>Note: Fields can only be selected as join keys on Tuple DataSets.</b><br/>
			 * 
			 * The resulting {@link org.apache.flink.api.java.operators.JoinOperator.DefaultJoin} wraps each pair of joining elements into a {@link Tuple2}, with 
			 * the element of the first input being the first field of the tuple and the element of the 
			 * second input being the second field of the tuple. 
			 *
			 * @param fields The indexes of the Tuple fields of the second join DataSet that should be used as keys.
			 * @return A DefaultJoin that represents the joined DataSet.
			 */
			public DefaultJoin<I1, I2> equalTo(int... fields) {
				return createJoinOperator(new Keys.ExpressionKeys<I2>(fields, input2.getType()));
			}

			/**
			 * Continues a Join transformation and defines the  fields of the second join
			 * {@link DataSet} that should be used as join keys.<br/>
			 *
			 * The resulting {@link org.apache.flink.api.java.operators.JoinOperator.DefaultJoin} wraps each pair of joining elements into a {@link Tuple2}, with
			 * the element of the first input being the first field of the tuple and the element of the
			 * second input being the second field of the tuple.
			 *
			 * @param fields The fields of the second join DataSet that should be used as keys.
			 * @return A DefaultJoin that represents the joined DataSet.
			 */
			public DefaultJoin<I1, I2> equalTo(String... fields) {
				return createJoinOperator(new Keys.ExpressionKeys<I2>(fields, input2.getType()));
			}

			/**
			 * Continues a Join transformation and defines a {@link KeySelector} function for the second join {@link DataSet}.</br>
			 * The KeySelector function is called for each element of the second DataSet and extracts a single 
			 * key value on which the DataSet is joined. </br>
			 * 
			 * The resulting {@link org.apache.flink.api.java.operators.JoinOperator.DefaultJoin} wraps each pair of joining elements into a {@link Tuple2}, with 
			 * the element of the first input being the first field of the tuple and the element of the 
			 * second input being the second field of the tuple. 
			 * 
			 * @param keySelector The KeySelector function which extracts the key values from the second DataSet on which it is joined.
			 * @return A DefaultJoin that represents the joined DataSet.
			 */
			public <K> DefaultJoin<I1, I2> equalTo(KeySelector<I2, K> keySelector) {
				TypeInformation<K> keyType = TypeExtractor.getKeySelectorTypes(keySelector, input2.getType());
				return createJoinOperator(new Keys.SelectorFunctionKeys<I2, K>(keySelector, input2.getType(), keyType));
			}
			
			protected DefaultJoin<I1, I2> createJoinOperator(Keys<I2> keys2) {
				if (keys2 == null) {
					throw new NullPointerException("The join keys may not be null.");
				}
				
				if (keys2.isEmpty()) {
					throw new InvalidProgramException("The join keys may not be empty.");
				}
				
				try {
					keys1.areCompatible(keys2);
				} catch (IncompatibleKeysException e) {
					throw new InvalidProgramException("The pair of join keys are not compatible with each other.",e);
				}

				return new DefaultJoin<I1, I2>(input1, input2, keys1, keys2, joinHint, Utils.getCallLocationName(4));
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  default join functions
	// --------------------------------------------------------------------------------------------
	
	public static final class DefaultFlatJoinFunction<T1, T2> extends RichFlatJoinFunction<T1, T2, Tuple2<T1, T2>> {

		private static final long serialVersionUID = 1L;
		private final Tuple2<T1, T2> outTuple = new Tuple2<T1, T2>();

		@Override
		public void join(T1 first, T2 second, Collector<Tuple2<T1,T2>> out) throws Exception {
			outTuple.f0 = first;
			outTuple.f1 = second;
			out.collect(outTuple);
		}
	}
	
	public static final class ProjectFlatJoinFunction<T1, T2, R extends Tuple> extends RichFlatJoinFunction<T1, T2, R> {
		
		private static final long serialVersionUID = 1L;
		
		private final int[] fields;
		private final boolean[] isFromFirst;
		private final R outTuple;
	
		/**
		 * Instantiates and configures a ProjectJoinFunction.
		 * Creates output tuples by copying fields of joined input tuples (or a full input object) into an output tuple.
		 * 
		 * @param fields List of indexes fields that should be copied to the output tuple. 
		 * 					If the full input object should be copied (for example in case of a non-tuple input) the index should be -1. 
		 * @param isFromFirst List of flags indicating whether the field should be copied from the first (true) or the second (false) input.
		 * @param outTupleInstance An instance of an output tuple.
		 */
		private ProjectFlatJoinFunction(int[] fields, boolean[] isFromFirst, R outTupleInstance) {
			
			if(fields.length != isFromFirst.length) {
				throw new IllegalArgumentException("Fields and isFromFirst arrays must have same length!"); 
			}
			this.fields = fields;
			this.isFromFirst = isFromFirst;
			this.outTuple = outTupleInstance;
		}

		protected int[] getFields() {
			return fields;
		}

		protected boolean[] getIsFromFirst() {
			return isFromFirst;
		}

		public void join(T1 in1, T2 in2, Collector<R> out) {
			for(int i=0; i<fields.length; i++) {
				if(isFromFirst[i]) {
					if(fields[i] >= 0 && in1 != null) {
						outTuple.setField(((Tuple)in1).getField(fields[i]), i);
					} else {
						outTuple.setField(in1, i);
					}
				} else {
					if(fields[i] >= 0 && in2 != null) {
						outTuple.setField(((Tuple)in2).getField(fields[i]), i);
					} else {
						outTuple.setField(in2, i);
					}
				}
			}
			out.collect(outTuple);
		}
		
	}
	
	public static final class JoinProjection<I1, I2> {
		
		private final DataSet<I1> ds1;
		private final DataSet<I2> ds2;
		private final Keys<I1> keys1;
		private final Keys<I2> keys2;
		private final JoinHint hint;
		
		private int[] fieldIndexes;
		private boolean[] isFieldInFirst;
		
		private final int numFieldsDs1;
		private final int numFieldsDs2;
		
		public JoinProjection(DataSet<I1> ds1, DataSet<I2> ds2, Keys<I1> keys1, Keys<I2> keys2, JoinHint hint, int[] firstFieldIndexes, int[] secondFieldIndexes) {
			
			this.ds1 = ds1;
			this.ds2 = ds2;
			this.keys1 = keys1;
			this.keys2 = keys2;
			this.hint = hint;
			
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
				throw new IllegalArgumentException("Input is not a Tuple. Call projectFirst() (or projectSecond()) without arguments to include it.");
			} else if(this.fieldIndexes.length > 22) {
				throw new IllegalArgumentException("You may select only up to twenty-two (22) fields.");
			}
			
			if(isTuple) {
				this.isFieldInFirst = new boolean[this.fieldIndexes.length];
				
				// check field indexes and adapt to position in tuple
				int maxFieldIndex = firstInput ? numFieldsDs1 : numFieldsDs2;
				for(int i=0; i<this.fieldIndexes.length; i++) {
					Preconditions.checkElementIndex(this.fieldIndexes[i], maxFieldIndex);

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

		}
		
		/**
		 * Continues a ProjectJoin transformation and adds fields of the first join input.<br/>
		 * If the first join input is a {@link Tuple} {@link DataSet}, fields can be selected by their index.
		 * If the first join input is not a Tuple DataSet, no parameters should be passed.<br/>
		 * 
		 * Fields of the first and second input can be added by chaining the method calls of
		 * {@link org.apache.flink.api.java.operators.JoinOperator.JoinProjection#projectFirst(int...)} and 
		 * {@link org.apache.flink.api.java.operators.JoinOperator.JoinProjection#projectSecond(int...)}.
		 * 
		 * @param firstFieldIndexes If the first input is a Tuple DataSet, the indexes of the selected fields.
		 * 					   For a non-Tuple DataSet, do not provide parameters.
		 * 					   The order of fields in the output tuple is defined by to the order of field indexes.
		 * @return A JoinProjection that needs to be converted into a {@link ProjectOperator} to complete the 
		 *           ProjectJoin transformation by calling the corresponding {@code types()} function.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public JoinProjection<I1, I2> projectFirst(int... firstFieldIndexes) {
			
			boolean isFirstTuple;
			
			if(ds1.getType() instanceof TupleTypeInfo && firstFieldIndexes.length > 0) {
				isFirstTuple = true;
			} else {
				isFirstTuple = false;
			}
			
			if(!isFirstTuple && firstFieldIndexes.length != 0) {
				// field index provided for non-Tuple input
				throw new IllegalArgumentException("Input is not a Tuple. Call projectFirst() without arguments to include it.");
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
		 * Continues a ProjectJoin transformation and adds fields of the second join input.<br/>
		 * If the second join input is a {@link Tuple} {@link DataSet}, fields can be selected by their index.
		 * If the second join input is not a Tuple DataSet, no parameters should be passed.<br/>
		 * 
		 * Fields of the first and second input can be added by chaining the method calls of
		 * {@link org.apache.flink.api.java.operators.JoinOperator.JoinProjection#projectFirst(int...)} and
		 * {@link org.apache.flink.api.java.operators.JoinOperator.JoinProjection#projectSecond(int...)}.
		 * 
		 * @param secondFieldIndexes If the second input is a Tuple DataSet, the indexes of the selected fields. 
		 * 					   For a non-Tuple DataSet, do not provide parameters.
		 * 					   The order of fields in the output tuple is defined by to the order of field indexes.
		 * @return A JoinProjection that needs to be converted into a {@link ProjectOperator} to complete the 
		 *           ProjectJoin transformation by calling the corresponding {@code types()} function.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public JoinProjection<I1, I2> projectSecond(int... secondFieldIndexes) {
			
			boolean isSecondTuple;
			
			if(ds2.getType() instanceof TupleTypeInfo && secondFieldIndexes.length > 0) {
				isSecondTuple = true;
			} else {
				isSecondTuple = false;
			}
			
			if(!isSecondTuple && secondFieldIndexes.length != 0) {
				// field index provided for non-Tuple input
				throw new IllegalArgumentException("Input is not a Tuple. Call projectSecond() without arguments to include it.");
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
		 * Chooses a projectTupleX according to the length of {@link JoinProjection#fieldIndexes} 
		 * 
		 * @return The projected DataSet.
		 * 
		 * @see Projection
		 */
		@SuppressWarnings("unchecked")
		public <OUT extends Tuple> ProjectJoin<I1, I2, OUT> projectTupleX() {
			ProjectJoin<I1, I2, OUT> projectJoin = null;

			switch (fieldIndexes.length) {
			case 1: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple1(); break;
			case 2: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple2(); break;
			case 3: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple3(); break;
			case 4: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple4(); break;
			case 5: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple5(); break;
			case 6: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple6(); break;
			case 7: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple7(); break;
			case 8: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple8(); break;
			case 9: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple9(); break;
			case 10: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple10(); break;
			case 11: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple11(); break;
			case 12: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple12(); break;
			case 13: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple13(); break;
			case 14: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple14(); break;
			case 15: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple15(); break;
			case 16: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple16(); break;
			case 17: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple17(); break;
			case 18: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple18(); break;
			case 19: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple19(); break;
			case 20: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple20(); break;
			case 21: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple21(); break;
			case 22: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple22(); break;
			case 23: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple23(); break;
			case 24: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple24(); break;
			case 25: projectJoin = (ProjectJoin<I1, I2, OUT>) projectTuple25(); break;
			default: throw new IllegalStateException("Excessive arity in tuple.");
			}

			return projectJoin;
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0> ProjectJoin<I1, I2, Tuple1<T0>> projectTuple1() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple1<T0>> tType = new TupleTypeInfo<Tuple1<T0>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple1<T0>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1> ProjectJoin<I1, I2, Tuple2<T0, T1>> projectTuple2() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple2<T0, T1>> tType = new TupleTypeInfo<Tuple2<T0, T1>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple2<T0, T1>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2> ProjectJoin<I1, I2, Tuple3<T0, T1, T2>> projectTuple3() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple3<T0, T1, T2>> tType = new TupleTypeInfo<Tuple3<T0, T1, T2>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple3<T0, T1, T2>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3> ProjectJoin<I1, I2, Tuple4<T0, T1, T2, T3>> projectTuple4() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple4<T0, T1, T2, T3>> tType = new TupleTypeInfo<Tuple4<T0, T1, T2, T3>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple4<T0, T1, T2, T3>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4> ProjectJoin<I1, I2, Tuple5<T0, T1, T2, T3, T4>> projectTuple5() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple5<T0, T1, T2, T3, T4>> tType = new TupleTypeInfo<Tuple5<T0, T1, T2, T3, T4>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple5<T0, T1, T2, T3, T4>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5> ProjectJoin<I1, I2, Tuple6<T0, T1, T2, T3, T4, T5>> projectTuple6() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple6<T0, T1, T2, T3, T4, T5>> tType = new TupleTypeInfo<Tuple6<T0, T1, T2, T3, T4, T5>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple6<T0, T1, T2, T3, T4, T5>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6> ProjectJoin<I1, I2, Tuple7<T0, T1, T2, T3, T4, T5, T6>> projectTuple7() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple7<T0, T1, T2, T3, T4, T5, T6>> tType = new TupleTypeInfo<Tuple7<T0, T1, T2, T3, T4, T5, T6>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple7<T0, T1, T2, T3, T4, T5, T6>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7> ProjectJoin<I1, I2, Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> projectTuple8() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> tType = new TupleTypeInfo<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8> ProjectJoin<I1, I2, Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> projectTuple9() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> tType = new TupleTypeInfo<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9> ProjectJoin<I1, I2, Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> projectTuple10() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> tType = new TupleTypeInfo<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> ProjectJoin<I1, I2, Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> projectTuple11() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> tType = new TupleTypeInfo<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> ProjectJoin<I1, I2, Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> projectTuple12() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> tType = new TupleTypeInfo<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> ProjectJoin<I1, I2, Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> projectTuple13() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> tType = new TupleTypeInfo<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13> ProjectJoin<I1, I2, Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> projectTuple14() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> tType = new TupleTypeInfo<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14> ProjectJoin<I1, I2, Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> projectTuple15() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> tType = new TupleTypeInfo<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15> ProjectJoin<I1, I2, Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> projectTuple16() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> tType = new TupleTypeInfo<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16> ProjectJoin<I1, I2, Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> projectTuple17() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> tType = new TupleTypeInfo<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17> ProjectJoin<I1, I2, Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> projectTuple18() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> tType = new TupleTypeInfo<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18> ProjectJoin<I1, I2, Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> projectTuple19() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> tType = new TupleTypeInfo<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19> ProjectJoin<I1, I2, Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> projectTuple20() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> tType = new TupleTypeInfo<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20> ProjectJoin<I1, I2, Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> projectTuple21() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> tType = new TupleTypeInfo<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21> ProjectJoin<I1, I2, Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> projectTuple22() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> tType = new TupleTypeInfo<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22> ProjectJoin<I1, I2, Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>> projectTuple23() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>> tType = new TupleTypeInfo<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23> ProjectJoin<I1, I2, Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>> projectTuple24() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>> tType = new TupleTypeInfo<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24> ProjectJoin<I1, I2, Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>> projectTuple25() {
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes);
			TupleTypeInfo<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>> tType = new TupleTypeInfo<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType, this);
		}

		// END_OF_TUPLE_DEPENDENT_CODE
		// -----------------------------------------------------------------------------------------
		
		private TypeInformation<?>[] extractFieldTypes(int[] fields) {
			
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

				fieldTypes[i] = typeInfo;
			}
			
			return fieldTypes;
		}
				
	}
}
