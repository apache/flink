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

import java.security.InvalidParameterException;
import java.util.Arrays;

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.common.functions.GenericJoiner;
import eu.stratosphere.api.common.functions.GenericMap;
import eu.stratosphere.api.common.operators.BinaryOperatorInformation;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.UnaryOperatorInformation;
import eu.stratosphere.api.common.operators.base.JoinOperatorBase;
import eu.stratosphere.api.common.operators.base.MapOperatorBase;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.DeltaIteration.SolutionSetPlaceHolder;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.operators.Keys.FieldPositionKeys;
import eu.stratosphere.api.java.operators.translation.KeyExtractingMapper;
import eu.stratosphere.api.java.operators.translation.PlanUnwrappingJoinOperator;
import eu.stratosphere.api.java.operators.translation.TupleKeyExtractingMapper;
//CHECKSTYLE.OFF: AvoidStarImport - Needed for TupleGenerator
import eu.stratosphere.api.java.tuple.*;
//CHECKSTYLE.ON: AvoidStarImport
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.types.TypeInformation;

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
	
	/**
	 * An enumeration of hints, optionally usable to tell the system how exactly execute the join.
	 */
	public static enum JoinHint {
		/**
		 * leave the choice how to do the join to the optimizer. If in doubt, the
		 * optimizer will choose a repartitioning join.
		 */
		OPTIMIZER_CHOOSES,
		
		/**
		 * Hint that the first join input is much smaller than the second. This results in
		 * broadcasting and hashing the first input, unless the optimizer infers that
		 * prior existing partitioning is available that is even cheaper to exploit.
		 */
		BROADCAST_HASH_FIRST,
		
		/**
		 * Hint that the second join input is much smaller than the second. This results in
		 * broadcasting and hashing the second input, unless the optimizer infers that
		 * prior existing partitioning is available that is even cheaper to exploit.
		 */
		BROADCAST_HASH_SECOND,
		
		/**
		 * Hint that the first join input is a bit smaller than the second. This results in
		 * repartitioning both inputs and hashing the first input, unless the optimizer infers that
		 * prior existing partitioning and orders are available that are even cheaper to exploit.
		 */
		REPARTITION_HASH_FIRST,
		
		/**
		 * Hint that the second join input is a bit smaller than the second. This results in
		 * repartitioning both inputs and hashing the second input, unless the optimizer infers that
		 * prior existing partitioning and orders are available that are even cheaper to exploit.
		 */
		REPARTITION_HASH_SECOND,
		
		/**
		 * Hint that the join should repartitioning both inputs and use sorting and merging
		 * as the join strategy.
		 */
		REPARTITION_SORT_MERGE,
	};
	
	private final Keys<I1> keys1;
	private final Keys<I2> keys2;
	
	private JoinHint joinHint;
	
	protected JoinOperator(DataSet<I1> input1, DataSet<I2> input2, 
			Keys<I1> keys1, Keys<I2> keys2,
			TypeInformation<OUT> returnType, JoinHint hint)
	{
		super(input1, input2, returnType);
		
		if (keys1 == null || keys2 == null) {
			throw new NullPointerException();
		}
		
		this.keys1 = keys1;
		this.keys2 = keys2;
		this.joinHint = hint;
	}
	
	protected Keys<I1> getKeys1() {
		return this.keys1;
	}
	
	protected Keys<I2> getKeys2() {
		return this.keys2;
	}
	
	protected JoinHint getJoinHint() {
		return this.joinHint;
	}
	
	// --------------------------------------------------------------------------------------------
	// special join types
	// --------------------------------------------------------------------------------------------
	
	/**
	 * A Join transformation that applies a {@JoinFunction} on each pair of joining elements.<br/>
	 * It also represents the {@link DataSet} that is the result of a Join transformation. 
	 * 
	 * @param <I1> The type of the first input DataSet of the Join transformation.
	 * @param <I2> The type of the second input DataSet of the Join transformation.
	 * @param <OUT> The type of the result of the Join transformation.
	 * 
	 * @see JoinFunction
	 * @see DataSet
	 */
	public static class EquiJoin<I1, I2, OUT> extends JoinOperator<I1, I2, OUT> {
		
		private final JoinFunction<I1, I2, OUT> function;
		
		@SuppressWarnings("unused")
		private boolean preserve1;
		@SuppressWarnings("unused")
		private boolean preserve2;
		
		protected EquiJoin(DataSet<I1> input1, DataSet<I2> input2, 
				Keys<I1> keys1, Keys<I2> keys2, JoinFunction<I1, I2, OUT> function,
				TypeInformation<OUT> returnType, JoinHint hint)
		{
			super(input1, input2, keys1, keys2, returnType, hint);
			
			if (function == null) {
				throw new NullPointerException();
			}
			
			this.function = function;
			extractSemanticAnnotationsFromUdf(function.getClass());
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
		protected eu.stratosphere.api.common.operators.base.JoinOperatorBase<?, ?, OUT, ?> translateToDataFlow(Operator<I1> input1, Operator<I2> input2) {
			
			String name = getName() != null ? getName() : function.getClass().getName();
			
			if (super.keys1 instanceof Keys.SelectorFunctionKeys 
					&& super.keys2 instanceof Keys.SelectorFunctionKeys
					&& super.keys1.areCompatibale(super.keys2)) {
				
				@SuppressWarnings("unchecked")
				Keys.SelectorFunctionKeys<I1, ?> selectorKeys1 = (Keys.SelectorFunctionKeys<I1, ?>) super.keys1;
				@SuppressWarnings("unchecked")
				Keys.SelectorFunctionKeys<I2, ?> selectorKeys2 = (Keys.SelectorFunctionKeys<I2, ?>) super.keys2;
				
				PlanUnwrappingJoinOperator<I1, I2, OUT, ?> po = 
						translateSelectorFunctionJoin(selectorKeys1, selectorKeys2, function, 
						getInput1Type(), getInput2Type(), getResultType(), name, input1, input2);
				
				// set dop
				po.setDegreeOfParallelism(this.getParallelism());
				
				return po;
				
			}
			else if (super.keys1 instanceof Keys.FieldPositionKeys 
						&& super.keys2 instanceof Keys.FieldPositionKeys)
			{
				if (!super.keys1.areCompatibale(super.keys2)) {
					throw new InvalidProgramException("The types of the key fields do not match.");
				}
				
				int[] logicalKeyPositions1 = super.keys1.computeLogicalKeyPositions();
				int[] logicalKeyPositions2 = super.keys2.computeLogicalKeyPositions();
				
				JoinOperatorBase<I1, I2, OUT, GenericJoiner<I1, I2, OUT>> po =
						new JoinOperatorBase<I1, I2, OUT, GenericJoiner<I1, I2, OUT>>(function,
								new BinaryOperatorInformation<I1, I2, OUT>(getInput1Type(), getInput2Type(), getResultType()),
								logicalKeyPositions1, logicalKeyPositions2,
								name);
				
				// set inputs
				po.setFirstInput(input1);
				po.setSecondInput(input2);
				// set dop
				po.setDegreeOfParallelism(this.getParallelism());
				
				return po;
			}
			else if (super.keys1 instanceof Keys.FieldPositionKeys 
					&& super.keys2 instanceof Keys.SelectorFunctionKeys
					&& super.keys1.areCompatibale(super.keys2)
				) {
			
				int[] logicalKeyPositions1 = super.keys1.computeLogicalKeyPositions();
				
				@SuppressWarnings("unchecked")
				Keys.SelectorFunctionKeys<I2, ?> selectorKeys2 = (Keys.SelectorFunctionKeys<I2, ?>) super.keys2;
				
				PlanUnwrappingJoinOperator<I1, I2, OUT, ?> po = 
						translateSelectorFunctionJoinRight(logicalKeyPositions1, selectorKeys2, function, 
						getInput1Type(), getInput2Type(), getResultType(), name, input1, input2);
				
				// set dop
				po.setDegreeOfParallelism(this.getParallelism());
				
				return po;
			}
			else if (super.keys1 instanceof Keys.SelectorFunctionKeys
					&& super.keys2 instanceof Keys.FieldPositionKeys 
					&& super.keys1.areCompatibale(super.keys2)
				) {
				
				@SuppressWarnings("unchecked")
				Keys.SelectorFunctionKeys<I1, ?> selectorKeys1 = (Keys.SelectorFunctionKeys<I1, ?>) super.keys1;
				
				int[] logicalKeyPositions2 = super.keys2.computeLogicalKeyPositions();
				
				PlanUnwrappingJoinOperator<I1, I2, OUT, ?> po =
						translateSelectorFunctionJoinLeft(selectorKeys1, logicalKeyPositions2, function, 
						getInput1Type(), getInput2Type(), getResultType(), name, input1, input2);
				
				// set dop
				po.setDegreeOfParallelism(this.getParallelism());
				
				return po;
			}
			else {
				throw new UnsupportedOperationException("Unrecognized or incompatible key types.");
			}
			
		}
		
		private static <I1, I2, K, OUT> PlanUnwrappingJoinOperator<I1, I2, OUT, K> translateSelectorFunctionJoin(
				Keys.SelectorFunctionKeys<I1, ?> rawKeys1, Keys.SelectorFunctionKeys<I2, ?> rawKeys2, 
				JoinFunction<I1, I2, OUT> function, 
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

			final MapOperatorBase<I1, Tuple2<K, I1>, GenericMap<I1, Tuple2<K, I1>>> keyMapper1 =
					new MapOperatorBase<I1, Tuple2<K, I1>, GenericMap<I1, Tuple2<K, I1>>>(extractor1, new UnaryOperatorInformation<I1, Tuple2<K, I1>>(inputType1, typeInfoWithKey1), "Key Extractor 1");
			final MapOperatorBase<I2, Tuple2<K, I2>, GenericMap<I2, Tuple2<K, I2>>> keyMapper2 =
					new MapOperatorBase<I2, Tuple2<K, I2>, GenericMap<I2, Tuple2<K, I2>>>(extractor2, new UnaryOperatorInformation<I2, Tuple2<K, I2>>(inputType2, typeInfoWithKey2), "Key Extractor 2");
			final PlanUnwrappingJoinOperator<I1, I2, OUT, K> join = new PlanUnwrappingJoinOperator<I1, I2, OUT, K>(function, keys1, keys2, name, outputType, typeInfoWithKey1, typeInfoWithKey2);
			
			join.setFirstInput(keyMapper1);
			join.setSecondInput(keyMapper2);
			
			keyMapper1.setInput(input1);
			keyMapper2.setInput(input2);
			// set dop
			keyMapper1.setDegreeOfParallelism(input1.getDegreeOfParallelism());
			keyMapper2.setDegreeOfParallelism(input2.getDegreeOfParallelism());
			
			return join;
		}
		
		private static <I1, I2, K, OUT> PlanUnwrappingJoinOperator<I1, I2, OUT, K> translateSelectorFunctionJoinRight(
				int[] logicalKeyPositions1, Keys.SelectorFunctionKeys<I2, ?> rawKeys2, 
				JoinFunction<I1, I2, OUT> function, 
				TypeInformation<I1> inputType1, TypeInformation<I2> inputType2, TypeInformation<OUT> outputType, String name,
				Operator<I1> input1, Operator<I2> input2)
		{
			if(!inputType1.isTupleType()) {
				throw new InvalidParameterException("Should not happen.");
			}
			
			@SuppressWarnings("unchecked")
			final Keys.SelectorFunctionKeys<I2, K> keys2 = (Keys.SelectorFunctionKeys<I2, K>) rawKeys2;
			
			final TypeInformation<Tuple2<K, I1>> typeInfoWithKey1 = new TupleTypeInfo<Tuple2<K, I1>>(keys2.getKeyType(), inputType1); // assume same key, checked by Key.areCompatibale() before
			final TypeInformation<Tuple2<K, I2>> typeInfoWithKey2 = new TupleTypeInfo<Tuple2<K, I2>>(keys2.getKeyType(), inputType2);
			
			final TupleKeyExtractingMapper<I1, K> extractor1 = new TupleKeyExtractingMapper<I1, K>(logicalKeyPositions1[0]);
			final KeyExtractingMapper<I2, K> extractor2 = new KeyExtractingMapper<I2, K>(keys2.getKeyExtractor());

			final MapOperatorBase<I1, Tuple2<K, I1>, GenericMap<I1, Tuple2<K, I1>>> keyMapper1 =
					new MapOperatorBase<I1, Tuple2<K, I1>, GenericMap<I1, Tuple2<K, I1>>>(extractor1, new UnaryOperatorInformation<I1, Tuple2<K, I1>>(inputType1, typeInfoWithKey1), "Key Extractor 1");
			final MapOperatorBase<I2, Tuple2<K, I2>, GenericMap<I2, Tuple2<K, I2>>> keyMapper2 =
					new MapOperatorBase<I2, Tuple2<K, I2>, GenericMap<I2, Tuple2<K, I2>>>(extractor2, new UnaryOperatorInformation<I2, Tuple2<K, I2>>(inputType2, typeInfoWithKey2), "Key Extractor 2");
			
			final PlanUnwrappingJoinOperator<I1, I2, OUT, K> join = new PlanUnwrappingJoinOperator<I1, I2, OUT, K>(function, logicalKeyPositions1, keys2, name, outputType, typeInfoWithKey1, typeInfoWithKey2);
			
			join.setFirstInput(keyMapper1);
			join.setSecondInput(keyMapper2);
			
			keyMapper1.setInput(input1);
			keyMapper2.setInput(input2);
			// set dop
			keyMapper1.setDegreeOfParallelism(input1.getDegreeOfParallelism());
			keyMapper2.setDegreeOfParallelism(input2.getDegreeOfParallelism());
			
			return join;
		}
		
		private static <I1, I2, K, OUT> PlanUnwrappingJoinOperator<I1, I2, OUT, K> translateSelectorFunctionJoinLeft(
				Keys.SelectorFunctionKeys<I1, ?> rawKeys1, int[] logicalKeyPositions2,
				JoinFunction<I1, I2, OUT> function, 
				TypeInformation<I1> inputType1, TypeInformation<I2> inputType2, TypeInformation<OUT> outputType, String name,
				Operator<I1> input1, Operator<I2> input2)
		{
			if(!inputType2.isTupleType()) {
				throw new InvalidParameterException("Should not happen.");
			}
			
			@SuppressWarnings("unchecked")
			final Keys.SelectorFunctionKeys<I1, K> keys1 = (Keys.SelectorFunctionKeys<I1, K>) rawKeys1;
			
			final TypeInformation<Tuple2<K, I1>> typeInfoWithKey1 = new TupleTypeInfo<Tuple2<K, I1>>(keys1.getKeyType(), inputType1); // assume same key, checked by Key.areCompatibale() before
			final TypeInformation<Tuple2<K, I2>> typeInfoWithKey2 = new TupleTypeInfo<Tuple2<K, I2>>(keys1.getKeyType(), inputType2);
			
			final KeyExtractingMapper<I1, K> extractor1 = new KeyExtractingMapper<I1, K>(keys1.getKeyExtractor());
			final TupleKeyExtractingMapper<I2, K> extractor2 = new TupleKeyExtractingMapper<I2, K>(logicalKeyPositions2[0]);

			final MapOperatorBase<I1, Tuple2<K, I1>, GenericMap<I1, Tuple2<K, I1>>> keyMapper1 =
					new MapOperatorBase<I1, Tuple2<K, I1>, GenericMap<I1, Tuple2<K, I1>>>(extractor1, new UnaryOperatorInformation<I1, Tuple2<K, I1>>(inputType1, typeInfoWithKey1), "Key Extractor 1");
			final MapOperatorBase<I2, Tuple2<K, I2>, GenericMap<I2, Tuple2<K, I2>>> keyMapper2 =
					new MapOperatorBase<I2, Tuple2<K, I2>, GenericMap<I2, Tuple2<K, I2>>>(extractor2, new UnaryOperatorInformation<I2, Tuple2<K, I2>>(inputType2, typeInfoWithKey2), "Key Extractor 2");
			
			final PlanUnwrappingJoinOperator<I1, I2, OUT, K> join = new PlanUnwrappingJoinOperator<I1, I2, OUT, K>(function, keys1, logicalKeyPositions2, name, outputType, typeInfoWithKey1, typeInfoWithKey2);
			
			join.setFirstInput(keyMapper1);
			join.setSecondInput(keyMapper2);
			
			keyMapper1.setInput(input1);
			keyMapper2.setInput(input2);
			// set dop
			keyMapper1.setDegreeOfParallelism(input1.getDegreeOfParallelism());
			keyMapper2.setDegreeOfParallelism(input2.getDegreeOfParallelism());
			
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
				Keys<I1> keys1, Keys<I2> keys2, JoinHint hint)
		{
			super(input1, input2, keys1, keys2, 
				(JoinFunction<I1, I2, Tuple2<I1, I2>>) new DefaultJoinFunction<I1, I2>(),
				new TupleTypeInfo<Tuple2<I1, I2>>(input1.getType(), input2.getType()), hint);
		}
		
		/**
		 * Finalizes a Join transformation by applying a {@link JoinFunction} to each pair of joined elements.<br/>
		 * Each JoinFunction call returns exactly one element. 
		 * 
		 * @param function The JoinFunction that is called for each pair of joined elements.
		 * @return An EquiJoin that represents the joined result DataSet
		 * 
		 * @see JoinFunction
		 * @see EquiJoin
		 * @see DataSet
		 */
		public <R> EquiJoin<I1, I2, R> with(JoinFunction<I1, I2, R> function) {
			if (function == null) {
				throw new NullPointerException("Join function must not be null.");
			}
			TypeInformation<R> returnType = TypeExtractor.getJoinReturnTypes(function, getInput1Type(), getInput2Type());
			return new EquiJoin<I1, I2, R>(getInput1(), getInput2(), getKeys1(), getKeys2(), function, returnType, getJoinHint());
		}
		
		/**
		 * Initiates a ProjectJoin transformation and projects the first join input<br/>
		 * If the first join input is a {@link Tuple} {@link DataSet}, fields can be selected by their index.
		 * If the first join input is not a Tuple DataSet, no parameters should be passed.<br/>
		 * 
		 * Fields of the first and second input can be added by chaining the method calls of
		 * {@link JoinProjection#projectFirst(int...)} and {@link JoinProjection#projectSecond(int...)}.
		 * 
		 * @param firstFieldIndexes If the first input is a Tuple DataSet, the indexes of the selected fields.
		 * 					   For a non-Tuple DataSet, do not provide parameters.
		 * 					   The order of fields in the output tuple is defined by to the order of field indexes.
		 * @return A JoinProjection that needs to be converted into a {@link ProjectJoin} to complete the 
		 *           Join transformation by calling {@link JoinProjection#types()}.
		 * 
		 * @see Tuple
		 * @see DataSet
		 * @see JoinProjection
		 * @see ProjectJoin
		 */
		public JoinProjection<I1, I2> projectFirst(int... firstFieldIndexes) {
			return new JoinProjection<I1, I2>(getInput1(), getInput2(), getKeys1(), getKeys2(), getJoinHint(), firstFieldIndexes, null);
		}
		
		/**
		 * Initiates a ProjectJoin transformation and projects the second join input<br/>
		 * If the second join input is a {@link Tuple} {@link DataSet}, fields can be selected by their index.
		 * If the second join input is not a Tuple DataSet, no parameters should be passed.<br/>
		 * 
		 * Fields of the first and second input can be added by chaining the method calls of
		 * {@link JoinProjection#projectFirst(int...)} and {@link JoinProjection#projectSecond(int...)}.
		 * 
		 * @param fieldIndexes If the second input is a Tuple DataSet, the indexes of the selected fields. 
		 * 					   For a non-Tuple DataSet, do not provide parameters.
		 * 					   The order of fields in the output tuple is defined by to the order of field indexes.
		 * @return A JoinProjection that needs to be converted into a {@link ProjectJoin} to complete the 
		 *           Join transformation by calling {@link JoinProjection#types()}.
		 * 
		 * @see Tuple
		 * @see DataSet
		 * @see JoinProjection
		 * @see ProjectJoin
		 */
		public JoinProjection<I1, I2> projectSecond(int... secondFieldIndexes) {
			return new JoinProjection<I1, I2>(getInput1(), getInput2(), getKeys1(), getKeys2(), getJoinHint(), null, secondFieldIndexes);
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
	private static final class ProjectJoin<I1, I2, OUT extends Tuple> extends EquiJoin<I1, I2, OUT> {
		
		protected ProjectJoin(DataSet<I1> input1, DataSet<I2> input2, Keys<I1> keys1, Keys<I2> keys2, JoinHint hint, int[] fields, boolean[] isFromFirst, TupleTypeInfo<OUT> returnType) {
			super(input1, input2, keys1, keys2, 
					new ProjectJoinFunction<I1, I2, OUT>(fields, isFromFirst, returnType.createSerializer().createInstance()), 
					returnType, hint);
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
	 * {@link JoinOperatorSets#where(int...)} or {@link JoinOperatorSets#where(KeySelector)}.
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
		 * @param fields The indexes of the Tuple fields of the first join DataSets that should be used as keys.
		 * @return An incomplete Join transformation. 
		 *           Call {@link JoinOperatorSetsPredicate#equalTo(int...)} or {@link JoinOperatorSetsPredicate#equalTo(KeySelector)}
		 *           to continue the Join. 
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public JoinOperatorSetsPredicate where(int... fields) {
			return new JoinOperatorSetsPredicate(new Keys.FieldPositionKeys<I1>(fields, input1.getType()));
		}
		
		/**
		 * Continues a Join transformation and defines a {@link KeySelector} function for the first join {@link DataSet}.</br>
		 * The KeySelector function is called for each element of the first DataSet and extracts a single 
		 * key value on which the DataSet is joined. </br>
		 * 
		 * @param keySelector The KeySelector function which extracts the key values from the DataSet on which it is joined.
		 * @return An incomplete Join transformation. 
		 *           Call {@link JoinOperatorSetsPredicate#equalTo(int...)} or {@link JoinOperatorSetsPredicate#equalTo(KeySelector)}
		 *           to continue the Join. 
		 * 
		 * @see KeySelector
		 * @see DataSet
		 */
		public <K extends Comparable<K>> JoinOperatorSetsPredicate where(KeySelector<I1, K> keySelector) {
			return new JoinOperatorSetsPredicate(new Keys.SelectorFunctionKeys<I1, K>(keySelector, input1.getType()));
		}
		
		// ----------------------------------------------------------------------------------------
		
		/**
		 * Intermediate step of a Join transformation. <br/>
		 * To continue the Join transformation, select the join key of the second input {@link DataSet} by calling 
		 * {@link JoinOperatorSetsPredicate#equalTo(int...)} or {@link JoinOperatorSetsPredicate#equalTo(KeySelector)}.
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
			 * The resulting {@link DefaultJoin} wraps each pair of joining elements into a {@link Tuple2}, with 
			 * the element of the first input being the first field of the tuple and the element of the 
			 * second input being the second field of the tuple. 
			 * 
			 * @param fields The indexes of the Tuple fields of the second join DataSet that should be used as keys.
			 * @return A DefaultJoin that represents the joined DataSet.
			 */
			public DefaultJoin<I1, I2> equalTo(int... fields) {
				return createJoinOperator(new Keys.FieldPositionKeys<I2>(fields, input2.getType()));
			}

			/**
			 * Continues a Join transformation and defines a {@link KeySelector} function for the second join {@link DataSet}.</br>
			 * The KeySelector function is called for each element of the second DataSet and extracts a single 
			 * key value on which the DataSet is joined. </br>
			 * 
			 * The resulting {@link DefaultJoin} wraps each pair of joining elements into a {@link Tuple2}, with 
			 * the element of the first input being the first field of the tuple and the element of the 
			 * second input being the second field of the tuple. 
			 * 
			 * @param keySelector The KeySelector function which extracts the key values from the second DataSet on which it is joined.
			 * @return A DefaultJoin that represents the joined DataSet.
			 */
			public <K> DefaultJoin<I1, I2> equalTo(KeySelector<I2, K> keySelector) {
				return createJoinOperator(new Keys.SelectorFunctionKeys<I2, K>(keySelector, input2.getType()));
			}
			
			protected DefaultJoin<I1, I2> createJoinOperator(Keys<I2> keys2) {
				if (keys2 == null) {
					throw new NullPointerException("The join keys may not be null.");
				}
				
				if (keys2.isEmpty()) {
					throw new InvalidProgramException("The join keys may not be empty.");
				}
				
				if (!keys1.areCompatibale(keys2)) {
					throw new InvalidProgramException("The pair of join keys are not compatible with each other.");
				}
				
				
				// sanity check solution set key mismatches
				if (input1 instanceof SolutionSetPlaceHolder) {
					if (keys1 instanceof FieldPositionKeys) {
						int[] positions = ((FieldPositionKeys<?>) keys1).computeLogicalKeyPositions();
						((SolutionSetPlaceHolder<?>) input1).checkJoinKeyFields(positions);
					} else {
						throw new InvalidProgramException("Currently, the solution set may only be joined with using tuple field positions.");
					}
				}
				if (input2 instanceof SolutionSetPlaceHolder) {
					if (keys2 instanceof FieldPositionKeys) {
						int[] positions = ((FieldPositionKeys<?>) keys2).computeLogicalKeyPositions();
						((SolutionSetPlaceHolder<?>) input2).checkJoinKeyFields(positions);
					} else {
						throw new InvalidProgramException("Currently, the solution set may only be joined with using tuple field positions.");
					}
				}
				
				
				return new DefaultJoin<I1, I2>(input1, input2, keys1, keys2, joinHint);
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  default join functions
	// --------------------------------------------------------------------------------------------
	
	public static final class DefaultJoinFunction<T1, T2> extends JoinFunction<T1, T2, Tuple2<T1, T2>> {

		private static final long serialVersionUID = 1L;
		private final Tuple2<T1, T2> outTuple = new Tuple2<T1, T2>();

		@Override
		public Tuple2<T1, T2> join(T1 first, T2 second) throws Exception {
			outTuple.f0 = first;
			outTuple.f1 = second;
			return outTuple;
		}
	}
	
	public static final class ProjectJoinFunction<T1, T2, R extends Tuple> extends JoinFunction<T1, T2, R> {
		
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
		private ProjectJoinFunction(int[] fields, boolean[] isFromFirst, R outTupleInstance) {
			
			if(fields.length != isFromFirst.length) {
				throw new IllegalArgumentException("Fields and isFromFirst arrays must have same length!"); 
			}
			this.fields = fields;
			this.isFromFirst = isFromFirst;
			this.outTuple = outTupleInstance;
		}
		
		public R join(T1 in1, T2 in2) {
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
	
	public static final class LeftSemiJoinFunction<T1, T2> extends JoinFunction<T1, T2, T1> {

		private static final long serialVersionUID = 1L;

		@Override
		public T1 join(T1 left, T2 right) throws Exception {
			return left;
		}
	}
	
	public static final class RightSemiJoinFunction<T1, T2> extends JoinFunction<T1, T2, T2> {

		private static final long serialVersionUID = 1L;

		@Override
		public T2 join(T1 left, T2 right) throws Exception {
			return right;
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

		}
		
		/**
		 * Continues a ProjectJoin transformation and adds fields of the first join input.<br/>
		 * If the first join input is a {@link Tuple} {@link DataSet}, fields can be selected by their index.
		 * If the first join input is not a Tuple DataSet, no parameters should be passed.<br/>
		 * 
		 * Fields of the first and second input can be added by chaining the method calls of
		 * {@link JoinProjection#projectFirst(int...)} and {@link JoinProjection#projectSecond(int...)}.
		 * 
		 * @param firstFieldIndexes If the first input is a Tuple DataSet, the indexes of the selected fields.
		 * 					   For a non-Tuple DataSet, do not provide parameters.
		 * 					   The order of fields in the output tuple is defined by to the order of field indexes.
		 * @return A JoinProjection that needs to be converted into a {@link ProjectOperator} to complete the 
		 *           ProjectJoin transformation by calling {@link JoinProjection#types()}.
		 * 
		 * @see Tuple
		 * @see DataSet
		 * @see JoinProjection
		 * @see ProjectJoin
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
		 * Continues a ProjectJoin transformation and adds fields of the second join input.<br/>
		 * If the second join input is a {@link Tuple} {@link DataSet}, fields can be selected by their index.
		 * If the second join input is not a Tuple DataSet, no parameters should be passed.<br/>
		 * 
		 * Fields of the first and second input can be added by chaining the method calls of
		 * {@link JoinProjection#projectFirst(int...)} and {@link JoinProjection#projectSecond(int...)}.
		 * 
		 * @param fieldIndexes If the second input is a Tuple DataSet, the indexes of the selected fields. 
		 * 					   For a non-Tuple DataSet, do not provide parameters.
		 * 					   The order of fields in the output tuple is defined by to the order of field indexes.
		 * @return A JoinProjection that needs to be converted into a {@link ProjectOperator} to complete the 
		 *           ProjectJoin transformation by calling {@link JoinProjection#types()}.
		 * 
		 * @see Tuple
		 * @see DataSet
		 * @see JoinProjection
		 * @see ProjectJoin
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
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0> ProjectJoin<I1, I2, Tuple1<T0>> types(Class<T0> type0) {
			Class<?>[] types = {type0};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple1<T0>> tType = new TupleTypeInfo<Tuple1<T0>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple1<T0>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
		 * Requires the classes of the fields of the resulting tuples. 
		 * 
		 * @param type0 The class of field '0' of the result tuples.
		 * @param type1 The class of field '1' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1> ProjectJoin<I1, I2, Tuple2<T0, T1>> types(Class<T0> type0, Class<T1> type1) {
			Class<?>[] types = {type0, type1};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple2<T0, T1>> tType = new TupleTypeInfo<Tuple2<T0, T1>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple2<T0, T1>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2> ProjectJoin<I1, I2, Tuple3<T0, T1, T2>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2) {
			Class<?>[] types = {type0, type1, type2};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple3<T0, T1, T2>> tType = new TupleTypeInfo<Tuple3<T0, T1, T2>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple3<T0, T1, T2>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2, T3> ProjectJoin<I1, I2, Tuple4<T0, T1, T2, T3>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3) {
			Class<?>[] types = {type0, type1, type2, type3};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple4<T0, T1, T2, T3>> tType = new TupleTypeInfo<Tuple4<T0, T1, T2, T3>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple4<T0, T1, T2, T3>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2, T3, T4> ProjectJoin<I1, I2, Tuple5<T0, T1, T2, T3, T4>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4) {
			Class<?>[] types = {type0, type1, type2, type3, type4};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple5<T0, T1, T2, T3, T4>> tType = new TupleTypeInfo<Tuple5<T0, T1, T2, T3, T4>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple5<T0, T1, T2, T3, T4>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2, T3, T4, T5> ProjectJoin<I1, I2, Tuple6<T0, T1, T2, T3, T4, T5>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple6<T0, T1, T2, T3, T4, T5>> tType = new TupleTypeInfo<Tuple6<T0, T1, T2, T3, T4, T5>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple6<T0, T1, T2, T3, T4, T5>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2, T3, T4, T5, T6> ProjectJoin<I1, I2, Tuple7<T0, T1, T2, T3, T4, T5, T6>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple7<T0, T1, T2, T3, T4, T5, T6>> tType = new TupleTypeInfo<Tuple7<T0, T1, T2, T3, T4, T5, T6>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple7<T0, T1, T2, T3, T4, T5, T6>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2, T3, T4, T5, T6, T7> ProjectJoin<I1, I2, Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> tType = new TupleTypeInfo<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8> ProjectJoin<I1, I2, Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> tType = new TupleTypeInfo<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9> ProjectJoin<I1, I2, Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> tType = new TupleTypeInfo<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> ProjectJoin<I1, I2, Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> tType = new TupleTypeInfo<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> ProjectJoin<I1, I2, Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> tType = new TupleTypeInfo<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> ProjectJoin<I1, I2, Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> tType = new TupleTypeInfo<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13> ProjectJoin<I1, I2, Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> tType = new TupleTypeInfo<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14> ProjectJoin<I1, I2, Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> tType = new TupleTypeInfo<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15> ProjectJoin<I1, I2, Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> tType = new TupleTypeInfo<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16> ProjectJoin<I1, I2, Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> tType = new TupleTypeInfo<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17> ProjectJoin<I1, I2, Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> tType = new TupleTypeInfo<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18> ProjectJoin<I1, I2, Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> tType = new TupleTypeInfo<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19> ProjectJoin<I1, I2, Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> tType = new TupleTypeInfo<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20> ProjectJoin<I1, I2, Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19, Class<T20> type20) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> tType = new TupleTypeInfo<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21> ProjectJoin<I1, I2, Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19, Class<T20> type20, Class<T21> type21) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20, type21};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> tType = new TupleTypeInfo<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		 * @param type22 The class of field '22' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22> ProjectJoin<I1, I2, Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19, Class<T20> type20, Class<T21> type21, Class<T22> type22) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20, type21, type22};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>> tType = new TupleTypeInfo<Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple23<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		 * @param type22 The class of field '22' of the result tuples.
		 * @param type23 The class of field '23' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23> ProjectJoin<I1, I2, Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19, Class<T20> type20, Class<T21> type21, Class<T22> type22, Class<T23> type23) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20, type21, type22, type23};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>> tType = new TupleTypeInfo<Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple24<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
		}

		/**
		 * Projects a pair of joined elements to a {@link Tuple} with the previously selected fields. 
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
		 * @param type22 The class of field '22' of the result tuples.
		 * @param type23 The class of field '23' of the result tuples.
		 * @param type24 The class of field '24' of the result tuples.
		 * @return The projected data set.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24> ProjectJoin<I1, I2, Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19, Class<T20> type20, Class<T21> type21, Class<T22> type22, Class<T23> type23, Class<T24> type24) {
			Class<?>[] types = {type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20, type21, type22, type23, type24};
			if(types.length != this.fieldIndexes.length) {
				throw new IllegalArgumentException("Numbers of projected fields and types do not match.");
			}
			
			TypeInformation<?>[] fTypes = extractFieldTypes(fieldIndexes, types);
			TupleTypeInfo<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>> tType = new TupleTypeInfo<Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>>(fTypes);

			return new ProjectJoin<I1, I2, Tuple25<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24>>(this.ds1, this.ds2, this.keys1, this.keys2, this.hint, this.fieldIndexes, this.isFieldInFirst, tType);
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
