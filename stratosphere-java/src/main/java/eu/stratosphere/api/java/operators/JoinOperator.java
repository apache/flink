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

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.operators.translation.BinaryNodeTranslation;
import eu.stratosphere.api.java.operators.translation.KeyExtractingMapper;
import eu.stratosphere.api.java.operators.translation.PlanJoinOperator;
import eu.stratosphere.api.java.operators.translation.PlanMapOperator;
import eu.stratosphere.api.java.operators.translation.PlanUnwrappingJoinOperator;
import eu.stratosphere.api.java.operators.translation.TupleKeyExtractingMapper;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.api.java.typeutils.TypeInformation;

/**
 *
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
		
		if (keys1 == null || keys2 == null)
			throw new NullPointerException();
		
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
	
	public static class EquiJoin<I1, I2, OUT> extends JoinOperator<I1, I2, OUT> {
		
		private final JoinFunction<I1, I2, OUT> function;
		
		private boolean preserve1;
		private boolean preserve2;
		
		protected EquiJoin(DataSet<I1> input1, DataSet<I2> input2, 
				Keys<I1> keys1, Keys<I2> keys2, JoinFunction<I1, I2, OUT> function,
				TypeInformation<OUT> returnType, JoinHint hint)
		{
			super(input1, input2, keys1, keys2, returnType, hint);
			
			if (function == null)
				throw new NullPointerException();
			
			this.function = function;
		}
		
		
		public EquiJoin<I1, I2, OUT> leftOuter() {
			this.preserve1 = true;
			return this;
		}

		public EquiJoin<I1, I2, OUT> rightOuter() {
			this.preserve2 = true;
			return this;
		}
		
		public EquiJoin<I1, I2, OUT> fullOuter() {
			this.preserve1 = true;
			this.preserve2 = true;
			return this;
		}
		
		@Override
		protected BinaryNodeTranslation translateToDataFlow() {

			String name = getName() != null ? getName() : function.getClass().getName();
			
			if (super.keys1 instanceof Keys.SelectorFunctionKeys 
					&& super.keys2 instanceof Keys.SelectorFunctionKeys
					&& super.keys1.areCompatibale(super.keys2)) {
				
				@SuppressWarnings("unchecked")
				Keys.SelectorFunctionKeys<I1, ?> selectorKeys1 = (Keys.SelectorFunctionKeys<I1, ?>) super.keys1;
				@SuppressWarnings("unchecked")
				Keys.SelectorFunctionKeys<I2, ?> selectorKeys2 = (Keys.SelectorFunctionKeys<I2, ?>) super.keys2;
				
				return translateSelectorFunctionJoin(selectorKeys1, selectorKeys2, function, 
						getInput1Type(), getInput2Type(), getResultType(), name);
				
			}
			else if (super.keys1 instanceof Keys.FieldPositionKeys 
						&& super.keys2 instanceof Keys.FieldPositionKeys 
						&& super.keys1.areCompatibale(super.keys2)
					) {
				
				int[] logicalKeyPositions1 = super.keys1.computeLogicalKeyPositions();
				int[] logicalKeyPositions2 = super.keys2.computeLogicalKeyPositions();
				
				return new BinaryNodeTranslation(
						new PlanJoinOperator<I1, I2, OUT>(function, logicalKeyPositions1, logicalKeyPositions2, 
								name, getInput1Type(), getInput2Type(), getResultType()));
			}
			else if (super.keys1 instanceof Keys.FieldPositionKeys 
					&& super.keys2 instanceof Keys.SelectorFunctionKeys
					&& super.keys1.areCompatibale(super.keys2)
				) {
			
				int[] logicalKeyPositions1 = super.keys1.computeLogicalKeyPositions();
				
				@SuppressWarnings("unchecked")
				Keys.SelectorFunctionKeys<I2, ?> selectorKeys2 = (Keys.SelectorFunctionKeys<I2, ?>) super.keys2;
				
				return translateSelectorFunctionJoinRight(logicalKeyPositions1, selectorKeys2, function, 
						getInput1Type(), getInput2Type(), getResultType(), name);
			}
			else if (super.keys1 instanceof Keys.SelectorFunctionKeys
					&& super.keys2 instanceof Keys.FieldPositionKeys 
					&& super.keys1.areCompatibale(super.keys2)
				) {
				
				@SuppressWarnings("unchecked")
				Keys.SelectorFunctionKeys<I1, ?> selectorKeys1 = (Keys.SelectorFunctionKeys<I1, ?>) super.keys1;
				
				int[] logicalKeyPositions2 = super.keys2.computeLogicalKeyPositions();
				
				return translateSelectorFunctionJoinLeft(selectorKeys1, logicalKeyPositions2, function, 
						getInput1Type(), getInput2Type(), getResultType(), name);
			}
			else {
				throw new UnsupportedOperationException("Unrecognized or incompatible key types.");
			}
		}
		
		private static <I1, I2, K, OUT> BinaryNodeTranslation translateSelectorFunctionJoin(
				Keys.SelectorFunctionKeys<I1, ?> rawKeys1, Keys.SelectorFunctionKeys<I2, ?> rawKeys2, 
				JoinFunction<I1, I2, OUT> function, 
				TypeInformation<I1> inputType1, TypeInformation<I2> inputType2, TypeInformation<OUT> outputType, String name)
		{
			@SuppressWarnings("unchecked")
			final Keys.SelectorFunctionKeys<I1, K> keys1 = (Keys.SelectorFunctionKeys<I1, K>) rawKeys1;
			@SuppressWarnings("unchecked")
			final Keys.SelectorFunctionKeys<I2, K> keys2 = (Keys.SelectorFunctionKeys<I2, K>) rawKeys2;
			
			final TypeInformation<Tuple2<K, I1>> typeInfoWithKey1 = new TupleTypeInfo<Tuple2<K, I1>>(keys1.getKeyType(), inputType1);
			final TypeInformation<Tuple2<K, I2>> typeInfoWithKey2 = new TupleTypeInfo<Tuple2<K, I2>>(keys2.getKeyType(), inputType2);
			
			final KeyExtractingMapper<I1, K> extractor1 = new KeyExtractingMapper<I1, K>(keys1.getKeyExtractor());
			final KeyExtractingMapper<I2, K> extractor2 = new KeyExtractingMapper<I2, K>(keys2.getKeyExtractor());
			
			final PlanMapOperator<I1, Tuple2<K, I1>> keyMapper1 = new PlanMapOperator<I1, Tuple2<K, I1>>(extractor1, "Key Extractor 1", inputType1, typeInfoWithKey1);
			final PlanMapOperator<I2, Tuple2<K, I2>> keyMapper2 = new PlanMapOperator<I2, Tuple2<K, I2>>(extractor2, "Key Extractor 2", inputType2, typeInfoWithKey2);
			final PlanUnwrappingJoinOperator<I1, I2, OUT, K> join = new PlanUnwrappingJoinOperator<I1, I2, OUT, K>(function, keys1, keys2, name, outputType, typeInfoWithKey1, typeInfoWithKey2);
			
			join.addFirstInput(keyMapper1);
			join.addSecondInput(keyMapper2);
			
			return new BinaryNodeTranslation(keyMapper1, keyMapper2, join);
		}
		
		private static <I1, I2, K, OUT> BinaryNodeTranslation translateSelectorFunctionJoinRight(
				int[] logicalKeyPositions1, Keys.SelectorFunctionKeys<I2, ?> rawKeys2, 
				JoinFunction<I1, I2, OUT> function, 
				TypeInformation<I1> inputType1, TypeInformation<I2> inputType2, TypeInformation<OUT> outputType, String name)
		{
			if(!inputType1.isTupleType())
				throw new InvalidParameterException("Should not happen.");
			
			@SuppressWarnings("unchecked")
			final Keys.SelectorFunctionKeys<I2, K> keys2 = (Keys.SelectorFunctionKeys<I2, K>) rawKeys2;
			
			final TypeInformation<Tuple2<K, I1>> typeInfoWithKey1 = new TupleTypeInfo<Tuple2<K, I1>>(keys2.getKeyType(), inputType1); // assume same key, checked by Key.areCompatibale() before
			final TypeInformation<Tuple2<K, I2>> typeInfoWithKey2 = new TupleTypeInfo<Tuple2<K, I2>>(keys2.getKeyType(), inputType2);
			
			final TupleKeyExtractingMapper<I1, K> extractor1 = new TupleKeyExtractingMapper<I1, K>(logicalKeyPositions1[0]);
			final KeyExtractingMapper<I2, K> extractor2 = new KeyExtractingMapper<I2, K>(keys2.getKeyExtractor());
			
			final PlanMapOperator<I1, Tuple2<K, I1>> keyMapper1 = new PlanMapOperator<I1, Tuple2<K, I1>>(extractor1, "Key Extractor 1", inputType1, typeInfoWithKey1);
			final PlanMapOperator<I2, Tuple2<K, I2>> keyMapper2 = new PlanMapOperator<I2, Tuple2<K, I2>>(extractor2, "Key Extractor 2", inputType2, typeInfoWithKey2);
			
			final PlanUnwrappingJoinOperator<I1, I2, OUT, K> join = new PlanUnwrappingJoinOperator<I1, I2, OUT, K>(function, logicalKeyPositions1, keys2, name, outputType, typeInfoWithKey1, typeInfoWithKey2);
			
			join.addFirstInput(keyMapper1);
			join.addSecondInput(keyMapper2);
			
			return new BinaryNodeTranslation(keyMapper1, keyMapper2, join);
		}
		
		private static <I1, I2, K, OUT> BinaryNodeTranslation translateSelectorFunctionJoinLeft(
				Keys.SelectorFunctionKeys<I1, ?> rawKeys1, int[] logicalKeyPositions2,
				JoinFunction<I1, I2, OUT> function, 
				TypeInformation<I1> inputType1, TypeInformation<I2> inputType2, TypeInformation<OUT> outputType, String name)
		{
			if(!inputType2.isTupleType())
				throw new InvalidParameterException("Should not happen.");
			
			@SuppressWarnings("unchecked")
			final Keys.SelectorFunctionKeys<I1, K> keys1 = (Keys.SelectorFunctionKeys<I1, K>) rawKeys1;
			
			final TypeInformation<Tuple2<K, I1>> typeInfoWithKey1 = new TupleTypeInfo<Tuple2<K, I1>>(keys1.getKeyType(), inputType1); // assume same key, checked by Key.areCompatibale() before
			final TypeInformation<Tuple2<K, I2>> typeInfoWithKey2 = new TupleTypeInfo<Tuple2<K, I2>>(keys1.getKeyType(), inputType2);
			
			final KeyExtractingMapper<I1, K> extractor1 = new KeyExtractingMapper<I1, K>(keys1.getKeyExtractor());
			final TupleKeyExtractingMapper<I2, K> extractor2 = new TupleKeyExtractingMapper<I2, K>(logicalKeyPositions2[0]);
			
			final PlanMapOperator<I1, Tuple2<K, I1>> keyMapper1 = new PlanMapOperator<I1, Tuple2<K, I1>>(extractor1, "Key Extractor 1", inputType1, typeInfoWithKey1);
			final PlanMapOperator<I2, Tuple2<K, I2>> keyMapper2 = new PlanMapOperator<I2, Tuple2<K, I2>>(extractor2, "Key Extractor 2", inputType2, typeInfoWithKey2);
			
			final PlanUnwrappingJoinOperator<I1, I2, OUT, K> join = new PlanUnwrappingJoinOperator<I1, I2, OUT, K>(function, keys1, logicalKeyPositions2, name, outputType, typeInfoWithKey1, typeInfoWithKey2);
			
			join.addFirstInput(keyMapper1);
			join.addSecondInput(keyMapper2);
			
			return new BinaryNodeTranslation(keyMapper1, keyMapper2, join);
		}
	}
	
	public static final class DefaultJoin<I1, I2> extends EquiJoin<I1, I2, Tuple2<I1, I2>> {

		protected DefaultJoin(DataSet<I1> input1, DataSet<I2> input2, 
				Keys<I1> keys1, Keys<I2> keys2, JoinHint hint)
		{
			super(input1, input2, keys1, keys2,
				(JoinFunction<I1, I2, Tuple2<I1, I2>>) new DefaultJoinFunction<I1, I2>(),
				new TupleTypeInfo<Tuple2<I1, I2>>(input1.getType(), input2.getType()), hint);
		}
		
		
		public <R> EquiJoin<I1, I2, R> with(JoinFunction<I1, I2, R> function) {
			TypeInformation<R> returnType = TypeExtractor.getJoinReturnTypes(function, getInput1Type(), getInput2Type());
			return new EquiJoin<I1, I2, R>(getInput1(), getInput2(), getKeys1(), getKeys2(), function, returnType, getJoinHint());
		}
		
		public JoinOperator<I1, I2, I1> leftSemiJoin() {
			return new LeftSemiJoin<I1, I2>(getInput1(), getInput2(), getKeys1(), getKeys2(), getJoinHint());
		}
		
		public JoinOperator<I1, I2, I2> rightSemiJoin() {
			return new RightSemiJoin<I1, I2>(getInput1(), getInput2(), getKeys1(), getKeys2(), getJoinHint());
		}
		
		public JoinOperator<I1, I2, I1> leftAntiJoin() {
			return new LeftAntiJoin<I1, I2>(getInput1(), getInput2(), getKeys1(), getKeys2(), getJoinHint());
		}
		
		public JoinOperator<I1, I2, I2> rightAntiJoin() {
			return new RightAntiJoin<I1, I2>(getInput1(), getInput2(), getKeys1(), getKeys2(), getJoinHint());
		}
	}
	

	

	private static final class LeftAntiJoin<I1, I2> extends JoinOperator<I1, I2, I1> {
		
		protected LeftAntiJoin(DataSet<I1> input1, DataSet<I2> input2, Keys<I1> keys1, Keys<I2> keys2, JoinHint hint) {
			super(input1, input2, keys1, keys2, input1.getType(), hint);
		}
		
		@Override
		protected BinaryNodeTranslation translateToDataFlow() {
			throw new UnsupportedOperationException("LeftAntiJoin operator currently not supported.");
		}
	}
	
	private static final class RightAntiJoin<I1, I2> extends JoinOperator<I1, I2, I2> {
		
		protected RightAntiJoin(DataSet<I1> input1, DataSet<I2> input2, Keys<I1> keys1, Keys<I2> keys2, JoinHint hint) {
			super(input1, input2, keys1, keys2, input2.getType(), hint);
		}
		
		@Override
		protected BinaryNodeTranslation translateToDataFlow() {
			throw new UnsupportedOperationException("RightAntiJoin operator currently not supported.");
		}
	}
	
	private static final class LeftSemiJoin<I1, I2> extends EquiJoin<I1, I2, I1> {
		
		protected LeftSemiJoin(DataSet<I1> input1, DataSet<I2> input2, Keys<I1> keys1, Keys<I2> keys2, JoinHint hint) {
			super(input1, input2, keys1, keys2, new LeftSemiJoinFunction<I1, I2>(), input1.getType(), hint);
		}
		
		@Override
		protected BinaryNodeTranslation translateToDataFlow() {
			// TODO: Runtime support required. Each left tuple may be returned only once.
			// 	     Special exec strategy (runtime + optimizer) based on hash join required. 
			// 		 Either no duplicates of right side in HT or left tuples removed from HT after first match.
			throw new UnsupportedOperationException("LeftSemiJoin operator currently not supported.");
		}
	}
	
	private static final class RightSemiJoin<I1, I2> extends EquiJoin<I1, I2, I2> {
		
		protected RightSemiJoin(DataSet<I1> input1, DataSet<I2> input2, Keys<I1> keys1, Keys<I2> keys2, JoinHint hint) {
			super(input1, input2, keys1, keys2, new RightSemiJoinFunction<I1, I2>(), input2.getType(), hint);
		}
		
		@Override
		protected BinaryNodeTranslation translateToDataFlow() {
			// TODO: Runtime support required. Each right tuple may be returned only once.
			// 	     Special exec strategy (runtime + optimizer) based on hash join required. 
			// 		 Either no duplicates of left side in HT or right tuples removed from HT after first match.
			throw new UnsupportedOperationException("RightSemiJoin operator currently not supported.");
		}
	}
	
	// --------------------------------------------------------------------------------------------
	// Builder classes for incremental construction
	// --------------------------------------------------------------------------------------------
	
	public static final class JoinOperatorSets<I1, I2> {
		
		private final DataSet<I1> input1;
		private final DataSet<I2> input2;
		
		private final JoinHint joinHint;
		
		public JoinOperatorSets(DataSet<I1> input1, DataSet<I2> input2) {
			this(input1, input2, JoinHint.OPTIMIZER_CHOOSES);
		}
		
		public JoinOperatorSets(DataSet<I1> input1, DataSet<I2> input2, JoinHint hint) {
			if (input1 == null || input2 == null)
				throw new NullPointerException();
			
			this.input1 = input1;
			this.input2 = input2;
			this.joinHint = hint;
		}
		
		
		public JoinOperatorSetsPredicate where(int... fields) {
			return new JoinOperatorSetsPredicate(new Keys.FieldPositionKeys<I1>(fields, input1.getType()));
		}
		
		public <K extends Comparable<K>> JoinOperatorSetsPredicate where(KeySelector<I1, K> keyExtractor) {
			return new JoinOperatorSetsPredicate(new Keys.SelectorFunctionKeys<I1, K>(keyExtractor, input1.getType()));
		}
		
		public JoinOperatorSetsPredicate where(String keyExpression) {
			return new JoinOperatorSetsPredicate(new Keys.ExpressionKeys<I1>(keyExpression, input1.getType()));
		}
	
		// ----------------------------------------------------------------------------------------
		
		public class JoinOperatorSetsPredicate {
			
			private final Keys<I1> keys1;
			
			private JoinOperatorSetsPredicate(Keys<I1> keys1) {
				if (keys1 == null)
					throw new NullPointerException();
				
				if (keys1.isEmpty()) {
					throw new InvalidProgramException("The join keys must not be empty.");
				}
				
				this.keys1 = keys1;
			}
			
			
			public DefaultJoin<I1, I2> equalTo(int... fields) {
				return createJoinOperator(new Keys.FieldPositionKeys<I2>(fields, input2.getType()));
			}

			public DefaultJoin<I1, I2> equalTo(String keyExpression) {
				return createJoinOperator(new Keys.ExpressionKeys<I2>(keyExpression, input2.getType()));
			}
			
			public <K> DefaultJoin<I1, I2> equalTo(KeySelector<I2, K> keyExtractor) {
				return createJoinOperator(new Keys.SelectorFunctionKeys<I2, K>(keyExtractor, input2.getType()));
			}
			
			protected DefaultJoin<I1, I2> createJoinOperator(Keys<I2> keys2) {
				if (keys2 == null)
					throw new NullPointerException();
				
				if (keys2.isEmpty()) {
					throw new InvalidProgramException("The join keys must not be empty.");
				}
				
				if (!keys1.areCompatibale(keys2)) {
					throw new InvalidProgramException("The pair of join keys are not compatible with each other.");
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

		@Override
		public Tuple2<T1, T2> join(T1 first, T2 second) throws Exception {
			return new Tuple2<T1, T2>(first, second);
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
}
