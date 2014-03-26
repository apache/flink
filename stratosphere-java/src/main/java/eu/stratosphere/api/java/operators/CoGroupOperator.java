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

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.operators.translation.BinaryNodeTranslation;
import eu.stratosphere.api.java.operators.translation.KeyExtractingMapper;
import eu.stratosphere.api.java.operators.translation.PlanCogroupOperator;
import eu.stratosphere.api.java.operators.translation.PlanMapOperator;
import eu.stratosphere.api.java.operators.translation.PlanUnwrappingCoGroupOperator;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.api.java.typeutils.TypeInformation;

/**
 *
 */
public class CoGroupOperator<I1, I2, OUT> extends TwoInputUdfOperator<I1, I2, OUT, CoGroupOperator<I1, I2, OUT>> {
	
	private final CoGroupFunction<I1, I2, OUT> function;

	private final Keys<I1> keys1;
	private final Keys<I2> keys2;


	protected CoGroupOperator(DataSet<I1> input1, DataSet<I2> input2,
							  Keys<I1> keys1, Keys<I2> keys2,
							  CoGroupFunction<I1, I2, OUT> function,
							  TypeInformation<OUT> returnType)
	{
		super(input1, input2, returnType);

		this.function = function;

		if (keys1 == null || keys2 == null)
			throw new NullPointerException();
		
		this.keys1 = keys1;
		this.keys2 = keys2;
	}
	
	protected Keys<I1> getKeys1() {
		return this.keys1;
	}
	
	protected Keys<I2> getKeys2() {
		return this.keys2;
	}
	
	@Override
	protected BinaryNodeTranslation translateToDataFlow() {
		String name = getName() != null ? getName() : function.getClass().getName();
		
		if (keys1 instanceof Keys.SelectorFunctionKeys 
				&& keys2 instanceof Keys.SelectorFunctionKeys
				&& keys1.areCompatibale(keys2)) {
			
			@SuppressWarnings("unchecked")
			Keys.SelectorFunctionKeys<I1, ?> selectorKeys1 = (Keys.SelectorFunctionKeys<I1, ?>) keys1;
			@SuppressWarnings("unchecked")
			Keys.SelectorFunctionKeys<I2, ?> selectorKeys2 = (Keys.SelectorFunctionKeys<I2, ?>) keys2;
			
			return translateSelectorFunctionCoGroup(selectorKeys1, selectorKeys2, function, 
					getInput1Type(), getInput2Type(), getResultType(), name);
			
		}
		else if (keys1 instanceof Keys.FieldPositionKeys 
				&& keys2 instanceof Keys.FieldPositionKeys 
				&& keys1.areCompatibale(keys2)
			) {
		
		int[] logicalKeyPositions1 = keys1.computeLogicalKeyPositions();
		int[] logicalKeyPositions2 = keys2.computeLogicalKeyPositions();
		
		return new BinaryNodeTranslation(
				new PlanCogroupOperator<I1, I2, OUT>(function, logicalKeyPositions1, logicalKeyPositions2, 
						name, getInput1Type(), getInput2Type(), getResultType()));
		}
		else {
			throw new UnsupportedOperationException("Unrecognized or incompatible key types.");
		}
	}
	
	private static <I1, I2, K, OUT> BinaryNodeTranslation translateSelectorFunctionCoGroup(
			Keys.SelectorFunctionKeys<I1, ?> rawKeys1, Keys.SelectorFunctionKeys<I2, ?> rawKeys2, 
			CoGroupFunction<I1, I2, OUT> function, 
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
		final PlanUnwrappingCoGroupOperator<I1, I2, OUT, K> join = new PlanUnwrappingCoGroupOperator<I1, I2, OUT, K>(function, keys1, keys2, name, outputType, typeInfoWithKey1, typeInfoWithKey2);
		
		join.addFirstInput(keyMapper1);
		join.addSecondInput(keyMapper2);
		
		return new BinaryNodeTranslation(keyMapper1, keyMapper2, join);
	}

	// --------------------------------------------------------------------------------------------
	// Builder classes for incremental construction
	// --------------------------------------------------------------------------------------------
	
	public static final class CoGroupOperatorSets<I1, I2> {
		
		private final DataSet<I1> input1;
		private final DataSet<I2> input2;
		
		public CoGroupOperatorSets(DataSet<I1> input1, DataSet<I2> input2) {
			if (input1 == null || input2 == null)
				throw new NullPointerException();
			
			this.input1 = input1;
			this.input2 = input2;
		}
		
		public CoGroupOperatorSetsPredicate where(int... fields) {
			return new CoGroupOperatorSetsPredicate(new Keys.FieldPositionKeys<I1>(fields, input1.getType()));
		}
		
		public <K> CoGroupOperatorSetsPredicate where(KeySelector<I1, K> keyExtractor) {
			return new CoGroupOperatorSetsPredicate(new Keys.SelectorFunctionKeys<I1, K>(keyExtractor, input1.getType()));
		}
		
		public CoGroupOperatorSetsPredicate where(String keyExpression) {
			return new CoGroupOperatorSetsPredicate(new Keys.ExpressionKeys<I1>(keyExpression, input1.getType()));
		}
	
		// ----------------------------------------------------------------------------------------
		
		public final class CoGroupOperatorSetsPredicate {
			
			private final Keys<I1> keys1;
			
			private CoGroupOperatorSetsPredicate(Keys<I1> keys1) {
				if (keys1 == null)
					throw new NullPointerException();
				
				if (keys1.isEmpty()) {
					throw new InvalidProgramException("The join keys must not be empty.");
				}
				
				this.keys1 = keys1;
			}
			
			
			public CoGroupOperatorWithoutFunction equalTo(int... fields) {
				return createCoGroupOperator(new Keys.FieldPositionKeys<I2>(fields, input2.getType()));
				
			}
			
			public <K> CoGroupOperatorWithoutFunction equalTo(KeySelector<I2, K> keyExtractor) {
				return createCoGroupOperator(new Keys.SelectorFunctionKeys<I2, K>(keyExtractor, input2.getType()));
			}
			
			public CoGroupOperatorWithoutFunction equalTo(String keyExpression) {
				return createCoGroupOperator(new Keys.ExpressionKeys<I2>(keyExpression, input2.getType()));
			}
			
			
			private CoGroupOperatorWithoutFunction createCoGroupOperator(Keys<I2> keys2) {
				if (keys2 == null)
					throw new NullPointerException();
				
				if (keys2.isEmpty()) {
					throw new InvalidProgramException("The join keys must not be empty.");
				}
				
				if (!keys1.areCompatibale(keys2)) {
					throw new InvalidProgramException("The pair of join keys are not compatible with each other.");
				}
				
				return new CoGroupOperatorWithoutFunction(keys2);
			}

			public final class CoGroupOperatorWithoutFunction {
				private final Keys<I2> keys2;

				private CoGroupOperatorWithoutFunction(Keys<I2> keys2) {
					if (keys2 == null)
						throw new NullPointerException();

					if (keys2.isEmpty()) {
						throw new InvalidProgramException("The join keys must not be empty.");
					}

					this.keys2 = keys2;
				}

				public <R> CoGroupOperator<I1, I2, R> with(CoGroupFunction<I1, I2, R> function) {
					TypeInformation<R> returnType = TypeExtractor.getCoGroupReturnTypes(function, input1.getType(), input2.getType());
					return new CoGroupOperator<I1, I2, R>(input1, input2, keys1, keys2, function, returnType);
				}
			}
		}
	}
}
