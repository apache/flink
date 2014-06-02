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
import eu.stratosphere.api.common.functions.GenericCoGrouper;
import eu.stratosphere.api.common.functions.GenericMap;
import eu.stratosphere.api.common.operators.BinaryOperatorInformation;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.UnaryOperatorInformation;
import eu.stratosphere.api.common.operators.base.CoGroupOperatorBase;
import eu.stratosphere.api.common.operators.base.MapOperatorBase;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.operators.translation.KeyExtractingMapper;
import eu.stratosphere.api.java.operators.translation.PlanUnwrappingCoGroupOperator;
import eu.stratosphere.api.java.operators.translation.TupleKeyExtractingMapper;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.types.TypeInformation;

/**
 * A {@link DataSet} that is the result of a CoGroup transformation. 
 * 
 * @param <I1> The type of the first input DataSet of the CoGroup transformation.
 * @param <I2> The type of the second input DataSet of the CoGroup transformation.
 * @param <OUT> The type of the result of the CoGroup transformation.
 * 
 * @see DataSet
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

		if (keys1 == null || keys2 == null) {
			throw new NullPointerException();
		}

		this.keys1 = keys1;
		this.keys2 = keys2;
		
		extractSemanticAnnotationsFromUdf(function.getClass());
	}

	protected Keys<I1> getKeys1() {
		return this.keys1;
	}

	protected Keys<I2> getKeys2() {
		return this.keys2;
	}

	@Override
	protected eu.stratosphere.api.common.operators.base.CoGroupOperatorBase<?, ?, OUT, ?> translateToDataFlow(Operator<I1> input1, Operator<I2> input2) {
		
		String name = getName() != null ? getName() : function.getClass().getName();

		if (keys1 instanceof Keys.SelectorFunctionKeys
				&& keys2 instanceof Keys.SelectorFunctionKeys
				&& keys1.areCompatibale(keys2)) {

			@SuppressWarnings("unchecked")
			Keys.SelectorFunctionKeys<I1, ?> selectorKeys1 = (Keys.SelectorFunctionKeys<I1, ?>) keys1;
			@SuppressWarnings("unchecked")
			Keys.SelectorFunctionKeys<I2, ?> selectorKeys2 = (Keys.SelectorFunctionKeys<I2, ?>) keys2;

			PlanUnwrappingCoGroupOperator<I1, I2, OUT, ?> po =
					translateSelectorFunctionCoGroup(selectorKeys1, selectorKeys2, function,
					getInput1Type(), getInput2Type(), getResultType(), name, input1, input2);

			// set dop
			po.setDegreeOfParallelism(this.getParallelism());

			return po;

		}
		else if (keys1 instanceof Keys.FieldPositionKeys
				&& keys2 instanceof Keys.FieldPositionKeys
				&& keys1.areCompatibale(keys2)
			) {

			int[] logicalKeyPositions1 = keys1.computeLogicalKeyPositions();
			int[] logicalKeyPositions2 = keys2.computeLogicalKeyPositions();
			
			CoGroupOperatorBase<I1, I2, OUT, GenericCoGrouper<I1, I2, OUT>> po =
					new CoGroupOperatorBase<I1, I2, OUT, GenericCoGrouper<I1, I2, OUT>>(
							function, new BinaryOperatorInformation<I1, I2, OUT>(getInput1Type(), getInput2Type(), getResultType()),
							logicalKeyPositions1, logicalKeyPositions2, name);
			
			// set inputs
			po.setFirstInput(input1);
			po.setSecondInput(input2);

			// set dop
			po.setDegreeOfParallelism(this.getParallelism());

			return po;

		}
		else if (keys1 instanceof Keys.FieldPositionKeys
				&& keys2 instanceof Keys.SelectorFunctionKeys
				&& keys1.areCompatibale(keys2)
			) {

			int[] logicalKeyPositions1 = keys1.computeLogicalKeyPositions();

			@SuppressWarnings("unchecked")
			Keys.SelectorFunctionKeys<I2, ?> selectorKeys2 = (Keys.SelectorFunctionKeys<I2, ?>) keys2;

			PlanUnwrappingCoGroupOperator<I1, I2, OUT, ?> po =
					translateSelectorFunctionCoGroupRight(logicalKeyPositions1, selectorKeys2, function,
					getInput1Type(), getInput2Type(), getResultType(), name, input1, input2);

			// set dop
			po.setDegreeOfParallelism(this.getParallelism());

			return po;
		}
		else if (keys1 instanceof Keys.SelectorFunctionKeys
				&& keys2 instanceof Keys.FieldPositionKeys
				&& keys1.areCompatibale(keys2)
			) {

			@SuppressWarnings("unchecked")
			Keys.SelectorFunctionKeys<I1, ?> selectorKeys1 = (Keys.SelectorFunctionKeys<I1, ?>) keys1;

			int[] logicalKeyPositions2 = keys2.computeLogicalKeyPositions();

			PlanUnwrappingCoGroupOperator<I1, I2, OUT, ?> po =
					translateSelectorFunctionCoGroupLeft(selectorKeys1, logicalKeyPositions2, function,
					getInput1Type(), getInput2Type(), getResultType(), name, input1, input2);

			// set dop
			po.setDegreeOfParallelism(this.getParallelism());

			return po;
		}
		else {
			throw new UnsupportedOperationException("Unrecognized or incompatible key types.");
		}
	}


	private static <I1, I2, K, OUT> PlanUnwrappingCoGroupOperator<I1, I2, OUT, K> translateSelectorFunctionCoGroup(
			Keys.SelectorFunctionKeys<I1, ?> rawKeys1, Keys.SelectorFunctionKeys<I2, ?> rawKeys2,
			CoGroupFunction<I1, I2, OUT> function,
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
		final PlanUnwrappingCoGroupOperator<I1, I2, OUT, K> cogroup = new PlanUnwrappingCoGroupOperator<I1, I2, OUT, K>(function, keys1, keys2, name, outputType, typeInfoWithKey1, typeInfoWithKey2);

		cogroup.setFirstInput(keyMapper1);
		cogroup.setSecondInput(keyMapper2);

		keyMapper1.setInput(input1);
		keyMapper2.setInput(input2);
		// set dop
		keyMapper1.setDegreeOfParallelism(input1.getDegreeOfParallelism());
		keyMapper2.setDegreeOfParallelism(input2.getDegreeOfParallelism());

		return cogroup;
	}

	private static <I1, I2, K, OUT> PlanUnwrappingCoGroupOperator<I1, I2, OUT, K> translateSelectorFunctionCoGroupRight(
			int[] logicalKeyPositions1, Keys.SelectorFunctionKeys<I2, ?> rawKeys2,
			CoGroupFunction<I1, I2, OUT> function,
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
		
		final PlanUnwrappingCoGroupOperator<I1, I2, OUT, K> cogroup = new PlanUnwrappingCoGroupOperator<I1, I2, OUT, K>(function, logicalKeyPositions1, keys2, name, outputType, typeInfoWithKey1, typeInfoWithKey2);

		cogroup.setFirstInput(keyMapper1);
		cogroup.setSecondInput(keyMapper2);

		keyMapper1.setInput(input1);
		keyMapper2.setInput(input2);
		// set dop
		keyMapper1.setDegreeOfParallelism(input1.getDegreeOfParallelism());
		keyMapper2.setDegreeOfParallelism(input2.getDegreeOfParallelism());

		return cogroup;
	}

	private static <I1, I2, K, OUT> PlanUnwrappingCoGroupOperator<I1, I2, OUT, K> translateSelectorFunctionCoGroupLeft(
			Keys.SelectorFunctionKeys<I1, ?> rawKeys1, int[] logicalKeyPositions2,
			CoGroupFunction<I1, I2, OUT> function,
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
		
		final PlanUnwrappingCoGroupOperator<I1, I2, OUT, K> cogroup = new PlanUnwrappingCoGroupOperator<I1, I2, OUT, K>(function, keys1, logicalKeyPositions2, name, outputType, typeInfoWithKey1, typeInfoWithKey2);

		cogroup.setFirstInput(keyMapper1);
		cogroup.setSecondInput(keyMapper2);

		keyMapper1.setInput(input1);
		keyMapper2.setInput(input2);
		// set dop
		keyMapper1.setDegreeOfParallelism(input1.getDegreeOfParallelism());
		keyMapper2.setDegreeOfParallelism(input2.getDegreeOfParallelism());

		return cogroup;
	}

	// --------------------------------------------------------------------------------------------
	// Builder classes for incremental construction
	// --------------------------------------------------------------------------------------------

	/**
	 * Intermediate step of a CoGroup transformation. <br/>
	 * To continue the CoGroup transformation, select the grouping key of the first input {@link DataSet} by calling 
	 * {@link CoGroupOperatorSets#where(int...)} or {@link CoGroupOperatorSets#where(KeySelector)}.
	 *
	 * @param <I1> The type of the first input DataSet of the CoGroup transformation.
	 * @param <I2> The type of the second input DataSet of the CoGroup transformation.
	 */
	public static final class CoGroupOperatorSets<I1, I2> {

		private final DataSet<I1> input1;
		private final DataSet<I2> input2;

		public CoGroupOperatorSets(DataSet<I1> input1, DataSet<I2> input2) {
			if (input1 == null || input2 == null) {
				throw new NullPointerException();
			}

			this.input1 = input1;
			this.input2 = input2;
		}

		/**
		 * Continues a CoGroup transformation. <br/>
		 * Defines the {@link Tuple} fields of the first co-grouped {@link DataSet} that should be used as grouping keys.<br/>
		 * <b>Note: Fields can only be selected as grouping keys on Tuple DataSets.</b><br/>
		 * 
		 * @param fields The indexes of the Tuple fields of the first co-grouped DataSets that should be used as keys.
		 * @return An incomplete CoGroup transformation. 
		 *           Call {@link CoGroupOperatorSetsPredicate#equalTo()} to continue the CoGroup. 
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public CoGroupOperatorSetsPredicate where(int... fields) {
			return new CoGroupOperatorSetsPredicate(new Keys.FieldPositionKeys<I1>(fields, input1.getType()));
		}

		/**
		 * Continues a CoGroup transformation and defines a {@link KeySelector} function for the first co-grouped {@link DataSet}.</br>
		 * The KeySelector function is called for each element of the first DataSet and extracts a single 
		 * key value on which the DataSet is grouped. </br>
		 * 
		 * @param keySelector The KeySelector function which extracts the key values from the DataSet on which it is grouped.
		 * @return An incomplete CoGroup transformation. 
		 *           Call {@link CoGroupOperatorSetsPredicate#equalTo()} to continue the CoGroup. 
		 * 
		 * @see KeySelector
		 * @see DataSet
		 */
		public <K> CoGroupOperatorSetsPredicate where(KeySelector<I1, K> keyExtractor) {
			return new CoGroupOperatorSetsPredicate(new Keys.SelectorFunctionKeys<I1, K>(keyExtractor, input1.getType()));
		}

		// ----------------------------------------------------------------------------------------

		/**
		 * Intermediate step of a CoGroup transformation. <br/>
		 * To continue the CoGroup transformation, select the grouping key of the second input {@link DataSet} by calling 
		 * {@link CoGroupOperatorSetsPredicate#equalTo(int...)} or {@link CoGroupOperatorSetsPredicate#equalTo(KeySelector)}.
		 *
		 */
		public final class CoGroupOperatorSetsPredicate {

			private final Keys<I1> keys1;

			private CoGroupOperatorSetsPredicate(Keys<I1> keys1) {
				if (keys1 == null) {
					throw new NullPointerException();
				}

				if (keys1.isEmpty()) {
					throw new InvalidProgramException("The join keys must not be empty.");
				}

				this.keys1 = keys1;
			}

			/**
			 * Continues a CoGroup transformation and defines the {@link Tuple} fields of the second co-grouped 
			 * {@link DataSet} that should be used as grouping keys.<br/>
			 * <b>Note: Fields can only be selected as grouping keys on Tuple DataSets.</b><br/>
			 * 
			 * @param fields The indexes of the Tuple fields of the second co-grouped DataSet that should be used as keys.
			 * @return An incomplete CoGroup transformation. 
			 *           Call {@link CoGroupOperatorWithoutFunction#with(CoGroupFunction))} to finalize the CoGroup transformation. 
			 */
			public CoGroupOperatorWithoutFunction equalTo(int... fields) {
				return createCoGroupOperator(new Keys.FieldPositionKeys<I2>(fields, input2.getType()));

			}

			/**
			 * Continues a CoGroup transformation and defines a {@link KeySelector} function for the second co-grouped {@link DataSet}.</br>
			 * The KeySelector function is called for each element of the second DataSet and extracts a single 
			 * key value on which the DataSet is grouped. </br>
			 * 
			 * @param keySelector The KeySelector function which extracts the key values from the second DataSet on which it is grouped.
			 * @return An incomplete CoGroup transformation. 
			 *           Call {@link CoGroupOperatorWithoutFunction#with(CoGroupFunction))} to finalize the CoGroup transformation. 
			 */
			public <K> CoGroupOperatorWithoutFunction equalTo(KeySelector<I2, K> keyExtractor) {
				return createCoGroupOperator(new Keys.SelectorFunctionKeys<I2, K>(keyExtractor, input2.getType()));
			}

			/**
			 * Intermediate step of a CoGroup transformation. <br/>
			 * To continue the CoGroup transformation, provide a {@link CoGroupFunction} by calling 
			 * {@link CoGroupOperatorWithoutFunction#with(CoGroupFunction))}.
			 *
			 */
			private CoGroupOperatorWithoutFunction createCoGroupOperator(Keys<I2> keys2) {
				if (keys2 == null) {
					throw new NullPointerException();
				}

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
					if (keys2 == null) {
						throw new NullPointerException();
					}

					if (keys2.isEmpty()) {
						throw new InvalidProgramException("The join keys must not be empty.");
					}

					this.keys2 = keys2;
				}

				/**
				 * Finalizes a CoGroup transformation by applying a {@link CoGroupFunction} to groups of elements with identical keys.<br/>
				 * Each CoGroupFunction call returns an arbitrary number of keys. 
				 * 
				 * @param function The CoGroupFunction that is called for all groups of elements with identical keys.
				 * @return An CoGroupOperator that represents the co-grouped result DataSet.
				 * 
				 * @see CoGroupFunction
				 * @see DataSet
				 */
				public <R> CoGroupOperator<I1, I2, R> with(CoGroupFunction<I1, I2, R> function) {
					if (function == null) {
						throw new NullPointerException("CoGroup function must not be null.");
					}
					TypeInformation<R> returnType = TypeExtractor.getCoGroupReturnTypes(function, input1.getType(), input2.getType());
					return new CoGroupOperator<I1, I2, R>(input1, input2, keys1, keys2, function, returnType);
				}
			}
		}
	}
}
