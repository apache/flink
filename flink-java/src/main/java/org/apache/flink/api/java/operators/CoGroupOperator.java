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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.CoGroupOperatorBase;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.api.java.operators.DeltaIteration.SolutionSetPlaceHolder;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.Keys.ExpressionKeys;
import org.apache.flink.api.java.operators.Keys.IncompatibleKeysException;
import org.apache.flink.api.java.operators.translation.KeyExtractingMapper;
import org.apache.flink.api.java.operators.translation.PlanBothUnwrappingCoGroupOperator;
import org.apache.flink.api.java.operators.translation.PlanLeftUnwrappingCoGroupOperator;
import org.apache.flink.api.java.operators.translation.PlanRightUnwrappingCoGroupOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

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
	
	private final String defaultName;
	
	private final List<Pair<Integer, Order>> groupSortKeyOrderFirst;
	private final List<Pair<Integer, Order>> groupSortKeyOrderSecond;
	
	private Partitioner<?> customPartitioner;


	public CoGroupOperator(DataSet<I1> input1, DataSet<I2> input2, Keys<I1> keys1, Keys<I2> keys2,
							CoGroupFunction<I1, I2, OUT> function, TypeInformation<OUT> returnType,
							Partitioner<?> customPartitioner, String defaultName)
	{
		this(input1, input2, keys1, keys2, function, returnType, null, null, customPartitioner, defaultName);
	}
	
	public CoGroupOperator(DataSet<I1> input1, DataSet<I2> input2, Keys<I1> keys1, Keys<I2> keys2,
			CoGroupFunction<I1, I2, OUT> function, TypeInformation<OUT> returnType,
			List<Pair<Integer, Order>> groupSortKeyOrderFirst, List<Pair<Integer, Order>> groupSortKeyOrderSecond,
			Partitioner<?> customPartitioner, String defaultName)
	{
		super(input1, input2, returnType);

		this.function = function;
		this.customPartitioner = customPartitioner;
		this.defaultName = defaultName;
		
		this.groupSortKeyOrderFirst = groupSortKeyOrderFirst == null ? Collections.<Pair<Integer, Order>>emptyList() : groupSortKeyOrderFirst;
		this.groupSortKeyOrderSecond = groupSortKeyOrderSecond == null ? Collections.<Pair<Integer, Order>>emptyList() : groupSortKeyOrderSecond;

		if (keys1 == null || keys2 == null) {
			throw new NullPointerException();
		}

		// sanity check solution set key mismatches
		if (input1 instanceof SolutionSetPlaceHolder) {
			if (keys1 instanceof ExpressionKeys) {
				int[] positions = ((ExpressionKeys<?>) keys1).computeLogicalKeyPositions();
				((SolutionSetPlaceHolder<?>) input1).checkJoinKeyFields(positions);
			} else {
				throw new InvalidProgramException("Currently, the solution set may only be CoGrouped with using tuple field positions.");
			}
		}
		if (input2 instanceof SolutionSetPlaceHolder) {
			if (keys2 instanceof ExpressionKeys) {
				int[] positions = ((ExpressionKeys<?>) keys2).computeLogicalKeyPositions();
				((SolutionSetPlaceHolder<?>) input2).checkJoinKeyFields(positions);
			} else {
				throw new InvalidProgramException("Currently, the solution set may only be CoGrouped with using tuple field positions.");
			}
		}

		this.keys1 = keys1;
		this.keys2 = keys2;

		UdfOperatorUtils.analyzeDualInputUdf(this, CoGroupFunction.class, defaultName, function, keys1, keys2);
	}
	
	@Override
	protected CoGroupFunction<I1, I2, OUT> getFunction() {
		return function;
	}

	@Override
	public DualInputSemanticProperties getSemanticProperties() {

		DualInputSemanticProperties props = super.getSemanticProperties();

		// offset semantic information by extracted key fields
		if(props != null &&
					(this.keys1 instanceof Keys.SelectorFunctionKeys ||
					this.keys2 instanceof Keys.SelectorFunctionKeys)) {

			int numFields1 = this.getInput1Type().getTotalFields();
			int numFields2 = this.getInput2Type().getTotalFields();
			int offset1 = (this.keys1 instanceof Keys.SelectorFunctionKeys) ?
					((Keys.SelectorFunctionKeys<?,?>) this.keys1).getKeyType().getTotalFields() : 0;
			int offset2 = (this.keys2 instanceof Keys.SelectorFunctionKeys) ?
					((Keys.SelectorFunctionKeys<?,?>) this.keys2).getKeyType().getTotalFields() : 0;

			props = SemanticPropUtil.addSourceFieldOffsets(props, numFields1, numFields2, offset1, offset2);
		}

		return props;
	}

	protected Keys<I1> getKeys1() {
		return this.keys1;
	}

	protected Keys<I2> getKeys2() {
		return this.keys2;
	}
	
	/**
	 * Sets a custom partitioner for the CoGroup operation. The partitioner will be called on the join keys to determine
	 * the partition a key should be assigned to. The partitioner is evaluated on both inputs in the
	 * same way.
	 * <p>
	 * NOTE: A custom partitioner can only be used with single-field CoGroup keys, not with composite CoGroup keys.
	 * 
	 * @param partitioner The custom partitioner to be used.
	 * @return This CoGroup operator, to allow for function chaining.
	 */
	public CoGroupOperator<I1, I2, OUT> withPartitioner(Partitioner<?> partitioner) {
		if (partitioner != null) {
			keys1.validateCustomPartitioner(partitioner, null);
			keys2.validateCustomPartitioner(partitioner, null);
		}
		this.customPartitioner = getInput1().clean(partitioner);
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

	@Override
	protected org.apache.flink.api.common.operators.base.CoGroupOperatorBase<?, ?, OUT, ?> translateToDataFlow(Operator<I1> input1, Operator<I2> input2) {
		
		String name = getName() != null ? getName() : "CoGroup at " + defaultName;
		try {
			keys1.areCompatible(keys2);
		}
		catch (IncompatibleKeysException e) {
			throw new InvalidProgramException("The types of the key fields do not match.", e);
		}
		
		final org.apache.flink.api.common.operators.base.CoGroupOperatorBase<?, ?, OUT, ?> po;

		if (keys1 instanceof Keys.SelectorFunctionKeys
				&& keys2 instanceof Keys.SelectorFunctionKeys) {

			@SuppressWarnings("unchecked")
			Keys.SelectorFunctionKeys<I1, ?> selectorKeys1 =
					(Keys.SelectorFunctionKeys<I1, ?>) keys1;
			@SuppressWarnings("unchecked")
			Keys.SelectorFunctionKeys<I2, ?> selectorKeys2 =
					(Keys.SelectorFunctionKeys<I2, ?>) keys2;

			po = translateSelectorFunctionCoGroup(selectorKeys1, selectorKeys2, function,
					getInput1Type(), getInput2Type(), getResultType(), name, input1, input2);

			po.setParallelism(getParallelism());
			po.setCustomPartitioner(customPartitioner);
		}
		else if (keys2 instanceof Keys.SelectorFunctionKeys) {

			int[] logicalKeyPositions1 = keys1.computeLogicalKeyPositions();

			@SuppressWarnings("unchecked")
			Keys.SelectorFunctionKeys<I2, ?> selectorKeys2 = (Keys.SelectorFunctionKeys<I2, ?>) keys2;

			po = translateSelectorFunctionCoGroupRight(logicalKeyPositions1, selectorKeys2, function,
							getInput1Type(), getInput2Type(), getResultType(), name, input1, input2);

			po.setParallelism(getParallelism());
			po.setCustomPartitioner(customPartitioner);
		}
		else if (keys1 instanceof Keys.SelectorFunctionKeys) {

			@SuppressWarnings("unchecked")
			Keys.SelectorFunctionKeys<I1, ?> selectorKeys1 = (Keys.SelectorFunctionKeys<I1, ?>) keys1;

			int[] logicalKeyPositions2 = keys2.computeLogicalKeyPositions();

			po = translateSelectorFunctionCoGroupLeft(selectorKeys1, logicalKeyPositions2, function,
							getInput1Type(), getInput2Type(), getResultType(), name, input1, input2);
		}
		else if ( keys1 instanceof Keys.ExpressionKeys && keys2 instanceof Keys.ExpressionKeys)
			{
			try {
				keys1.areCompatible(keys2);
			} catch (IncompatibleKeysException e) {
				throw new InvalidProgramException("The types of the key fields do not match.", e);
			}

			int[] logicalKeyPositions1 = keys1.computeLogicalKeyPositions();
			int[] logicalKeyPositions2 = keys2.computeLogicalKeyPositions();
			
			CoGroupOperatorBase<I1, I2, OUT, CoGroupFunction<I1, I2, OUT>> op =
					new CoGroupOperatorBase<I1, I2, OUT, CoGroupFunction<I1, I2, OUT>>(
							function, new BinaryOperatorInformation<I1, I2, OUT>(getInput1Type(), getInput2Type(), getResultType()),
							logicalKeyPositions1, logicalKeyPositions2, name);
			
			op.setFirstInput(input1);
			op.setSecondInput(input2);
			po = op;
		}
		else {
			throw new UnsupportedOperationException("Unrecognized or incompatible key types.");
		}
		
		// configure shared characteristics
		po.setParallelism(getParallelism());
		po.setCustomPartitioner(customPartitioner);
		
		if (groupSortKeyOrderFirst.size() > 0) {
			Ordering o = new Ordering();
			for (Pair<Integer, Order> entry : groupSortKeyOrderFirst) {
				o.appendOrdering(entry.getLeft(), null, entry.getRight());
			}
			po.setGroupOrderForInputOne(o);
		}
		if (groupSortKeyOrderSecond.size() > 0) {
			Ordering o = new Ordering();
			for (Pair<Integer, Order> entry : groupSortKeyOrderSecond) {
				o.appendOrdering(entry.getLeft(), null, entry.getRight());
			}
			po.setGroupOrderForInputTwo(o);
		}
		
		return po;
	}


	private static <I1, I2, K, OUT> PlanBothUnwrappingCoGroupOperator<I1, I2, OUT, K> translateSelectorFunctionCoGroup(
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
		
		final MapOperatorBase<I1, Tuple2<K, I1>, MapFunction<I1, Tuple2<K, I1>>> keyMapper1 =
				new MapOperatorBase<I1, Tuple2<K, I1>, MapFunction<I1, Tuple2<K, I1>>>(extractor1, new UnaryOperatorInformation<I1, Tuple2<K, I1>>(inputType1, typeInfoWithKey1), "Key Extractor 1");
		final MapOperatorBase<I2, Tuple2<K, I2>, MapFunction<I2, Tuple2<K, I2>>> keyMapper2 =
				new MapOperatorBase<I2, Tuple2<K, I2>, MapFunction<I2, Tuple2<K, I2>>>(extractor2, new UnaryOperatorInformation<I2, Tuple2<K, I2>>(inputType2, typeInfoWithKey2), "Key Extractor 2");
		final PlanBothUnwrappingCoGroupOperator<I1, I2, OUT, K> cogroup = new PlanBothUnwrappingCoGroupOperator<I1, I2, OUT, K>(function, keys1, keys2, name, outputType, typeInfoWithKey1, typeInfoWithKey2);

		cogroup.setFirstInput(keyMapper1);
		cogroup.setSecondInput(keyMapper2);

		keyMapper1.setInput(input1);
		keyMapper2.setInput(input2);
		// set parallelism
		keyMapper1.setParallelism(input1.getParallelism());
		keyMapper2.setParallelism(input2.getParallelism());

		return cogroup;
	}

	private static <I1, I2, K, OUT> PlanRightUnwrappingCoGroupOperator<I1, I2, OUT, K> translateSelectorFunctionCoGroupRight(
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

		final TypeInformation<Tuple2<K, I2>> typeInfoWithKey2 =
				new TupleTypeInfo<Tuple2<K, I2>>(keys2.getKeyType(), inputType2);

		final KeyExtractingMapper<I2, K> extractor2 =
				new KeyExtractingMapper<I2, K>(keys2.getKeyExtractor());

		final MapOperatorBase<I2, Tuple2<K, I2>, MapFunction<I2, Tuple2<K, I2>>> keyMapper2 =
				new MapOperatorBase<I2, Tuple2<K, I2>, MapFunction<I2, Tuple2<K, I2>>>(
						extractor2,
						new UnaryOperatorInformation<I2, Tuple2<K, I2>>(inputType2, typeInfoWithKey2),
						"Key Extractor 2");
		
		final PlanRightUnwrappingCoGroupOperator<I1, I2, OUT, K> cogroup =
				new PlanRightUnwrappingCoGroupOperator<I1, I2, OUT, K>(
						function,
						logicalKeyPositions1,
						keys2,
						name,
						outputType,
						inputType1,
						typeInfoWithKey2);

		cogroup.setFirstInput(input1);
		cogroup.setSecondInput(keyMapper2);

		keyMapper2.setInput(input2);
		// set parallelism
		keyMapper2.setParallelism(input2.getParallelism());

		return cogroup;
	}

	private static <I1, I2, K, OUT> PlanLeftUnwrappingCoGroupOperator<I1, I2, OUT, K> translateSelectorFunctionCoGroupLeft(
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

		final TypeInformation<Tuple2<K, I1>> typeInfoWithKey1 =
				new TupleTypeInfo<Tuple2<K, I1>>(keys1.getKeyType(), inputType1);

		final KeyExtractingMapper<I1, K> extractor1 = new KeyExtractingMapper<I1, K>(keys1.getKeyExtractor());

		final MapOperatorBase<I1, Tuple2<K, I1>, MapFunction<I1, Tuple2<K, I1>>> keyMapper1 =
				new MapOperatorBase<I1, Tuple2<K, I1>, MapFunction<I1, Tuple2<K, I1>>>(
						extractor1,
						new UnaryOperatorInformation<I1, Tuple2<K, I1>>(inputType1, typeInfoWithKey1),
						"Key Extractor 1");

		final PlanLeftUnwrappingCoGroupOperator<I1, I2, OUT, K> cogroup =
				new PlanLeftUnwrappingCoGroupOperator<I1, I2, OUT, K>(
						function,
						keys1,
						logicalKeyPositions2,
						name,
						outputType,
						typeInfoWithKey1,
						inputType2);

		cogroup.setFirstInput(keyMapper1);
		cogroup.setSecondInput(input2);

		keyMapper1.setInput(input1);
		// set parallelism
		keyMapper1.setParallelism(input1.getParallelism());

		return cogroup;
	}

	// --------------------------------------------------------------------------------------------
	// Builder classes for incremental construction
	// --------------------------------------------------------------------------------------------

	/**
	 * Intermediate step of a CoGroup transformation. <br>
	 * To continue the CoGroup transformation, select the grouping key of the first input {@link DataSet} by calling 
	 * {@link org.apache.flink.api.java.operators.CoGroupOperator.CoGroupOperatorSets#where(int...)} or {@link org.apache.flink.api.java.operators.CoGroupOperator.CoGroupOperatorSets#where(KeySelector)}.
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
		 * Continues a CoGroup transformation. <br>
		 * Defines the {@link Tuple} fields of the first co-grouped {@link DataSet} that should be used as grouping keys.<br>
		 * <b>Note: Fields can only be selected as grouping keys on Tuple DataSets.</b><br>
		 *
		 *
		 * @param fields The indexes of the Tuple fields of the first co-grouped DataSets that should be used as keys.
		 * @return An incomplete CoGroup transformation.
		 *           Call {@link org.apache.flink.api.java.operators.CoGroupOperator.CoGroupOperatorSets.CoGroupOperatorSetsPredicate#equalTo(int...)} to continue the CoGroup.
		 * 
		 * @see Tuple
		 * @see DataSet
		 */
		public CoGroupOperatorSetsPredicate where(int... fields) {
			return new CoGroupOperatorSetsPredicate(new Keys.ExpressionKeys<I1>(fields, input1.getType()));
		}

		/**
		 * Continues a CoGroup transformation. <br>
		 * Defines the fields of the first co-grouped {@link DataSet} that should be used as grouping keys. Fields
		 * are the names of member fields of the underlying type of the data set.
		 *
		 *
		 * @param fields The  fields of the first co-grouped DataSets that should be used as keys.
		 * @return An incomplete CoGroup transformation.
		 *           Call {@link org.apache.flink.api.java.operators.CoGroupOperator.CoGroupOperatorSets.CoGroupOperatorSetsPredicate#equalTo(int...)} to continue the CoGroup.
		 *
		 * @see Tuple
		 * @see DataSet
		 */
		public CoGroupOperatorSetsPredicate where(String... fields) {
			return new CoGroupOperatorSetsPredicate(new Keys.ExpressionKeys<I1>(fields, input1.getType()));
		}

		/**
		 * Continues a CoGroup transformation and defines a {@link KeySelector} function for the first co-grouped {@link DataSet}.<br>
		 * The KeySelector function is called for each element of the first DataSet and extracts a single 
		 * key value on which the DataSet is grouped. <br>
		 * 
		 * @param keyExtractor The KeySelector function which extracts the key values from the DataSet on which it is grouped.
		 * @return An incomplete CoGroup transformation. 
		 *           Call {@link org.apache.flink.api.java.operators.CoGroupOperator.CoGroupOperatorSets.CoGroupOperatorSetsPredicate#equalTo(int...)} to continue the CoGroup.
		 * 
		 * @see KeySelector
		 * @see DataSet
		 */
		public <K> CoGroupOperatorSetsPredicate where(KeySelector<I1, K> keyExtractor) {
			TypeInformation<K> keyType = TypeExtractor.getKeySelectorTypes(keyExtractor, input1.getType());
			return new CoGroupOperatorSetsPredicate(new Keys.SelectorFunctionKeys<I1, K>(keyExtractor, input1.getType(), keyType));
		}

		// ----------------------------------------------------------------------------------------

		/**
		 * Intermediate step of a CoGroup transformation. <br>
		 * To continue the CoGroup transformation, select the grouping key of the second input {@link DataSet} by calling 
		 * {@link org.apache.flink.api.java.operators.CoGroupOperator.CoGroupOperatorSets.CoGroupOperatorSetsPredicate#equalTo(int...)} or {@link org.apache.flink.api.java.operators.CoGroupOperator.CoGroupOperatorSets.CoGroupOperatorSetsPredicate#equalTo(KeySelector)}.
		 *
		 */
		public final class CoGroupOperatorSetsPredicate {

			private final Keys<I1> keys1;

			private CoGroupOperatorSetsPredicate(Keys<I1> keys1) {
				if (keys1 == null) {
					throw new NullPointerException();
				}

				if (keys1.isEmpty()) {
					throw new InvalidProgramException("The co-group keys must not be empty.");
				}

				this.keys1 = keys1;
			}

			/**
			 * Continues a CoGroup transformation and defines the {@link Tuple} fields of the second co-grouped 
			 * {@link DataSet} that should be used as grouping keys.<br>
			 * <b>Note: Fields can only be selected as grouping keys on Tuple DataSets.</b><br>
			 *
			 *
			 * @param fields The indexes of the Tuple fields of the second co-grouped DataSet that should be used as keys.
			 * @return An incomplete CoGroup transformation.
			 *           Call {@link org.apache.flink.api.java.operators.CoGroupOperator.CoGroupOperatorSets.CoGroupOperatorSetsPredicate.CoGroupOperatorWithoutFunction#with(org.apache.flink.api.common.functions.CoGroupFunction)} to finalize the CoGroup transformation.
			 */
			public CoGroupOperatorWithoutFunction equalTo(int... fields) {
				return createCoGroupOperator(new Keys.ExpressionKeys<I2>(fields, input2.getType()));
			}

			/**
			 * Continues a CoGroup transformation and defines the fields of the second co-grouped
			 * {@link DataSet} that should be used as grouping keys.<br>
			 *
			 *
			 * @param fields The  fields of the first co-grouped DataSets that should be used as keys.
			 * @return An incomplete CoGroup transformation.
			 *           Call {@link org.apache.flink.api.java.operators.CoGroupOperator.CoGroupOperatorSets.CoGroupOperatorSetsPredicate.CoGroupOperatorWithoutFunction#with(org.apache.flink.api.common.functions.CoGroupFunction)} to finalize the CoGroup transformation.
			 */
			public CoGroupOperatorWithoutFunction equalTo(String... fields) {
				return createCoGroupOperator(new Keys.ExpressionKeys<I2>(fields, input2.getType()));
			}

			/**
			 * Continues a CoGroup transformation and defines a {@link KeySelector} function for the second co-grouped {@link DataSet}.<br>
			 * The KeySelector function is called for each element of the second DataSet and extracts a single 
			 * key value on which the DataSet is grouped. <br>
			 * 
			 * @param keyExtractor The KeySelector function which extracts the key values from the second DataSet on which it is grouped.
			 * @return An incomplete CoGroup transformation. 
			 *           Call {@link org.apache.flink.api.java.operators.CoGroupOperator.CoGroupOperatorSets.CoGroupOperatorSetsPredicate.CoGroupOperatorWithoutFunction#with(org.apache.flink.api.common.functions.CoGroupFunction)} to finalize the CoGroup transformation.
			 */
			public <K> CoGroupOperatorWithoutFunction equalTo(KeySelector<I2, K> keyExtractor) {
				TypeInformation<K> keyType = TypeExtractor.getKeySelectorTypes(keyExtractor, input2.getType());
				return createCoGroupOperator(new Keys.SelectorFunctionKeys<I2, K>(keyExtractor, input2.getType(), keyType));
			}

			/**
			 * Intermediate step of a CoGroup transformation. <br>
			 * To continue the CoGroup transformation, provide a {@link org.apache.flink.api.common.functions.RichCoGroupFunction} by calling
			 * {@link org.apache.flink.api.java.operators.CoGroupOperator.CoGroupOperatorSets.CoGroupOperatorSetsPredicate.CoGroupOperatorWithoutFunction#with(org.apache.flink.api.common.functions.CoGroupFunction)}.
			 *
			 */
			private CoGroupOperatorWithoutFunction createCoGroupOperator(Keys<I2> keys2) {
				if (keys2 == null) {
					throw new NullPointerException();
				}

				if (keys2.isEmpty()) {
					throw new InvalidProgramException("The co-group keys must not be empty.");
				}
				try {
					keys1.areCompatible(keys2);
				} catch(IncompatibleKeysException ike) {
					throw new InvalidProgramException("The pair of co-group keys are not compatible with each other.", ike);
				}

				return new CoGroupOperatorWithoutFunction(keys2);
			}
			
			// ------------------------------------------------------------------------------------

			public final class CoGroupOperatorWithoutFunction {
				
				private final Keys<I2> keys2;
				
				private final List<Pair<Integer, Order>> groupSortKeyOrderFirst;
				private final List<Pair<Integer, Order>> groupSortKeyOrderSecond;
				
				private Partitioner<?> customPartitioner;

				private CoGroupOperatorWithoutFunction(Keys<I2> keys2) {
					if (keys2 == null) {
						throw new NullPointerException();
					}
					if (keys2.isEmpty()) {
						throw new InvalidProgramException("The co-group keys must not be empty.");
					}

					this.keys2 = keys2;
					
					this.groupSortKeyOrderFirst = new ArrayList<Pair<Integer, Order>>();
					this.groupSortKeyOrderSecond = new ArrayList<Pair<Integer, Order>>();
				}
				
				/**
				 * Sets a custom partitioner for the CoGroup operation. The partitioner will be called on the join keys to determine
				 * the partition a key should be assigned to. The partitioner is evaluated on both inputs in the
				 * same way.
				 * <p>
				 * NOTE: A custom partitioner can only be used with single-field CoGroup keys, not with composite CoGroup keys.
				 * 
				 * @param partitioner The custom partitioner to be used.
				 * @return This CoGroup operator, to allow for function chaining.
				 */
				public CoGroupOperatorWithoutFunction withPartitioner(Partitioner<?> partitioner) {
					if (partitioner != null) {
						keys1.validateCustomPartitioner(partitioner, null);
						keys2.validateCustomPartitioner(partitioner, null);
					}
					this.customPartitioner = input1.clean(partitioner);
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

				/**
				 * Finalizes a CoGroup transformation by applying a {@link org.apache.flink.api.common.functions.RichCoGroupFunction} to groups of elements with identical keys.<br>
				 * Each CoGroupFunction call returns an arbitrary number of keys. 
				 * 
				 * @param function The CoGroupFunction that is called for all groups of elements with identical keys.
				 * @return An CoGroupOperator that represents the co-grouped result DataSet.
				 * 
				 * @see org.apache.flink.api.common.functions.RichCoGroupFunction
				 * @see DataSet
				 */
				public <R> CoGroupOperator<I1, I2, R> with(CoGroupFunction<I1, I2, R> function) {
					if (function == null) {
						throw new NullPointerException("CoGroup function must not be null.");
					}
					TypeInformation<R> returnType = TypeExtractor.getCoGroupReturnTypes(function, input1.getType(), input2.getType(),
							Utils.getCallLocationName(), true);
					
					return new CoGroupOperator<I1, I2, R>(input1, input2, keys1, keys2, input1.clean(function), returnType,
							groupSortKeyOrderFirst, groupSortKeyOrderSecond,
							customPartitioner, Utils.getCallLocationName());
				}
				
				// --------------------------------------------------------------------------------
				//  Group Operations
				// --------------------------------------------------------------------------------
				
				/**
				 * Sorts {@link org.apache.flink.api.java.tuple.Tuple} elements within a group in the first input on the
				 * specified field in the specified {@link Order}.<br>
				 * <b>Note: Only groups of Tuple elements and Pojos can be sorted.</b><br>
				 * Groups can be sorted by multiple fields by chaining {@link #sortFirstGroup(int, Order)} calls.
				 * 
				 * @param field The Tuple field on which the group is sorted.
				 * @param order The Order in which the specified Tuple field is sorted.
				 * @return A SortedGrouping with specified order of group element.
				 * 
				 * @see org.apache.flink.api.java.tuple.Tuple
				 * @see Order
				 */
				public CoGroupOperatorWithoutFunction sortFirstGroup(int field, Order order) {
					if (!input1.getType().isTupleType()) {
						throw new InvalidProgramException("Specifying order keys via field positions is only valid for tuple data types");
					}
					if (field >= input1.getType().getArity()) {
						throw new IllegalArgumentException("Order key out of tuple bounds.");
					}
					ExpressionKeys<I1> ek = new ExpressionKeys<I1>(new int[]{field}, input1.getType());
					int[] groupOrderKeys = ek.computeLogicalKeyPositions();
					
					for (int key : groupOrderKeys) {
						this.groupSortKeyOrderFirst.add(new ImmutablePair<Integer, Order>(key, order));
					}
					
					return this;
				}
				
				/**
				 * Sorts {@link org.apache.flink.api.java.tuple.Tuple} elements within a group in the second input on the
				 * specified field in the specified {@link Order}.<br>
				 * <b>Note: Only groups of Tuple elements and Pojos can be sorted.</b><br>
				 * Groups can be sorted by multiple fields by chaining {@link #sortSecondGroup(int, Order)} calls.
				 * 
				 * @param field The Tuple field on which the group is sorted.
				 * @param order The Order in which the specified Tuple field is sorted.
				 * @return A SortedGrouping with specified order of group element.
				 * 
				 * @see org.apache.flink.api.java.tuple.Tuple
				 * @see Order
				 */
				public CoGroupOperatorWithoutFunction sortSecondGroup(int field, Order order) {
					if (!input2.getType().isTupleType()) {
						throw new InvalidProgramException("Specifying order keys via field positions is only valid for tuple data types");
					}
					if (field >= input2.getType().getArity()) {
						throw new IllegalArgumentException("Order key out of tuple bounds.");
					}
					ExpressionKeys<I2> ek = new ExpressionKeys<I2>(new int[]{field}, input2.getType());
					int[] groupOrderKeys = ek.computeLogicalKeyPositions();
					
					for (int key : groupOrderKeys) {
						this.groupSortKeyOrderSecond.add(new ImmutablePair<Integer, Order>(key, order));
					}
					
					return this;
				}
				
				/**
				 * Sorts Pojo or {@link org.apache.flink.api.java.tuple.Tuple} elements within a group in the first input on the
				 * specified field in the specified {@link Order}.<br>
				 * Groups can be sorted by multiple fields by chaining {@link #sortFirstGroup(String, Order)} calls.
				 * 
				 * @param fieldExpression The expression to the field on which the group is to be sorted.
				 * @param order The Order in which the specified Tuple field is sorted.
				 * @return A SortedGrouping with specified order of group element.
				 * 
				 * @see Order
				 */
				public CoGroupOperatorWithoutFunction sortFirstGroup(String fieldExpression, Order order) {
					if (! (input1.getType() instanceof CompositeType)) {
						throw new InvalidProgramException("Specifying order keys via field positions is only valid for composite data types (pojo / tuple / case class)");
					}
					ExpressionKeys<I1> ek = new ExpressionKeys<I1>(new String[]{fieldExpression}, input1.getType());
					int[] groupOrderKeys = ek.computeLogicalKeyPositions();
					
					for (int key : groupOrderKeys) {
						this.groupSortKeyOrderFirst.add(new ImmutablePair<Integer, Order>(key, order));
					}
					
					return this;
				}
				
				/**
				 * Sorts Pojo or {@link org.apache.flink.api.java.tuple.Tuple} elements within a group in the second input on the
				 * specified field in the specified {@link Order}.<br>
				 * Groups can be sorted by multiple fields by chaining {@link #sortSecondGroup(String, Order)} calls.
				 * 
				 * @param fieldExpression The expression to the field on which the group is to be sorted.
				 * @param order The Order in which the specified Tuple field is sorted.
				 * @return A SortedGrouping with specified order of group element.
				 * 
				 * @see Order
				 */
				public CoGroupOperatorWithoutFunction sortSecondGroup(String fieldExpression, Order order) {
					if (! (input2.getType() instanceof CompositeType)) {
						throw new InvalidProgramException("Specifying order keys via field positions is only valid for composite data types (pojo / tuple / case class)");
					}
					ExpressionKeys<I2> ek = new ExpressionKeys<I2>(new String[]{fieldExpression}, input2.getType());
					int[] groupOrderKeys = ek.computeLogicalKeyPositions();
					
					for (int key : groupOrderKeys) {
						this.groupSortKeyOrderSecond.add(new ImmutablePair<Integer, Order>(key, order));
					}
					
					return this;
				}
			}
		}
	}
}
