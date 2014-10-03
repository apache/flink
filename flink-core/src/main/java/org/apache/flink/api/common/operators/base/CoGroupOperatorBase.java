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

package org.apache.flink.api.common.operators.base;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.operators.DualInputOperator;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.util.ListKeyGroupedIterator;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeinfo.CompositeType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.GenericPairComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @see org.apache.flink.api.common.functions.CoGroupFunction
 */
public class CoGroupOperatorBase<IN1, IN2, OUT, FT extends CoGroupFunction<IN1, IN2, OUT>> extends DualInputOperator<IN1, IN2, OUT, FT> {

	/**
	 * The ordering for the order inside a group from input one.
	 */
	private Ordering groupOrder1;

	/**
	 * The ordering for the order inside a group from input two.
	 */
	private Ordering groupOrder2;

	// --------------------------------------------------------------------------------------------

	private boolean combinableFirst;

	private boolean combinableSecond;

	public CoGroupOperatorBase(UserCodeWrapper<FT> udf, BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo, int[] keyPositions1, int[] keyPositions2, String name) {
		super(udf, operatorInfo, keyPositions1, keyPositions2, name);
		this.combinableFirst = false;
		this.combinableSecond = false;
	}

	public CoGroupOperatorBase(FT udf, BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo, int[] keyPositions1, int[] keyPositions2, String name) {
		this(new UserCodeObjectWrapper<FT>(udf), operatorInfo, keyPositions1, keyPositions2, name);
	}

	public CoGroupOperatorBase(Class<? extends FT> udf, BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo, int[] keyPositions1, int[] keyPositions2, String name) {
		this(new UserCodeClassWrapper<FT>(udf), operatorInfo, keyPositions1, keyPositions2, name);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Sets the order of the elements within a group for the given input.
	 *
	 * @param inputNum The number of the input (here either <i>0</i> or <i>1</i>).
	 * @param order    The order for the elements in a group.
	 */
	public void setGroupOrder(int inputNum, Ordering order) {
		if (inputNum == 0) {
			this.groupOrder1 = order;
		}
		else if (inputNum == 1) {
			this.groupOrder2 = order;
		}
		else {
			throw new IndexOutOfBoundsException();
		}
	}

	/**
	 * Sets the order of the elements within a group for the first input.
	 *
	 * @param order The order for the elements in a group.
	 */
	public void setGroupOrderForInputOne(Ordering order) {
		setGroupOrder(0, order);
	}

	/**
	 * Sets the order of the elements within a group for the second input.
	 *
	 * @param order The order for the elements in a group.
	 */
	public void setGroupOrderForInputTwo(Ordering order) {
		setGroupOrder(1, order);
	}

	/**
	 * Gets the value order for an input, i.e. the order of elements within a group.
	 * If no such order has been set, this method returns null.
	 *
	 * @param inputNum The number of the input (here either <i>0</i> or <i>1</i>).
	 * @return The group order.
	 */
	public Ordering getGroupOrder(int inputNum) {
		if (inputNum == 0) {
			return this.groupOrder1;
		}
		else if (inputNum == 1) {
			return this.groupOrder2;
		}
		else {
			throw new IndexOutOfBoundsException();
		}
	}

	/**
	 * Gets the order of elements within a group for the first input.
	 * If no such order has been set, this method returns null.
	 *
	 * @return The group order for the first input.
	 */
	public Ordering getGroupOrderForInputOne() {
		return getGroupOrder(0);
	}

	/**
	 * Gets the order of elements within a group for the second input.
	 * If no such order has been set, this method returns null.
	 *
	 * @return The group order for the second input.
	 */
	public Ordering getGroupOrderForInputTwo() {
		return getGroupOrder(1);
	}

	// --------------------------------------------------------------------------------------------

	public boolean isCombinableFirst() {
		return this.combinableFirst;
	}

	public void setCombinableFirst(boolean combinableFirst) {
		this.combinableFirst = combinableFirst;
	}

	public boolean isCombinableSecond() {
		return this.combinableSecond;
	}

	public void setCombinableSecond(boolean combinableSecond) {
		this.combinableSecond = combinableSecond;
	}

	// ------------------------------------------------------------------------

	@Override
	protected List<OUT> executeOnCollections(List<IN1> input1, List<IN2> input2, RuntimeContext ctx) throws Exception {
		// --------------------------------------------------------------------
		// Setup
		// --------------------------------------------------------------------
		TypeInformation<IN1> inputType1 = getOperatorInfo().getFirstInputType();
		TypeInformation<IN2> inputType2 = getOperatorInfo().getSecondInputType();

		int[] inputKeys1 = getKeyColumns(0);
		int[] inputKeys2 = getKeyColumns(1);

		boolean[] inputSortDirections1 = new boolean[inputKeys1.length];
		boolean[] inputSortDirections2 = new boolean[inputKeys2.length];

		Arrays.fill(inputSortDirections1, true);
		Arrays.fill(inputSortDirections2, true);

		final TypeComparator<IN1> inputComparator1 = getTypeComparator(inputType1, inputKeys1, inputSortDirections1);
		final TypeComparator<IN2> inputComparator2 = getTypeComparator(inputType2, inputKeys2, inputSortDirections2);

		CoGroupSortListIterator<IN1, IN2> coGroupIterator =
				new CoGroupSortListIterator<IN1, IN2>(input1, inputComparator1, input2, inputComparator2);

		// --------------------------------------------------------------------
		// Run UDF
		// --------------------------------------------------------------------
		CoGroupFunction<IN1, IN2, OUT> function = userFunction.getUserCodeObject();

		FunctionUtils.setFunctionRuntimeContext(function, ctx);
		FunctionUtils.openFunction(function, parameters);

		List<OUT> result = new ArrayList<OUT>();
		Collector<OUT> resultCollector = new ListCollector<OUT>(result);

		while (coGroupIterator.next()) {
			function.coGroup(coGroupIterator.getValues1(), coGroupIterator.getValues2(), resultCollector);
		}

		FunctionUtils.closeFunction(function);

		return result;
	}

	@SuppressWarnings("unchecked")
	private <T> TypeComparator<T> getTypeComparator(TypeInformation<T> inputType, int[] inputKeys, boolean[] inputSortDirections) {
		if (!(inputType instanceof CompositeType)) {
			throw new InvalidProgramException("Input types of coGroup must be composite types.");
		}

		return ((CompositeType<T>) inputType).createComparator(inputKeys, inputSortDirections);
	}

	private static class CoGroupSortListIterator<IN1, IN2> {

		private static enum MatchStatus {
			NONE_REMAINED, FIRST_REMAINED, SECOND_REMAINED, FIRST_EMPTY, SECOND_EMPTY
		}

		private final ListKeyGroupedIterator<IN1> iterator1;

		private final ListKeyGroupedIterator<IN2> iterator2;

		private final TypePairComparator<IN1, IN2> pairComparator;

		private MatchStatus matchStatus;

		private Iterable<IN1> firstReturn;

		private Iterable<IN2> secondReturn;

		private CoGroupSortListIterator(
				List<IN1> input1, final TypeComparator<IN1> inputComparator1,
				List<IN2> input2, final TypeComparator<IN2> inputComparator2) {

			this.pairComparator = new GenericPairComparator<IN1, IN2>(inputComparator1, inputComparator2);

			this.iterator1 = new ListKeyGroupedIterator<IN1>(input1, inputComparator1);
			this.iterator2 = new ListKeyGroupedIterator<IN2>(input2, inputComparator2);

			// ----------------------------------------------------------------
			// Sort
			// ----------------------------------------------------------------
			Collections.sort(input1, new Comparator<IN1>() {
				@Override
				public int compare(IN1 o1, IN1 o2) {
					return inputComparator1.compare(o1, o2);
				}
			});

			Collections.sort(input2, new Comparator<IN2>() {
				@Override
				public int compare(IN2 o1, IN2 o2) {
					return inputComparator2.compare(o1, o2);
				}
			});
		}

		private boolean next() throws IOException {
			boolean firstEmpty = true;
			boolean secondEmpty = true;

			if (this.matchStatus != MatchStatus.FIRST_EMPTY) {
				if (this.matchStatus == MatchStatus.FIRST_REMAINED) {
					// comparator is still set correctly
					firstEmpty = false;
				}
				else {
					if (this.iterator1.nextKey()) {
						this.pairComparator.setReference(iterator1.getValues().getCurrent());
						firstEmpty = false;
					}
				}
			}

			if (this.matchStatus != MatchStatus.SECOND_EMPTY) {
				if (this.matchStatus == MatchStatus.SECOND_REMAINED) {
					secondEmpty = false;
				}
				else {
					if (iterator2.nextKey()) {
						secondEmpty = false;
					}
				}
			}

			if (firstEmpty && secondEmpty) {
				// both inputs are empty
				return false;
			}
			else if (firstEmpty && !secondEmpty) {
				// input1 is empty, input2 not
				this.firstReturn = Collections.emptySet();
				this.secondReturn = this.iterator2.getValues();
				this.matchStatus = MatchStatus.FIRST_EMPTY;
				return true;
			}
			else if (!firstEmpty && secondEmpty) {
				// input1 is not empty, input 2 is empty
				this.firstReturn = this.iterator1.getValues();
				this.secondReturn = Collections.emptySet();
				this.matchStatus = MatchStatus.SECOND_EMPTY;
				return true;
			}
			else {
				// both inputs are not empty
				final int comp = this.pairComparator.compareToReference(iterator2.getValues().getCurrent());

				if (0 == comp) {
					// keys match
					this.firstReturn = this.iterator1.getValues();
					this.secondReturn = this.iterator2.getValues();
					this.matchStatus = MatchStatus.NONE_REMAINED;
				}
				else if (0 < comp) {
					// key1 goes first
					this.firstReturn = this.iterator1.getValues();
					this.secondReturn = Collections.emptySet();
					this.matchStatus = MatchStatus.SECOND_REMAINED;
				}
				else {
					// key 2 goes first
					this.firstReturn = Collections.emptySet();
					this.secondReturn = this.iterator2.getValues();
					this.matchStatus = MatchStatus.FIRST_REMAINED;
				}
				return true;
			}
		}

		private Iterable<IN1> getValues1() {
			return firstReturn;
		}

		private Iterable<IN2> getValues2() {
			return secondReturn;
		}
	}
}
