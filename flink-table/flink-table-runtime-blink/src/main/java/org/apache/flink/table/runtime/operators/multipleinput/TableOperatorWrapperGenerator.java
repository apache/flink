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

package org.apache.flink.table.runtime.operators.multipleinput;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.transformations.UnionTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSpec;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A generator that generates a {@link TableOperatorWrapper} graph from a graph of {@link Transformation}.
 */
public class TableOperatorWrapperGenerator {

	/**
	 * Original input transformations for {@link MultipleInputStreamOperatorBase}.
	 */
	private final List<Transformation<?>> inputTransforms;

	/**
	 * The tail (root) transformation of the transformation-graph in {@link MultipleInputStreamOperatorBase}.
	 */
	private final Transformation<?> tailTransform;

	/**
	 * The read order corresponding to each transformation in {@link #inputTransforms}.
	 */
	private final int[] readOrders;

	/**
	 * The list of {@link InputInfo}, including input {@link Transformation} together with their {@link InputSpec} and {@link KeySelector}.
	 */
	private final List<InputInfo> inputInfoList;

	@Nullable
	private TypeInformation<?> stateKeyType;

	/**
	 * The head (leaf) operator wrappers of the operator-graph in {@link MultipleInputStreamOperatorBase}.
	 */
	private final List<TableOperatorWrapper<?>> headWrappers;

	/**
	 * The tail (root) operator wrapper of the operator-graph in {@link MultipleInputStreamOperatorBase}.
	 */
	private TableOperatorWrapper<?> tailWrapper;

	/**
	 * Map the visited transformation to its generated TableOperatorWrapper.
	 */
	private final Map<Transformation<?>, TableOperatorWrapper<?>> visitedTransforms;
	/**
	 * The identifier for each sub operator in {@link MultipleInputStreamOperatorBase}.
	 */
	private int identifierOfSubOp = 0;

	private int parallelism;
	private int maxParallelism;
	private ResourceSpec minResources;
	private ResourceSpec preferredResources;
	/**
	 * managed memory weight for batch operator.
	 */
	private long managedMemoryWeight;

	public TableOperatorWrapperGenerator(
			List<Transformation<?>> inputTransforms,
			Transformation<?> tailTransform) {
		this(inputTransforms, tailTransform, new int[inputTransforms.size()]);
	}

	public TableOperatorWrapperGenerator(
			List<Transformation<?>> inputTransforms,
			Transformation<?> tailTransform,
			int[] readOrders) {
		this.inputTransforms = inputTransforms;
		this.tailTransform = tailTransform;
		this.readOrders = readOrders;
		this.inputInfoList = new ArrayList<>();
		this.headWrappers = new ArrayList<>();
		this.visitedTransforms = new IdentityHashMap<>();

		this.parallelism = -1;
		this.maxParallelism = -1;
	}

	public void generate() {
		tailWrapper = visit(tailTransform);
		checkState(inputTransforms.size() == inputInfoList.size());
		calculateManagedMemoryFraction();
	}

	public List<InputInfo> getInputInfoList() {
		return inputInfoList;
	}

	@Nullable
	public TypeInformation<?> getStateKeyType() {
		return stateKeyType;
	}

	public List<TableOperatorWrapper<?>> getHeadWrappers() {
		return headWrappers;
	}

	public TableOperatorWrapper<?> getTailWrapper() {
		return tailWrapper;
	}

	public int getParallelism() {
		return parallelism;
	}

	public int getMaxParallelism() {
		return maxParallelism;
	}

	public ResourceSpec getMinResources() {
		return minResources;
	}

	public ResourceSpec getPreferredResources() {
		return preferredResources;
	}

	public long getManagedMemoryWeight() {
		return managedMemoryWeight;
	}

	private TableOperatorWrapper<?> visit(Transformation<?> transform) {
		// ignore UnionTransformation because it's not a real operator
		if (!(transform instanceof UnionTransformation)) {
			calcParallelismAndResource(transform);
		}

		return visitedTransforms.computeIfAbsent(transform, this::visitTransformation);
	}

	private void calcParallelismAndResource(Transformation<?> transform) {
		// do not check the parallelisms in multiple-input node are same,
		// because we should consider the following case:
		// Source1(100 parallelism) -> Calc(100 parallelism) -\
		//                                                     -> union -> join -> ...
		// Source2(50 parallelism)  -> Calc(50 parallelism) -/
		parallelism = Math.max(parallelism, transform.getParallelism());

		int currentMaxParallelism = transform.getMaxParallelism();
		if (maxParallelism < 0) {
			maxParallelism = currentMaxParallelism;
		} else {
			checkState(
					currentMaxParallelism < 0 || maxParallelism == currentMaxParallelism,
					"Max parallelism of a transformation in MultipleInput node is different from others. This is a bug.");
		}

		if (minResources == null) {
			minResources = transform.getMinResources();
			preferredResources = transform.getPreferredResources();
			managedMemoryWeight = transform.getManagedMemoryOperatorScopeUseCaseWeights()
					.getOrDefault(ManagedMemoryUseCase.BATCH_OP, 0);
		} else {
			minResources = minResources.merge(transform.getMinResources());
			preferredResources = preferredResources.merge(transform.getPreferredResources());
			managedMemoryWeight += transform.getManagedMemoryOperatorScopeUseCaseWeights()
					.getOrDefault(ManagedMemoryUseCase.BATCH_OP, 0);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private TableOperatorWrapper<?> visitTransformation(Transformation<?> transform) {
		if (transform instanceof OneInputTransformation) {
			return visitOneInputTransformation((OneInputTransformation) transform);
		} else if (transform instanceof TwoInputTransformation) {
			return visitTwoInputTransformation((TwoInputTransformation) transform);
		} else if (transform instanceof UnionTransformation) {
			return visitUnionTransformation((UnionTransformation) transform);
		} else {
			throw new RuntimeException("Unsupported Transformation: " + transform);
		}
	}

	private TableOperatorWrapper<?> visitOneInputTransformation(
			OneInputTransformation<RowData, RowData> transform) {
		Transformation<?> input = transform.getInputs().get(0);

		TableOperatorWrapper<?> wrapper = new TableOperatorWrapper<>(
				transform.getOperatorFactory(),
				genSubOperatorName(transform),
				Collections.singletonList(transform.getInputType()),
				transform.getOutputType()
		);

		int inputIdx = inputTransforms.indexOf(input);
		if (inputIdx >= 0) {
			processInput(input, inputIdx, wrapper, 1, transform.getStateKeySelector(), transform.getStateKeyType());
			headWrappers.add(wrapper);
		} else {
			TableOperatorWrapper<?> inputWrapper = visit(input);
			wrapper.addInput(inputWrapper, 1, transform.getStateKeySelector());
		}
		return wrapper;
	}

	private TableOperatorWrapper<?> visitTwoInputTransformation(
			TwoInputTransformation<RowData, RowData, RowData> transform) {
		Transformation<?> input1 = transform.getInput1();
		Transformation<?> input2 = transform.getInput2();
		int inputIdx1 = inputTransforms.indexOf(input1);
		int inputIdx2 = inputTransforms.indexOf(input2);

		TableOperatorWrapper<?> wrapper = new TableOperatorWrapper<>(
				transform.getOperatorFactory(),
				genSubOperatorName(transform),
				Arrays.asList(transform.getInputType1(), transform.getInputType2()),
				transform.getOutputType());

		if (inputIdx1 >= 0 && inputIdx2 >= 0) {
			processInput(input1, inputIdx1, wrapper, 1, transform.getStateKeySelector1(), transform.getStateKeyType());
			processInput(input2, inputIdx2, wrapper, 2, transform.getStateKeySelector2(), transform.getStateKeyType());
			headWrappers.add(wrapper);
		} else if (inputIdx1 >= 0) {
			TableOperatorWrapper<?> inputWrapper = visit(input2);
			wrapper.addInput(inputWrapper, 2, transform.getStateKeySelector2());

			processInput(input1, inputIdx1, wrapper, 1, transform.getStateKeySelector1(), transform.getStateKeyType());
			headWrappers.add(wrapper);
		} else if (inputIdx2 >= 0) {
			TableOperatorWrapper<?> inputWrapper = visit(input1);
			wrapper.addInput(inputWrapper, 1, transform.getStateKeySelector1());

			processInput(input2, inputIdx2, wrapper, 2, transform.getStateKeySelector2(), transform.getStateKeyType());
			headWrappers.add(wrapper);
		} else {
			TableOperatorWrapper<?> inputWrapper1 = visit(input1);
			wrapper.addInput(inputWrapper1, 1, transform.getStateKeySelector1());
			TableOperatorWrapper<?> inputWrapper2 = visit(input2);
			wrapper.addInput(inputWrapper2, 2, transform.getStateKeySelector2());
		}

		return wrapper;
	}

	private TableOperatorWrapper<?> visitUnionTransformation(
			UnionTransformation<RowData> transform) {
		TableOperatorWrapper<?> wrapper = new TableOperatorWrapper<>(
				SimpleOperatorFactory.of(new UnionStreamOperator()),
				genSubOperatorName(transform),
				transform.getInputs().stream().map(Transformation::getOutputType).collect(Collectors.toList()),
				transform.getOutputType());

		int numberOfHeadInput = 0;
		for (Transformation<?> input : transform.getInputs()) {
			int inputIdx = inputTransforms.indexOf(input);
			if (inputIdx >= 0) {
				numberOfHeadInput++;
				processInput(input, inputIdx, wrapper, 1, null, null); // always 1 here
			} else {
				TableOperatorWrapper<?> inputWrapper = visit(input);
				wrapper.addInput(inputWrapper, 1); // always 1 here
			}
		}

		if (numberOfHeadInput > 0) {
			headWrappers.add(wrapper);
		}
		return wrapper;
	}

	private void processInput(
			Transformation<?> input,
			int inputIdx,
			TableOperatorWrapper<?> outputWrapper,
			int outputOpInputId,
			@Nullable KeySelector<?, ?> keySelector,
			@Nullable TypeInformation<?> stateKeyType) {
		checkAndSetStateKeyType(stateKeyType);
		int inputId = inputInfoList.size() + 1;
		InputSpec inputSpec = new InputSpec(inputId, readOrders[inputIdx], outputWrapper, outputOpInputId);
		inputInfoList.add(new InputInfo(input, inputSpec, keySelector));
	}

	private void checkAndSetStateKeyType(TypeInformation<?> target) {
		if (stateKeyType != null) {
			if (!stateKeyType.equals(target)) {
				throw new TableException("This should not happen.");
			}
		} else {
			stateKeyType = target;
		}
	}

	/**
	 * calculate managed memory fraction for each operator wrapper.
	 */
	private void calculateManagedMemoryFraction() {
		for (Map.Entry<Transformation<?>, TableOperatorWrapper<?>> entry : visitedTransforms.entrySet()) {
			double fraction = 0;
			if (managedMemoryWeight != 0) {
				fraction = entry.getKey().getManagedMemoryOperatorScopeUseCaseWeights()
						.getOrDefault(ManagedMemoryUseCase.BATCH_OP, 0) * 1.0 / this.managedMemoryWeight;
			}
			entry.getValue().setManagedMemoryFraction(fraction);
		}
	}

	private String genSubOperatorName(Transformation<?> transformation) {
		return "SubOp" + (identifierOfSubOp++) + "_" + transformation.getName();
	}

	/**
	 * Describe some the input info for (Keyed)MultipleInputTransformation.
	 */
	public static class InputInfo {
		private final Transformation<?> inputTransform;
		private final InputSpec inputSpec;
		@Nullable
		private final KeySelector<?, ?> keySelector;

		@VisibleForTesting
		InputInfo(Transformation<?> inputTransform,
				InputSpec inputSpec) {
			this(inputTransform, inputSpec, null);
		}

		InputInfo(Transformation<?> inputTransform,
				InputSpec inputSpec,
				@Nullable KeySelector<?, ?> keySelector) {
			this.inputTransform = checkNotNull(inputTransform);
			this.inputSpec = checkNotNull(inputSpec);
			this.keySelector = keySelector;
		}

		public Transformation<?> getInputTransform() {
			return inputTransform;
		}

		public InputSpec getInputSpec() {
			return inputSpec;
		}

		@Nullable
		public KeySelector<?, ?> getKeySelector() {
			return keySelector;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			InputInfo inputInfo = (InputInfo) o;
			return inputTransform.equals(inputInfo.inputTransform) &&
					inputSpec.equals(inputInfo.inputSpec) &&
					Objects.equals(keySelector, inputInfo.keySelector);
		}

		@Override
		public int hashCode() {
			return Objects.hash(inputTransform, inputSpec, keySelector);
		}
	}
}
