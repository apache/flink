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

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.transformations.UnionTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSpec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A generator that generates a {@link TableOperatorWrapper} graph from a graph of {@link Transformation}.
 */
public class TableOperatorWrapperGenerator {

	/**
	 * Original input transformations for {@link MultipleInputStreamOperator}.
	 */
	private final List<Transformation<?>> inputTransforms;

	/**
	 * The tail (root) transformation of the transformation-graph in {@link MultipleInputStreamOperator}.
	 */
	private final Transformation<?> tailTransform;

	/**
	 * The read order corresponding to each transformation in {@link #inputTransforms}.
	 */
	private final int[] readOrders;

	/**
	 * Reordered input transformations which order corresponds to the order of {@link #inputSpecs}.
	 */
	private final List<Transformation<?>> orderedInputTransforms;

	/**
	 * The input specs which order corresponds to the order of {@link #orderedInputTransforms}.
	 */
	private final List<InputSpec> inputSpecs;

	/**
	 * The head (leaf) operator wrappers of the operator-graph in {@link MultipleInputStreamOperator}.
	 */
	private final List<TableOperatorWrapper<?>> headWrappers;

	/**
	 * The tail (root) operator wrapper of the operator-graph in {@link MultipleInputStreamOperator}.
	 */
	private TableOperatorWrapper<?> tailWrapper;

	/**
	 * Map the visited transformation to its generated TableOperatorWrapper.
	 */
	private final Map<Transformation<?>, TableOperatorWrapper<?>> visitedTransforms;
	/**
	 * The identifier for each sub operator in {@link MultipleInputStreamOperator}.
	 */
	private int identifierOfSubOp = 0;

	private int parallelism;
	private int maxParallelism;
	private ResourceSpec minResources;
	private ResourceSpec preferredResources;
	/**
	 * managed memory weight for batch operator.
	 */
	private int managedMemoryWeight;

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
		this.inputSpecs = new ArrayList<>();
		this.headWrappers = new ArrayList<>();
		this.orderedInputTransforms = new ArrayList<>();
		this.visitedTransforms = new IdentityHashMap<>();

		this.parallelism = -1;
		this.maxParallelism = -1;
	}

	public void generate() {
		tailWrapper = visit(tailTransform);
		checkState(orderedInputTransforms.size() == inputTransforms.size());
		checkState(orderedInputTransforms.size() == inputSpecs.size());
		calculateManagedMemoryFraction();
	}

	public List<Transformation<?>> getOrderedInputTransforms() {
		return orderedInputTransforms;
	}

	public List<InputSpec> getInputSpecs() {
		return inputSpecs;
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

	public int getManagedMemoryWeight() {
		return managedMemoryWeight;
	}

	private TableOperatorWrapper<?> visit(Transformation<?> transform) {
		// ignore UnionTransformation because it's not a really operator
		if (!(transform instanceof UnionTransformation)) {
			calcParallelismAndResource(transform);
		}

		final TableOperatorWrapper<?> wrapper;
		if (visitedTransforms.containsKey(transform)) {
			wrapper = visitedTransforms.get(transform);
		} else {
			wrapper = visitTransformation(transform);
			visitedTransforms.put(transform, wrapper);
		}
		return wrapper;
	}

	private void calcParallelismAndResource(Transformation<?> transform) {
		int currentParallelism = transform.getParallelism();
		if (parallelism < 0) {
			parallelism = currentParallelism;
		} else {
			checkState(
					currentParallelism < 0 || parallelism == currentParallelism,
					"Parallelism of a transformation in MultipleInputNode is different from others. This is a bug.");
		}

		int currentMaxParallelism = transform.getMaxParallelism();
		if (maxParallelism < 0) {
			maxParallelism = currentMaxParallelism;
		} else {
			checkState(
					currentMaxParallelism < 0 || maxParallelism == currentMaxParallelism,
					"Max parallelism of a transformation in MultipleInputNode is different from others. This is a bug.");
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

	@SuppressWarnings({"unchecked", "rawtypes"})
	private TableOperatorWrapper<?> visitTransformation(Transformation<?> transform) {
		if (transform instanceof OneInputTransformation) {
			return visitOneInputTransformation((OneInputTransformation) transform);
		} else if (transform instanceof TwoInputTransformation) {
			return visitTwoInputTransformation((TwoInputTransformation) transform);
		} else  if (transform instanceof UnionTransformation) {
			return visitUnionTransformation((UnionTransformation) transform);
		} else  {
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
			orderedInputTransforms.add(input);
			inputSpecs.add(createInputSpec(readOrders[inputIdx], wrapper, 1));
			headWrappers.add(wrapper);
		} else {
			TableOperatorWrapper<?> inputWrapper = visit(input);
			wrapper.addInput(inputWrapper, 1);
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
			orderedInputTransforms.add(input1);
			inputSpecs.add(createInputSpec(readOrders[inputIdx1], wrapper, 1));
			orderedInputTransforms.add(input2);
			inputSpecs.add(createInputSpec(readOrders[inputIdx2], wrapper, 2));
			headWrappers.add(wrapper);
		} else if (inputIdx1 >= 0) {
			TableOperatorWrapper<?> inputWrapper = visit(input2);
			wrapper.addInput(inputWrapper, 2);
			orderedInputTransforms.add(input1);
			inputSpecs.add(createInputSpec(readOrders[inputIdx1], wrapper, 1));
			headWrappers.add(wrapper);
		} else if (inputIdx2 >= 0) {
			TableOperatorWrapper<?> inputWrapper = visit(input1);
			wrapper.addInput(inputWrapper, 1);
			orderedInputTransforms.add(input2);
			inputSpecs.add(createInputSpec(readOrders[inputIdx2], wrapper, 2));
			headWrappers.add(wrapper);
		} else {
			TableOperatorWrapper<?> inputWrapper1 = visit(input1);
			wrapper.addInput(inputWrapper1, 1);
			TableOperatorWrapper<?> inputWrapper2 = visit(input2);
			wrapper.addInput(inputWrapper2, 2);
		}

		return wrapper;
	}

	private TableOperatorWrapper<?> visitUnionTransformation(
			UnionTransformation<RowData> transform) {
		// use MapFunction to combine the input data
		TableOperatorWrapper<?> wrapper = new TableOperatorWrapper<>(
				SimpleOperatorFactory.of(new UnionStreamOperator()),
				genSubOperatorName(transform),
				transform.getInputs().stream().map(Transformation::getOutputType).collect(Collectors.toList()),
				transform.getOutputType());

		int numberOfHeadInput = 0;
		for (Transformation<?> input : transform.getInputs()) {
			int inputIdx = inputTransforms.indexOf(input);
			if (inputIdx >= 0) {
				numberOfHeadInput ++;
				orderedInputTransforms.add(input);
				inputSpecs.add(createInputSpec(readOrders[inputIdx], wrapper, 1)); // always 1 here
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

	private InputSpec createInputSpec(int readOrder, TableOperatorWrapper<?> outputWrapper, int inputIndex) {
		checkNotNull(outputWrapper);
		int inputId = inputSpecs.size() + 1;
		return new InputSpec(inputId, readOrder, outputWrapper, inputIndex);
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
}
