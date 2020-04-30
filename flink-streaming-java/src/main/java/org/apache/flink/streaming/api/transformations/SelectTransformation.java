/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * This transformation represents a selection of only certain upstream elements. This must
 * follow a {@link org.apache.flink.streaming.api.transformations.SplitTransformation} that
 * splits elements into several logical streams with assigned names.
 *
 * <p>This does not create a physical operation, it only affects how upstream operations are
 * connected to downstream operations.
 *
 * @param <T> The type of the elements that result from this {@code SelectTransformation}
 */
@Internal
public class SelectTransformation<T> extends Transformation<T> {

	private final Transformation<T> input;
	private final List<String> selectedNames;

	/**
	 * Creates a new {@code SelectionTransformation} from the given input that only selects
	 * the streams with the selected names.
	 *
	 * @param input The input {@code Transformation}
	 * @param selectedNames The names from the upstream {@code SplitTransformation} that this
	 *                      {@code SelectTransformation} selects.
	 */
	public SelectTransformation(
		Transformation<T> input,
			List<String> selectedNames) {
		super("Select", input.getOutputType(), input.getParallelism());
		this.input = input;
		this.selectedNames = selectedNames;
	}

	/**
	 * Returns the input {@code Transformation}.
	 */
	public Transformation<T> getInput() {
		return input;
	}

	/**
	 * Returns the names of the split streams that this {@code SelectTransformation} selects.
	 */
	public List<String> getSelectedNames() {
		return selectedNames;
	}

	@Override
	public Collection<Transformation<?>> getTransitivePredecessors() {
		List<Transformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input.getTransitivePredecessors());
		return result;
	}
}
