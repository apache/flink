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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.util.OutputTag;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;


/**
 * This transformation represents a selection of a side output of an upstream operation with a
 * given {@link OutputTag}.
 *
 * <p>This does not create a physical operation, it only affects how upstream operations are
 * connected to downstream operations.
 *
 * @param <T> The type of the elements that result from this {@code SideOutputTransformation}
 */
public class SideOutputTransformation<T> extends Transformation<T> {
	private final Transformation<?> input;

	private final OutputTag<T> tag;

	public SideOutputTransformation(Transformation<?> input, final OutputTag<T> tag) {
		super("SideOutput", tag.getTypeInfo(), requireNonNull(input).getParallelism());
		this.input = input;
		this.tag = requireNonNull(tag);
	}

	public OutputTag<T> getOutputTag() {
		return tag;
	}

	@Override
	public List<Transformation<?>> getTransitivePredecessors() {
		List<Transformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input.getTransitivePredecessors());
		return result;
	}

	@Override
	public List<Transformation<?>> getInputs() {
		return Collections.singletonList(input);
	}
}
