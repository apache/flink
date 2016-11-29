/**
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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.OutputTag;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

import java.util.Collection;
import java.util.List;

public class SideOutputTransformation<T> extends StreamTransformation<T> {
	private final StreamTransformation<T> input;
	private final OutputTag<T> tag;

	public SideOutputTransformation(StreamTransformation input, final OutputTag<T> tag) {
		super("SideOutput", tag.getTypeInfo(), input.getParallelism());
		this.input = input;
		this.tag = tag;
	}

	/**
	 * Returns the input {@code StreamTransformation}.
	 */
	public StreamTransformation<T> getInput() {
		return input;
	}

	public OutputTag<T> getOutputTag() {
		return tag;
	}

	@Override
	public Collection<StreamTransformation<?>> getTransitivePredecessors() {
		List<StreamTransformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input.getTransitivePredecessors());
		return result;
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		throw new UnsupportedOperationException("Cannot set chaining strategy on Select Transformation.");
	}
}
