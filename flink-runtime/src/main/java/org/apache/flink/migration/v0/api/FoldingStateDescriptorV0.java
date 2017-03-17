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

package org.apache.flink.migration.v0.api;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import static java.util.Objects.requireNonNull;

/**
 * The descriptor for {@link FoldingState}s in SavepointV0.
 *
 * @param <ACC> Type of the value in the state
 */
@Deprecated
@SuppressWarnings("deprecation")
public class FoldingStateDescriptorV0<T, ACC> extends StateDescriptorV0<FoldingState<T, ACC>, ACC> {
	private static final long serialVersionUID = 1L;

	private final FoldFunction<T, ACC> foldFunction;

	/**
	 * Creates a new {@code FoldingStateDescriptorV0} with the given name and default value.
	 *
	 * @param name The (unique) name for the state.
	 * @param initialValue The initial value of the fold.
	 * @param foldFunction The {@code FoldFunction} used to aggregate the state.
	 * @param typeSerializer The type serializer of the values in the state.
	 */
	public FoldingStateDescriptorV0(String name, ACC initialValue, FoldFunction<T, ACC> foldFunction, TypeSerializer<ACC> typeSerializer) {
		super(name, typeSerializer, initialValue);
		this.foldFunction = requireNonNull(foldFunction);

		if (foldFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction of FoldingState can not be a RichFunction.");
		}
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		FoldingStateDescriptorV0<?, ?> that = (FoldingStateDescriptorV0<?, ?>) o;

		return serializer.equals(that.serializer) && name.equals(that.name);

	}

	@Override
	public StateDescriptor<FoldingState<T, ACC>, ACC> convert() {
		return new FoldingStateDescriptor<>(name, defaultValue, foldFunction, serializer);
	}

	@Override
	public int hashCode() {
		int result = serializer.hashCode();
		result = 31 * result + name.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "FoldingStateDescriptorV0{" +
				"serializer=" + serializer +
				", initialValue=" + defaultValue +
				", foldFunction=" + foldFunction +
				'}';
	}
}
