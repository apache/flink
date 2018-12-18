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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import static java.util.Objects.requireNonNull;

/**
 * {@link StateDescriptor} for {@link FoldingState}. This can be used to create partitioned
 * folding state.
 *
 * @param <T> Type of the values folded in the other state
 * @param <ACC> Type of the value in the state
 *
 * @deprecated will be removed in a future version in favor of {@link AggregatingStateDescriptor}
 */
@PublicEvolving
@Deprecated
public class FoldingStateDescriptor<T, ACC> extends StateDescriptor<FoldingState<T, ACC>, ACC> {
	private static final long serialVersionUID = 1L;


	private final FoldFunction<T, ACC> foldFunction;

	/**
	 * Creates a new {@code FoldingStateDescriptor} with the given name, type, and initial value.
	 *
	 * <p>If this constructor fails (because it is not possible to describe the type via a class),
	 * consider using the {@link #FoldingStateDescriptor(String, ACC, FoldFunction, TypeInformation)} constructor.
	 *
	 * @param name The (unique) name for the state.
	 * @param initialValue The initial value of the fold.
	 * @param foldFunction The {@code FoldFunction} used to aggregate the state.
	 * @param typeClass The type of the values in the state.
	 */
	public FoldingStateDescriptor(String name, ACC initialValue, FoldFunction<T, ACC> foldFunction, Class<ACC> typeClass) {
		super(name, typeClass, initialValue);
		this.foldFunction = requireNonNull(foldFunction);

		if (foldFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction of FoldingState can not be a RichFunction.");
		}
	}

	/**
	 * Creates a new {@code FoldingStateDescriptor} with the given name and default value.
	 *
	 * @param name The (unique) name for the state.
	 * @param initialValue The initial value of the fold.
	 * @param foldFunction The {@code FoldFunction} used to aggregate the state.
	 * @param typeInfo The type of the values in the state.
	 */
	public FoldingStateDescriptor(String name, ACC initialValue, FoldFunction<T, ACC> foldFunction, TypeInformation<ACC> typeInfo) {
		super(name, typeInfo, initialValue);
		this.foldFunction = requireNonNull(foldFunction);

		if (foldFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction of FoldingState can not be a RichFunction.");
		}
	}

	/**
	 * Creates a new {@code ValueStateDescriptor} with the given name and default value.
	 *
	 * @param name The (unique) name for the state.
	 * @param initialValue The initial value of the fold.
	 * @param foldFunction The {@code FoldFunction} used to aggregate the state.
	 * @param typeSerializer The type serializer of the values in the state.
	 */
	public FoldingStateDescriptor(String name, ACC initialValue, FoldFunction<T, ACC> foldFunction, TypeSerializer<ACC> typeSerializer) {
		super(name, typeSerializer, initialValue);
		this.foldFunction = requireNonNull(foldFunction);

		if (foldFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("FoldFunction of FoldingState can not be a RichFunction.");
		}
	}

	/**
	 * Returns the fold function to be used for the folding state.
	 */
	public FoldFunction<T, ACC> getFoldFunction() {
		return foldFunction;
	}

	@Override
	public Type getType() {
		return Type.FOLDING;
	}
}
