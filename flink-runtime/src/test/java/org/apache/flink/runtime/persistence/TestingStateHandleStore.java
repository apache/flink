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

package org.apache.flink.runtime.persistence;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Testing implementation for {@link StateHandleStore}.
 * @param <T> Type of state
 */
public class TestingStateHandleStore<T extends Serializable> implements StateHandleStore<T, IntegerResourceVersion> {

	private final BiFunctionWithException<String, T, RetrievableStateHandle<T>, Exception> addFunction;
	private final ThrowingConsumer<Tuple3<String, IntegerResourceVersion, T>, Exception> replaceConsumer;
	private final FunctionWithException<String, IntegerResourceVersion, Exception> existsFunction;
	private final FunctionWithException<String, RetrievableStateHandle<T>, Exception> getFunction;
	private final SupplierWithException<List<Tuple2<RetrievableStateHandle<T>, String>>, Exception> getAllSupplier;
	private final SupplierWithException<Collection<String>, Exception> getAllHandlesSupplier;
	private final FunctionWithException<String, Boolean, Exception> removeFunction;
	private final RunnableWithException removeAllRunnable;
	private final RunnableWithException clearEntriesRunnable;
	private final ThrowingConsumer<String, Exception> releaseConsumer;
	private final RunnableWithException releaseAllHandlesRunnable;

	private TestingStateHandleStore(
			BiFunctionWithException<String, T, RetrievableStateHandle<T>, Exception> addFunction,
			ThrowingConsumer<Tuple3<String, IntegerResourceVersion, T>, Exception> replaceConsumer,
			FunctionWithException<String, IntegerResourceVersion, Exception> existsFunction,
			FunctionWithException<String, RetrievableStateHandle<T>, Exception> getFunction,
			SupplierWithException<List<Tuple2<RetrievableStateHandle<T>, String>>, Exception> getAllSupplier,
			SupplierWithException<Collection<String>, Exception> getAllHandlesSupplier,
			FunctionWithException<String, Boolean, Exception> removeFunction,
			RunnableWithException removeAllRunnable,
			RunnableWithException clearEntriesRunnable,
			ThrowingConsumer<String, Exception> releaseConsumer,
			RunnableWithException releaseAllHandlesRunnable) {
		this.addFunction = addFunction;
		this.replaceConsumer = replaceConsumer;
		this.existsFunction = existsFunction;
		this.getFunction = getFunction;
		this.getAllSupplier = getAllSupplier;
		this.getAllHandlesSupplier = getAllHandlesSupplier;
		this.removeFunction = removeFunction;
		this.removeAllRunnable = removeAllRunnable;
		this.clearEntriesRunnable = clearEntriesRunnable;
		this.releaseConsumer = releaseConsumer;
		this.releaseAllHandlesRunnable = releaseAllHandlesRunnable;
	}

	@Override
	@Nullable
	public RetrievableStateHandle<T> addAndLock(String name, T state) throws Exception {
		return addFunction.apply(name, state);
	}

	@Override
	public void replace(String name, IntegerResourceVersion resourceVersion, T state) throws Exception {
		replaceConsumer.accept(new Tuple3<>(name, resourceVersion, state));
	}

	@Override
	public IntegerResourceVersion exists(String name) throws Exception {
		return existsFunction.apply(name);
	}

	@Override
	@Nullable
	public RetrievableStateHandle<T> getAndLock(String name) throws Exception {
		return getFunction.apply(name);
	}

	@Override
	public List<Tuple2<RetrievableStateHandle<T>, String>> getAllAndLock() throws Exception {
		return getAllSupplier.get();
	}

	@Override
	public Collection<String> getAllHandles() throws Exception {
		return getAllHandlesSupplier.get();
	}

	@Override
	public boolean releaseAndTryRemove(String name) throws Exception {
		return removeFunction.apply(name);
	}

	@Override
	public void releaseAndTryRemoveAll() throws Exception {
		removeAllRunnable.run();
	}

	@Override
	public void clearEntries() throws Exception {
		clearEntriesRunnable.run();
	}

	@Override
	public void release(String name) throws Exception {
		releaseConsumer.accept(name);
	}

	@Override
	public void releaseAll() throws Exception {
		releaseAllHandlesRunnable.run();
	}

	public static <T extends Serializable> Builder<T> builder() {
		return new Builder<>();
	}

	/**
	 * Builder class for {@link TestingStateHandleStore}.
	 */
	public static class Builder<T extends Serializable> {

		private BiFunctionWithException<String, T, RetrievableStateHandle<T>, Exception> addFunction =
			(ignore, state) -> null;
		private ThrowingConsumer<Tuple3<String, IntegerResourceVersion, T>, Exception> replaceConsumer = ignore -> {};
		private FunctionWithException<String, IntegerResourceVersion, Exception> existsFunction =
			ignore -> IntegerResourceVersion.notExisting();
		private FunctionWithException<String, RetrievableStateHandle<T>, Exception> getFunction =
			ignore -> null;
		private SupplierWithException<List<Tuple2<RetrievableStateHandle<T>, String>>, Exception> getAllSupplier =
			Collections::emptyList;
		private SupplierWithException<Collection<String>, Exception> getAllHandlesSupplier = Collections::emptyList;
		private FunctionWithException<String, Boolean, Exception> removeFunction = ignore -> false;
		private RunnableWithException removeAllRunnable = () -> {};
		private RunnableWithException clearEntriesRunnable = () -> {};

		private ThrowingConsumer<String, Exception> releaseConsumer = ignore -> {};
		private RunnableWithException releaseAllHandlesRunnable = () -> {};

		private Builder() {}

		public Builder<T> setAddFunction(
			BiFunctionWithException<String, T, RetrievableStateHandle<T>,
				Exception> addFunction) {
			this.addFunction = addFunction;
			return this;
		}

		public Builder<T> setReplaceConsumer(ThrowingConsumer<Tuple3<String, IntegerResourceVersion, T>, Exception> replaceConsumer) {
			this.replaceConsumer = replaceConsumer;
			return this;
		}

		public Builder<T> setExistsFunction(FunctionWithException<String, IntegerResourceVersion, Exception> existsFunction) {
			this.existsFunction = existsFunction;
			return this;
		}

		public Builder<T> setGetFunction(FunctionWithException<String, RetrievableStateHandle<T>, Exception> getFunction) {
			this.getFunction = getFunction;
			return this;
		}

		public Builder<T> setGetAllSupplier(
				SupplierWithException<List<Tuple2<RetrievableStateHandle<T>, String>>,
				Exception> getAllSupplier) {
			this.getAllSupplier = getAllSupplier;
			return this;
		}

		public Builder<T> setGetAllHandlesSupplier(
				SupplierWithException<Collection<String>, Exception> getAllHandlesSupplier) {
			this.getAllHandlesSupplier = getAllHandlesSupplier;
			return this;
		}

		public Builder<T> setRemoveFunction(FunctionWithException<String, Boolean, Exception> removeFunction) {
			this.removeFunction = removeFunction;
			return this;
		}

		public Builder<T> setRemoveAllRunnable(RunnableWithException removeAllRunnable) {
			this.removeAllRunnable = removeAllRunnable;
			return this;
		}

		public Builder<T> setClearEntriesRunnable(RunnableWithException clearEntriesRunnable) {
			this.clearEntriesRunnable = clearEntriesRunnable;
			return this;
		}

		public Builder<T> setReleaseConsumer(ThrowingConsumer<String, Exception> releaseConsumer) {
			this.releaseConsumer = releaseConsumer;
			return this;
		}

		public Builder<T> setReleaseAllHandlesRunnable(RunnableWithException releaseAllHandlesRunnable) {
			this.releaseAllHandlesRunnable = releaseAllHandlesRunnable;
			return this;
		}

		public TestingStateHandleStore<T> build() {
			return new TestingStateHandleStore<>(
				addFunction,
				replaceConsumer,
				existsFunction,
				getFunction,
				getAllSupplier,
				getAllHandlesSupplier,
				removeFunction,
				removeAllRunnable,
				clearEntriesRunnable,
				releaseConsumer,
				releaseAllHandlesRunnable);
		}
	}
}
