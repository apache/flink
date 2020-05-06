/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.TestingClassLoaderLease;

import java.util.function.Supplier;

/**
 * Testing implementation of the {@link JobTable.JobServices}.
 */
public class TestingJobServices implements JobTable.JobServices {

	private final Supplier<LibraryCacheManager.ClassLoaderHandle> classLoaderHandleSupplier;

	private final Runnable closeRunnable;

	private TestingJobServices(
			Supplier<LibraryCacheManager.ClassLoaderHandle> classLoaderHandleSupplier,
			Runnable closeRunnable) {
		this.classLoaderHandleSupplier = classLoaderHandleSupplier;
		this.closeRunnable = closeRunnable;
	}

	@Override
	public LibraryCacheManager.ClassLoaderHandle getClassLoaderHandle() {
		return classLoaderHandleSupplier.get();
	}

	@Override
	public void close() {
		closeRunnable.run();
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static final class Builder {

		private final TestingClassLoaderLease testingClassLoaderLease = TestingClassLoaderLease.newBuilder().build();
		private Supplier<LibraryCacheManager.ClassLoaderHandle> classLoaderHandleSupplier = () -> testingClassLoaderLease;
		private Runnable closeRunnable = () -> {};

		public Builder setClassLoaderHandleSupplier(Supplier<LibraryCacheManager.ClassLoaderHandle> classLoaderHandleSupplier) {
			this.classLoaderHandleSupplier = classLoaderHandleSupplier;
			return this;
		}

		public Builder setCloseRunnable(Runnable closeRunnable) {
			this.closeRunnable = closeRunnable;
			return this;
		}

		public TestingJobServices build() {
			return new TestingJobServices(classLoaderHandleSupplier, closeRunnable);
		}
	}
}
