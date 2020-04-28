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

package org.apache.flink.runtime.execution.librarycache;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.function.QuadConsumer;
import org.apache.flink.util.function.TriConsumer;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Testing {@link LibraryCacheManager} implementation.
 */
public class TestingLibraryCacheManager implements LibraryCacheManager {
	private final Function<JobID, ClassLoader> getClassLoaderFunction;
	private final TriConsumer<JobID, Collection<PermanentBlobKey>, Collection<URL>> registerJobConsumer;
	private final QuadConsumer<JobID, ExecutionAttemptID, Collection<PermanentBlobKey>, Collection<URL>> registerTaskConsumer;
	private final BiConsumer<JobID, ExecutionAttemptID> unregisterTaskConsumer;
	private final Consumer<JobID> unregisterJobConsumer;
	private final Runnable shutdownRunnable;
	private final Function<JobID, Boolean> hasClassLoaderFunction;

	private TestingLibraryCacheManager(
			Function<JobID, ClassLoader> getClassLoaderFunction,
			TriConsumer<JobID, Collection<PermanentBlobKey>, Collection<URL>> registerJobConsumer,
			QuadConsumer<JobID, ExecutionAttemptID, Collection<PermanentBlobKey>, Collection<URL>> registerTaskConsumer,
			BiConsumer<JobID, ExecutionAttemptID> unregisterTaskConsumer, Consumer<JobID> unregisterJobConsumer,
			Runnable shutdownRunnable,
			Function<JobID, Boolean> hasClassLoaderFunction) {
		this.getClassLoaderFunction = getClassLoaderFunction;
		this.registerJobConsumer = registerJobConsumer;
		this.registerTaskConsumer = registerTaskConsumer;
		this.unregisterTaskConsumer = unregisterTaskConsumer;
		this.unregisterJobConsumer = unregisterJobConsumer;
		this.shutdownRunnable = shutdownRunnable;
		this.hasClassLoaderFunction = hasClassLoaderFunction;
	}

	@Override
	public ClassLoader getClassLoader(JobID id) {
		return getClassLoaderFunction.apply(id);
	}

	@Override
	public void registerJob(JobID id, Collection<PermanentBlobKey> requiredJarFiles, Collection<URL> requiredClasspaths) throws IOException {
		registerJobConsumer.accept(id, requiredJarFiles, requiredClasspaths);
	}

	@Override
	public void registerTask(JobID id, ExecutionAttemptID execution, Collection<PermanentBlobKey> requiredJarFiles, Collection<URL> requiredClasspaths) throws IOException {
		registerTaskConsumer.accept(id, execution, requiredJarFiles, requiredClasspaths);
	}

	@Override
	public void unregisterTask(JobID id, ExecutionAttemptID execution) {
		unregisterTaskConsumer.accept(id, execution);
	}

	@Override
	public void unregisterJob(JobID id) {
		unregisterJobConsumer.accept(id);
	}

	@Override
	public void shutdown() {
		shutdownRunnable.run();
	}

	@Override
	public boolean hasClassLoader(@Nonnull JobID jobId) {
		return hasClassLoaderFunction.apply(jobId);
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static final class Builder {

		private Function<JobID, ClassLoader> getClassLoaderFunction = ignored -> Builder.class.getClassLoader();
		private TriConsumer<JobID, Collection<PermanentBlobKey>, Collection<URL>> registerJobConsumer = (ignoredA, ignoredB, ignoredC) -> {};
		private QuadConsumer<JobID, ExecutionAttemptID, Collection<PermanentBlobKey>, Collection<URL>> registerTaskConsumer = (ignoredA, ignoredB, ignoredC, ignoredD) -> {};
		private BiConsumer<JobID, ExecutionAttemptID> unregisterTaskConsumer = (ignoredA, ignoredB) -> {};
		private Consumer<JobID> unregisterJobConsumer = ignored -> {};
		private Runnable shutdownRunnable = () -> {};
		private Function<JobID, Boolean> hasClassLoaderFunction = ignored -> false;

		private Builder() {}

		public Builder setGetClassLoaderFunction(Function<JobID, ClassLoader> getClassLoaderFunction) {
			this.getClassLoaderFunction = getClassLoaderFunction;
			return this;
		}

		public Builder setRegisterJobConsumer(TriConsumer<JobID, Collection<PermanentBlobKey>, Collection<URL>> registerJobConsumer) {
			this.registerJobConsumer = registerJobConsumer;
			return this;
		}

		public Builder setRegisterTaskConsumer(QuadConsumer<JobID, ExecutionAttemptID, Collection<PermanentBlobKey>, Collection<URL>> registerTaskConsumer) {
			this.registerTaskConsumer = registerTaskConsumer;
			return this;
		}

		public Builder setUnregisterTaskConsumer(BiConsumer<JobID, ExecutionAttemptID> unregisterTaskConsumer) {
			this.unregisterTaskConsumer = unregisterTaskConsumer;
			return this;
		}

		public Builder setUnregisterJobConsumer(Consumer<JobID> unregisterJobConsumer) {
			this.unregisterJobConsumer = unregisterJobConsumer;
			return this;
		}

		public Builder setShutdownRunnable(Runnable shutdownRunnable) {
			this.shutdownRunnable = shutdownRunnable;
			return this;
		}

		public Builder setHasClassLoaderFunction(Function<JobID, Boolean> hasClassLoaderFunction) {
			this.hasClassLoaderFunction = hasClassLoaderFunction;
			return this;
		}

		public TestingLibraryCacheManager build() {
			return new TestingLibraryCacheManager(getClassLoaderFunction, registerJobConsumer, registerTaskConsumer, unregisterTaskConsumer, unregisterJobConsumer, shutdownRunnable, hasClassLoaderFunction);
		}
	}
}
