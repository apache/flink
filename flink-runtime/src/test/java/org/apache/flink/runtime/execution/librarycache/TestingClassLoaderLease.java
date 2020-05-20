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

import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.util.function.BiFunctionWithException;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;

/**
 * Testing implementation of {@link LibraryCacheManager.ClassLoaderLease}.
 */
public class TestingClassLoaderLease implements LibraryCacheManager.ClassLoaderLease {

	private final BiFunctionWithException<Collection<PermanentBlobKey>, Collection<URL>, ClassLoader, IOException> getOrResolveClassLoaderFunction;

	private final Runnable closeRunnable;

	public TestingClassLoaderLease(BiFunctionWithException<Collection<PermanentBlobKey>, Collection<URL>, ClassLoader, IOException> getOrResolveClassLoaderFunction, Runnable closeRunnable) {
		this.getOrResolveClassLoaderFunction = getOrResolveClassLoaderFunction;
		this.closeRunnable = closeRunnable;
	}

	@Override
	public ClassLoader getOrResolveClassLoader(Collection<PermanentBlobKey> requiredJarFiles, Collection<URL> requiredClasspaths) throws IOException {
		return getOrResolveClassLoaderFunction.apply(requiredJarFiles, requiredClasspaths);
	}

	@Override
	public void release() {
		closeRunnable.run();
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static final class Builder {
		private BiFunctionWithException<Collection<PermanentBlobKey>, Collection<URL>, ClassLoader, IOException> getOrResolveClassLoaderFunction = (ignoredA, ignoredB) -> Builder.class.getClassLoader();
		private Runnable closeRunnable = () -> {};

		private Builder() {}

		public Builder setGetOrResolveClassLoaderFunction(BiFunctionWithException<Collection<PermanentBlobKey>, Collection<URL>, ClassLoader, IOException> getOrResolveClassLoaderFunction) {
			this.getOrResolveClassLoaderFunction = getOrResolveClassLoaderFunction;
			return this;
		}

		public Builder setCloseRunnable(Runnable closeRunnable) {
			this.closeRunnable = closeRunnable;
			return this;
		}

		public TestingClassLoaderLease build() {
			return new TestingClassLoaderLease(getOrResolveClassLoaderFunction, closeRunnable);
		}
	}
}
