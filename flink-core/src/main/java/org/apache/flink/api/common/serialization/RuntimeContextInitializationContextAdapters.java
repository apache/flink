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

package org.apache.flink.api.common.serialization;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.function.Function;

/**
 * A set of adapters between {@link RuntimeContext}
 * and {@link DeserializationSchema.InitializationContext}
 * or {@link SerializationSchema.InitializationContext}.
 */
@Internal
public final class RuntimeContextInitializationContextAdapters {
	public static DeserializationSchema.InitializationContext deserializationAdapter(
			RuntimeContext runtimeContext) {
		return deserializationAdapter(runtimeContext, Function.identity());
	}

	public static DeserializationSchema.InitializationContext deserializationAdapter(
			RuntimeContext runtimeContext,
			Function<MetricGroup, MetricGroup> mapMetricGroup) {
		return new RuntimeContextDeserializationInitializationContextAdapter(
				runtimeContext,
				mapMetricGroup);
	}

	public static SerializationSchema.InitializationContext serializationAdapter(
			RuntimeContext runtimeContext) {
		return serializationAdapter(runtimeContext, Function.identity());
	}

	public static SerializationSchema.InitializationContext serializationAdapter(
			RuntimeContext runtimeContext,
			Function<MetricGroup, MetricGroup> mapMetricGroup) {
		return new RuntimeContextSerializationInitializationContextAdapter(
				runtimeContext,
				mapMetricGroup);
	}

	private static final class RuntimeContextDeserializationInitializationContextAdapter
			implements DeserializationSchema.InitializationContext {
		private final RuntimeContext runtimeContext;
		private final Function<MetricGroup, MetricGroup> mapMetricGroup;

		private RuntimeContextDeserializationInitializationContextAdapter(
				RuntimeContext runtimeContext,
				Function<MetricGroup, MetricGroup> mapMetricGroup) {
			this.runtimeContext = runtimeContext;
			this.mapMetricGroup = mapMetricGroup;
		}

		@Override
		public MetricGroup getMetricGroup() {
			return mapMetricGroup.apply(runtimeContext.getMetricGroup());
		}

		@Override
		public UserCodeClassLoader getUserCodeClassLoader() {
			return new RuntimeContextUserCodeClassLoaderAdapter(runtimeContext);
		}
	}

	private static final class RuntimeContextSerializationInitializationContextAdapter
			implements SerializationSchema.InitializationContext {
		private final RuntimeContext runtimeContext;
		private final Function<MetricGroup, MetricGroup> mapMetricGroup;

		private RuntimeContextSerializationInitializationContextAdapter(
				RuntimeContext runtimeContext,
				Function<MetricGroup, MetricGroup> mapMetricGroup) {
			this.runtimeContext = runtimeContext;
			this.mapMetricGroup = mapMetricGroup;
		}

		@Override
		public MetricGroup getMetricGroup() {
			return mapMetricGroup.apply(runtimeContext.getMetricGroup());
		}

		@Override
		public UserCodeClassLoader getUserCodeClassLoader() {
			return new RuntimeContextUserCodeClassLoaderAdapter(runtimeContext);
		}
	}

	private static final class RuntimeContextUserCodeClassLoaderAdapter
			implements UserCodeClassLoader {
		private final RuntimeContext runtimeContext;

		private RuntimeContextUserCodeClassLoaderAdapter(RuntimeContext runtimeContext) {
			this.runtimeContext = runtimeContext;
		}

		@Override
		public ClassLoader asClassLoader() {
			return runtimeContext.getUserCodeClassLoader();
		}

		@Override
		public void registerReleaseHookIfAbsent(String releaseHookName, Runnable releaseHook) {
			runtimeContext.registerUserCodeClassLoaderReleaseHookIfAbsent(
					releaseHookName,
					releaseHook);
		}
	}
}
