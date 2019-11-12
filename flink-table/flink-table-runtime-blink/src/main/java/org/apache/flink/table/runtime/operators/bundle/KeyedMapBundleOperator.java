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

package org.apache.flink.table.runtime.operators.bundle;

import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTrigger;

/**
 * The {@link KeyedMapBundleOperator} uses framework's key as bundle map key, thus can only be
 * used on {@link org.apache.flink.streaming.api.datastream.KeyedStream}.
 */
public class KeyedMapBundleOperator<K, V, IN, OUT> extends AbstractMapBundleOperator<K, V, IN, OUT> {

	private static final long serialVersionUID = 1L;

	public KeyedMapBundleOperator(
			MapBundleFunction<K, V, IN, OUT> function,
			BundleTrigger<IN> bundleTrigger) {
		super(function, bundleTrigger);
	}

	@Override
	protected K getKey(IN input) throws Exception {
		return (K) getCurrentKey();
	}
}
