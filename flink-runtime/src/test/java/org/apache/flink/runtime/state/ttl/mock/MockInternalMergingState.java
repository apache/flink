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

package org.apache.flink.runtime.state.ttl.mock;

import org.apache.flink.runtime.state.internal.InternalMergingState;

import java.util.Collection;
import java.util.function.Supplier;

/** In memory mock internal merging state base class. */
abstract class MockInternalMergingState<K, N, IN, ACC, OUT>
	extends MockInternalKvState<K, N, ACC> implements InternalMergingState<K, N, IN, ACC, OUT> {

	MockInternalMergingState() {
		super();
	}

	MockInternalMergingState(Supplier<ACC> emptyValue) {
		super(emptyValue);
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
		ACC acc = null;
		for (N n : sources) {
			setCurrentNamespace(n);
			ACC nAcc = getInternal();
			acc = nAcc == null ? acc : (acc == null ? nAcc : mergeState(acc, nAcc));
		}
		if (acc != null) {
			setCurrentNamespace(target);
			updateInternal(acc);
		}
	}

	abstract ACC mergeState(ACC acc, ACC nAcc) throws Exception;
}
