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

package org.apache.flink.runtime.zookeeper;

import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.flink.util.Preconditions;

/**
 * Wrapper class for a {@link VersionedValue} so that we don't expose a curator dependency in our
 * internal APIs. Such an exposure is problematic due to the relocation of curator.
 */
public class ZooKeeperVersionedValue<T> {

	private final VersionedValue<T> versionedValue;

	public ZooKeeperVersionedValue(VersionedValue<T> versionedValue) {
		this.versionedValue = Preconditions.checkNotNull(versionedValue);
	}

	public T getValue() {
		return versionedValue.getValue();
	}

	VersionedValue<T> getVersionedValue() {
		return versionedValue;
	}
}
