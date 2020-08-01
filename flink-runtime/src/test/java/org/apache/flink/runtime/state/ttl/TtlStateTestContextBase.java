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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.runtime.state.internal.InternalKvState;

import java.util.Objects;

/** Base class for state TTL test context. */
public abstract class TtlStateTestContextBase<S extends InternalKvState<?, String, ?>, UV, GV> {
	public S ttlState;

	public UV updateEmpty;
	public UV updateUnexpired;
	public UV updateExpired;

	public GV getUpdateEmpty;
	public GV getUnexpired;
	GV getUpdateExpired;

	public GV emptyValue = null;

	abstract void initTestValues();

	public abstract <US extends State, SV> StateDescriptor<US, SV> createStateDescriptor();

	public abstract void update(UV value) throws Exception;

	public abstract GV get() throws Exception;

	public abstract Object getOriginal() throws Exception;

	public boolean isOriginalEmptyValue() throws Exception {
		return Objects.equals(emptyValue, getOriginal());
	}

	public String getName() {
		return this.getClass().getSimpleName();
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName();
	}
}
