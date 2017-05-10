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

package org.apache.flink.streaming.api.graph;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.IOException;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.streaming.api.checkpoint.WithMasterCheckpointHook;
import org.apache.flink.util.SerializedValue;

/**
 * Utility class that turns a {@link WithMasterCheckpointHook} into a
 * {@link org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook.Factory}.
 */
class FunctionMasterCheckpointHookFactory implements MasterTriggerRestoreHook.Factory {

	private static final long serialVersionUID = 2L;

	private final SerializedValue<UserCodeObjectWrapper<?>> serializedCreator;

	FunctionMasterCheckpointHookFactory(WithMasterCheckpointHook<?> creator) {
		UserCodeObjectWrapper<?> wrapper = new UserCodeObjectWrapper<>(checkNotNull(creator));
		try {
			serializedCreator = new SerializedValue<UserCodeObjectWrapper<?>>(wrapper);
		} catch (IOException e) {
			throw new InvalidProgramException("The implementation of WithMasterCheckpointHook is not serializable.", e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> MasterTriggerRestoreHook<V> create(ClassLoader userClassLoader) {
		UserCodeObjectWrapper<WithMasterCheckpointHook<?>> wrapper;
		try {
			wrapper = (UserCodeObjectWrapper<WithMasterCheckpointHook<?>>) serializedCreator.deserializeValue(userClassLoader);
		} catch (ClassNotFoundException | IOException e) {
			throw new InvalidProgramException("The implementation of WithMasterCheckpointHook is not serializable.", e);
		}
		WithMasterCheckpointHook<?> creator = wrapper.getUserCodeObject(WithMasterCheckpointHook.class, userClassLoader);
		return (MasterTriggerRestoreHook<V>) creator.createMasterTriggerRestoreHook();
	}
}
