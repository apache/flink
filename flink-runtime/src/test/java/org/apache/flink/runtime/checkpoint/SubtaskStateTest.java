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

package org.apache.flink.runtime.checkpoint;


import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SubtaskStateTest {

	@Test
	public void testDifferentKeyRange(){
		ChainedStateHandle<StreamStateHandle> legacyOperatorState =
			(ChainedStateHandle<StreamStateHandle>) mock(ChainedStateHandle.class);

		KeyGroupsStateHandle managedKeyedState = mock(KeyGroupsStateHandle.class);
		KeyGroupsStateHandle rawKeyedState = mock(KeyGroupsStateHandle.class);
		KeyGroupRange managedKeyGroupRange = KeyGroupRange.of(0,9);
		KeyGroupRange rawKeyGroupRange = KeyGroupRange.of(10,19);

		when(managedKeyedState.getKeyGroupRange()).thenReturn(managedKeyGroupRange);
		when(rawKeyedState.getKeyGroupRange()).thenReturn(rawKeyGroupRange);

		try {
			new SubtaskState(legacyOperatorState, null, null, managedKeyedState, rawKeyedState);
			fail("Did not throw expected Exception");
		}catch(Exception expected){
			assertTrue(expected.getMessage().contains("is different with"));
		}
	}

	@Test
	public void testSameKeyRange(){
		ChainedStateHandle<StreamStateHandle> legacyOperatorState =
			(ChainedStateHandle<StreamStateHandle>) mock(ChainedStateHandle.class);

		KeyGroupsStateHandle managedKeyedState = mock(KeyGroupsStateHandle.class);
		KeyGroupsStateHandle rawKeyedState = mock(KeyGroupsStateHandle.class);
		KeyGroupRange managedKeyGroupRange = KeyGroupRange.of(0,9);
		KeyGroupRange rawKeyGroupRange = KeyGroupRange.of(0,9);

		when(managedKeyedState.getKeyGroupRange()).thenReturn(managedKeyGroupRange);
		when(rawKeyedState.getKeyGroupRange()).thenReturn(rawKeyGroupRange);

		try {
			new SubtaskState(legacyOperatorState, null, null, managedKeyedState, rawKeyedState);
		}catch(Exception unExpected){
			fail("Throw unExpected Exception");
		}
	}

	@Test
	public void testOnlyHasKeyedState(){
		ChainedStateHandle<StreamStateHandle> legacyOperatorState =
			(ChainedStateHandle<StreamStateHandle>) mock(ChainedStateHandle.class);

		KeyGroupsStateHandle managedKeyedState = mock(KeyGroupsStateHandle.class);
		KeyGroupRange managedKeyGroupRange = KeyGroupRange.of(0,9);
		KeyGroupRangeOffsets managedKeyGroupRangeOffsets = mock(KeyGroupRangeOffsets.class);

		when(managedKeyedState.getKeyGroupRange()).thenReturn(managedKeyGroupRange);

		try {
			new SubtaskState(legacyOperatorState, null, null, managedKeyedState, null);
		}catch(Exception unExpected){
			fail("Throw unExpected Exception");
		}
	}
}