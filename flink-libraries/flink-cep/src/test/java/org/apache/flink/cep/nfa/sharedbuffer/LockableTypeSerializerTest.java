/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cep.nfa.sharedbuffer;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.heap.TestDuplicateSerializer;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link org.apache.flink.cep.nfa.sharedbuffer.Lockable.LockableTypeSerializer}.
 */
public class LockableTypeSerializerTest {

	/**
	 * This tests that {@link Lockable.LockableTypeSerializer#duplicate()} works as expected.
	 */
	@Test
	public void testDuplicate() {
		IntSerializer nonDuplicatingInnerSerializer = IntSerializer.INSTANCE;
		Assert.assertSame(nonDuplicatingInnerSerializer, nonDuplicatingInnerSerializer.duplicate());
		Lockable.LockableTypeSerializer<Integer> candidateTestShallowDuplicate =
			new Lockable.LockableTypeSerializer<>(nonDuplicatingInnerSerializer);
		Assert.assertSame(candidateTestShallowDuplicate, candidateTestShallowDuplicate.duplicate());

		TestDuplicateSerializer duplicatingInnerSerializer = new TestDuplicateSerializer();
		Assert.assertNotSame(duplicatingInnerSerializer, duplicatingInnerSerializer.duplicate());

		Lockable.LockableTypeSerializer<Integer> candidateTestDeepDuplicate =
			new Lockable.LockableTypeSerializer<>(duplicatingInnerSerializer);

		Lockable.LockableTypeSerializer<Integer> deepDuplicate = candidateTestDeepDuplicate.duplicate();
		Assert.assertNotSame(candidateTestDeepDuplicate, deepDuplicate);
		Assert.assertNotSame(candidateTestDeepDuplicate.getElementSerializer(), deepDuplicate.getElementSerializer());
	}
}
