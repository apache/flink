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

package org.apache.flink.cep;

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotMigrationTestBase;
import org.apache.flink.cep.nfa.DeweyNumber;
import org.apache.flink.cep.nfa.NFAStateSerializer;
import org.apache.flink.cep.nfa.NFAStateSerializerSnapshot;
import org.apache.flink.cep.nfa.sharedbuffer.EventId;
import org.apache.flink.cep.nfa.sharedbuffer.NodeId;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferEdge;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferNode;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

/**
 * Migration tests for NFA-related serializers.
 */
@RunWith(Parameterized.class)
public class NFASerializerSnapshotsMigrationTest extends TypeSerializerSnapshotMigrationTestBase<Object> {

	public NFASerializerSnapshotsMigrationTest(TestSpecification<Object> testSpecification) {
		super(testSpecification);
	}

	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?>> testSpecifications() {

		final TestSpecifications testSpecifications = new TestSpecifications(MigrationVersion.v1_6, MigrationVersion.v1_7);

		testSpecifications.add(
			"event-id-serializer",
			EventId.EventIdSerializer.class,
			EventId.EventIdSerializer.EventIdSerializerSnapshot.class,
			() -> EventId.EventIdSerializer.INSTANCE);
		testSpecifications.add(
			"node-id-serializer",
			NodeId.NodeIdSerializer.class,
			NodeId.NodeIdSerializer.NodeIdSerializerSnapshot.class,
			NodeId.NodeIdSerializer::new);
		testSpecifications.add(
			"dewey-number-serializer",
			DeweyNumber.DeweyNumberSerializer.class,
			DeweyNumber.DeweyNumberSerializer.DeweyNumberSerializerSnapshot.class,
			() -> DeweyNumber.DeweyNumberSerializer.INSTANCE);
		testSpecifications.add(
			"shared-buffer-edge-serializer",
			SharedBufferEdge.SharedBufferEdgeSerializer.class,
			SharedBufferEdge.SharedBufferEdgeSerializer.SharedBufferEdgeSerializerSnapshot.class,
			SharedBufferEdge.SharedBufferEdgeSerializer::new);
		testSpecifications.add(
			"shared-buffer-node-serializer",
			SharedBufferNode.SharedBufferNodeSerializer.class,
			SharedBufferNode.SharedBufferNodeSerializer.SharedBufferNodeSerializerSnapshot.class,
			SharedBufferNode.SharedBufferNodeSerializer::new);
		testSpecifications.add(
			"nfa-state-serializer",
			NFAStateSerializer.class,
			NFAStateSerializerSnapshot.class,
			NFAStateSerializer::new);

		return testSpecifications.get();
	}
}
