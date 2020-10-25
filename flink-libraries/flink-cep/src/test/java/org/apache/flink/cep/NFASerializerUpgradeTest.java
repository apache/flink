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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.cep.nfa.DeweyNumber;
import org.apache.flink.cep.nfa.NFAState;
import org.apache.flink.cep.nfa.NFAStateSerializer;
import org.apache.flink.cep.nfa.sharedbuffer.EventId;
import org.apache.flink.cep.nfa.sharedbuffer.NodeId;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferEdge;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferNode;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.is;

/**
 * Migration tests for NFA-related serializers.
 */
@RunWith(Parameterized.class)
public class NFASerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Object, Object> {

	public NFASerializerUpgradeTest(TestSpecification<Object, Object> testSpecification) {
		super(testSpecification);
	}

	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {
		ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
		for (MigrationVersion migrationVersion : MIGRATION_VERSIONS) {
			testSpecifications.add(
					new TestSpecification<>(
							"event-id-serializer",
							migrationVersion,
							EventIdSerializerSetup.class,
							EventIdSerializerVerifier.class));
			testSpecifications.add(
					new TestSpecification<>(
							"node-id-serializer",
							migrationVersion,
							NodeIdSerializerSetup.class,
							NodeIdSerializerVerifier.class));
			testSpecifications.add(
					new TestSpecification<>(
							"dewey-number-serializer",
							migrationVersion,
							DeweyNumberSerializerSetup.class,
							DeweyNumberSerializerVerifier.class));
			testSpecifications.add(
					new TestSpecification<>(
							"shared-buffer-edge-serializer",
							migrationVersion,
							SharedBufferEdgeSerializerSetup.class,
							SharedBufferEdgeSerializerVerifier.class));
			testSpecifications.add(
					new TestSpecification<>(
							"shared-buffer-node-serializer",
							migrationVersion,
							SharedBufferNodeSerializerSetup.class,
							SharedBufferNodeSerializerVerifier.class));
			testSpecifications.add(
					new TestSpecification<>(
							"nfa-state-serializer",
							migrationVersion,
							NFAStateSerializerSetup.class,
							NFAStateSerializerVerifier.class));
		}

		return testSpecifications;
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "event-id-serializer"
	// ----------------------------------------------------------------------------------------------

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class EventIdSerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<EventId> {

		@Override
		public TypeSerializer<EventId> createPriorSerializer() {
			return EventId.EventIdSerializer.INSTANCE;
		}

		@Override
		public EventId createTestData() {
			return new EventId(42, 42L);
		}
	}

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class EventIdSerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<EventId> {

		@Override
		public TypeSerializer<EventId> createUpgradedSerializer() {
			return EventId.EventIdSerializer.INSTANCE;
		}

		@Override
		public Matcher<EventId> testDataMatcher() {
			return is(new EventId(42, 42L));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<EventId>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "node-id-serializer"
	// ----------------------------------------------------------------------------------------------

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class NodeIdSerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<NodeId> {

		@Override
		public TypeSerializer<NodeId> createPriorSerializer() {
			return new NodeId.NodeIdSerializer();
		}

		@Override
		public NodeId createTestData() {
			return new NodeId(new EventId(42, 42L), "ciao");
		}
	}

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class NodeIdSerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<NodeId> {

		@Override
		public TypeSerializer<NodeId> createUpgradedSerializer() {
			return new NodeId.NodeIdSerializer();
		}

		@Override
		public Matcher<NodeId> testDataMatcher() {
			return is(new NodeId(new EventId(42, 42L), "ciao"));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<NodeId>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "dewey-number-serializer"
	// ----------------------------------------------------------------------------------------------

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class DeweyNumberSerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<DeweyNumber> {

		@Override
		public TypeSerializer<DeweyNumber> createPriorSerializer() {
			return DeweyNumber.DeweyNumberSerializer.INSTANCE;
		}

		@Override
		public DeweyNumber createTestData() {
			return new DeweyNumber(42);
		}
	}

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class DeweyNumberSerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<DeweyNumber> {

		@Override
		public TypeSerializer<DeweyNumber> createUpgradedSerializer() {
			return DeweyNumber.DeweyNumberSerializer.INSTANCE;
		}

		@Override
		public Matcher<DeweyNumber> testDataMatcher() {
			return is(new DeweyNumber(42));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<DeweyNumber>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "shared-buffer-edge-serializer"
	// ----------------------------------------------------------------------------------------------

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class SharedBufferEdgeSerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<SharedBufferEdge> {

		@Override
		public TypeSerializer<SharedBufferEdge> createPriorSerializer() {
			return new SharedBufferEdge.SharedBufferEdgeSerializer();
		}

		@Override
		public SharedBufferEdge createTestData() {
			return new SharedBufferEdge(
					new NodeId(new EventId(42, 42L), "page"),
					new DeweyNumber(42));
		}
	}

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class SharedBufferEdgeSerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<SharedBufferEdge> {

		@Override
		public TypeSerializer<SharedBufferEdge> createUpgradedSerializer() {
			return new SharedBufferEdge.SharedBufferEdgeSerializer();
		}

		@Override
		public Matcher<SharedBufferEdge> testDataMatcher() {
			return is(new SharedBufferEdge(
					new NodeId(new EventId(42, 42L), "page"),
					new DeweyNumber(42)));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<SharedBufferEdge>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "shared-buffer-node-serializer"
	// ----------------------------------------------------------------------------------------------

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class SharedBufferNodeSerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<SharedBufferNode> {

		@Override
		public TypeSerializer<SharedBufferNode> createPriorSerializer() {
			return new SharedBufferNode.SharedBufferNodeSerializer();
		}

		@Override
		public SharedBufferNode createTestData() {
			SharedBufferNode result = new SharedBufferNode();
			result.addEdge(new SharedBufferEdge(
					new NodeId(new EventId(42, 42L), "page"),
					new DeweyNumber(42)));
			return result;
		}
	}

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class SharedBufferNodeSerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<SharedBufferNode> {

		@Override
		public TypeSerializer<SharedBufferNode> createUpgradedSerializer() {
			return new SharedBufferNode.SharedBufferNodeSerializer();
		}

		@Override
		public Matcher<SharedBufferNode> testDataMatcher() {
			SharedBufferNode result = new SharedBufferNode();
			result.addEdge(new SharedBufferEdge(
					new NodeId(new EventId(42, 42L), "page"),
					new DeweyNumber(42)));
			return is(result);
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<SharedBufferNode>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "nfa-state-serializer"
	// ----------------------------------------------------------------------------------------------

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class NFAStateSerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<NFAState> {

		@Override
		public TypeSerializer<NFAState> createPriorSerializer() {
			return new NFAStateSerializer();
		}

		@Override
		public NFAState createTestData() {
			return new NFAState(Collections.emptyList());
		}
	}

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class NFAStateSerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<NFAState> {

		@Override
		public TypeSerializer<NFAState> createUpgradedSerializer() {
			return new NFAStateSerializer();
		}

		@Override
		public Matcher<NFAState> testDataMatcher() {
			return is(new NFAState(Collections.emptyList()));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<NFAState>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}
}
