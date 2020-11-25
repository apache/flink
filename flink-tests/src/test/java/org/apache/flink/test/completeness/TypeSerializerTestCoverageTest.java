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

package org.apache.flink.test.completeness;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.SingleThreadAccessCheckingTypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.common.typeutils.base.EnumSerializer;
import org.apache.flink.api.common.typeutils.base.GenericArraySerializer;
import org.apache.flink.api.common.typeutils.base.InstantSerializer;
import org.apache.flink.api.common.typeutils.base.LocalDateSerializer;
import org.apache.flink.api.common.typeutils.base.LocalDateTimeSerializer;
import org.apache.flink.api.common.typeutils.base.LocalTimeSerializer;
import org.apache.flink.api.common.typeutils.base.NullValueSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.common.typeutils.base.array.BooleanPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.CharPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.DoublePrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.FloatPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.IntPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.LongPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.ShortPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.StringArraySerializer;
import org.apache.flink.api.java.typeutils.runtime.CopyableValueSerializer;
import org.apache.flink.api.java.typeutils.runtime.EitherSerializer;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.api.java.typeutils.runtime.Tuple0Serializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.java.typeutils.runtime.ValueSerializer;
import org.apache.flink.api.java.typeutils.runtime.WritableSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.api.scala.typeutils.EnumValueSerializer;
import org.apache.flink.api.scala.typeutils.NothingSerializer;
import org.apache.flink.api.scala.typeutils.OptionSerializer;
import org.apache.flink.api.scala.typeutils.TraversableSerializer;
import org.apache.flink.api.scala.typeutils.TrySerializer;
import org.apache.flink.api.scala.typeutils.UnitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.heap.TestDuplicateSerializer;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.operators.InternalTimersSnapshotReaderWriters;
import org.apache.flink.streaming.api.operators.co.IntervalJoinOperator;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.reflections.Reflections;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

/**
 * Scans the class path for type serializer and checks if there is a test for it.
 */
public class TypeSerializerTestCoverageTest extends TestLogger {

	@Test
	public void testTypeSerializerTestCoverage() {
		final Reflections reflections = new Reflections("org.apache.flink");

		final Set<Class<? extends TypeSerializer>> typeSerializers = reflections.getSubTypesOf(TypeSerializer.class);

		final Set<String> typeSerializerTestNames = reflections.getSubTypesOf(SerializerTestBase.class)
				.stream().map(Class::getName).collect(Collectors.toSet());
		final Set<String> typeSerializerUpgradeTestNames = reflections.getSubTypesOf(TypeSerializerUpgradeTestBase.class)
				.stream().map(Class::getName).collect(Collectors.toSet());
		final Set<String> typeSerializerUpgradeSetupNames = reflections.getSubTypesOf(TypeSerializerUpgradeTestBase.PreUpgradeSetup.class)
				.stream().map(Class::getSimpleName).collect(Collectors.toSet());

		// type serializer whitelist for SerializerTestBase test coverage
		final List<String> serializerTestBaseWhitelist = Arrays.asList(
				"org.apache.flink.streaming.api.operators.sort.KeyAndValueSerializer",
				ArrayListSerializer.class.getName(),
				EitherSerializer.class.getName(),
				SingleThreadAccessCheckingTypeSerializer.class.getName(),
				GenericArraySerializer.class.getName(),
				NullValueSerializer.class.getName(),
				Tuple0Serializer.class.getName(),
				CopyableValueSerializer.class.getName(),
				VoidSerializer.class.getName(),
				ValueSerializer.class.getName(),
				RowSerializer.class.getName(),
				StreamElementSerializer.class.getName(),
				WritableSerializer.class.getName(),
				KryoSerializer.class.getName(),
				UnloadableDummyTypeSerializer.class.getName(),
				TupleSerializer.class.getName(),
				EnumSerializer.class.getName(),
				CoGroupedStreams.UnionSerializer.class.getName(),
				TtlStateFactory.TtlSerializer.class.getName(),
				TimeWindow.Serializer.class.getName(),
				InternalTimersSnapshotReaderWriters.LegacyTimerSerializer.class.getName(),
				TwoPhaseCommitSinkFunction.StateSerializer.class.getName(),
				IntervalJoinOperator.BufferEntrySerializer.class.getName(),
				GlobalWindow.Serializer.class.getName(),
				org.apache.flink.queryablestate.client.VoidNamespaceSerializer.class.getName(),
				org.apache.flink.runtime.state.VoidNamespaceSerializer.class.getName(),
				EnumValueSerializer.class.getName(),
				OptionSerializer.class.getName(),
				TraversableSerializer.class.getName(),
				UnitSerializer.class.getName(),
				NothingSerializer.class.getName(),
				TrySerializer.class.getName(),
				"org.apache.flink.api.scala.package$Tuple2CaseClassSerializer",
				TestDuplicateSerializer.class.getName()
		);

		//  type serializer whitelist for TypeSerializerUpgradeTestBase test coverage
		final List<String> typeSerializerUpgradeTestBaseWhitelist = Arrays.asList(
				"org.apache.flink.streaming.api.operators.sort.KeyAndValueSerializer",
				InstantSerializer.class.getName(),
				SingleThreadAccessCheckingTypeSerializer.class.getName(),
				SimpleVersionedSerializerTypeSerializerProxy.class.getName(),
				Tuple0Serializer.class.getName(),
				CopyableValueSerializer.class.getName(),
				VoidSerializer.class.getName(),
				StringArraySerializer.class.getName(),
				LocalDateTimeSerializer.class.getName(),
				BooleanPrimitiveArraySerializer.class.getName(),
				BytePrimitiveArraySerializer.class.getName(),
				CharPrimitiveArraySerializer.class.getName(),
				DoublePrimitiveArraySerializer.class.getName(),
				FloatPrimitiveArraySerializer.class.getName(),
				IntPrimitiveArraySerializer.class.getName(),
				LongPrimitiveArraySerializer.class.getName(),
				ShortPrimitiveArraySerializer.class.getName(),
				LocalDateSerializer.class.getName(),
				LocalTimeSerializer.class.getName(),
				UnloadableDummyTypeSerializer.class.getName(),
				TimeWindow.Serializer.class.getName(),
				CoGroupedStreams.UnionSerializer.class.getName(),
				InternalTimersSnapshotReaderWriters.LegacyTimerSerializer.class.getName(),
				TwoPhaseCommitSinkFunction.StateSerializer.class.getName(),
				GlobalWindow.Serializer.class.getName(),
				UnitSerializer.class.getName(),
				NothingSerializer.class.getName(),
				TrySerializer.class.getName(),
				"org.apache.flink.api.scala.package$Tuple2CaseClassSerializer",
				TestDuplicateSerializer.class.getName()
		);

		// check if a test exists for each type serializer
		for (Class<? extends TypeSerializer> typeSerializer : typeSerializers) {
			// we skip abstract classes, test classes, inner classes and scala classes to skip type serializer defined in test classes
			if (Modifier.isAbstract(typeSerializer.getModifiers()) ||
					Modifier.isPrivate(typeSerializer.getModifiers()) ||
				typeSerializer.getName().contains("Test$") ||
				typeSerializer.getName().contains("TestBase$") ||
				typeSerializer.getName().contains("TestType$") ||
				typeSerializer.getName().contains("testutils") ||
				typeSerializer.getName().contains("ITCase$") ||
				typeSerializer.getName().contains("$$anon") ||
				typeSerializer.getName().contains("queryablestate")) {
				continue;
			}

			final String testToFind = typeSerializer.getName() + "Test";
			if (!typeSerializerTestNames.contains(testToFind) &&
					!serializerTestBaseWhitelist.contains(typeSerializer.getName())) {
				fail("Could not find test '" + testToFind + "' that covers '" + typeSerializer.getName() + "'.");
			}

			final String upgradeTestToFind = typeSerializer.getName() + "UpgradeTest";
			final String upgradeSetupToFind = typeSerializer.getSimpleName() + "Setup";
			if (!typeSerializerUpgradeTestNames.contains(upgradeTestToFind) &&
					!typeSerializerUpgradeSetupNames.contains(upgradeSetupToFind) &&
					!typeSerializerUpgradeTestBaseWhitelist.contains(typeSerializer.getName())) {
				fail("Could not find upgrade test '" + upgradeTestToFind + "' or setup '" + upgradeSetupToFind + "' that covers '" + typeSerializer.getName() + "'.");
			}
		}
	}
}
