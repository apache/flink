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

package org.apache.flink.connector.file.src;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerialization;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * Unit tests for the {@link FileSourceSplitSerializer}.
 */
public class PendingSplitsCheckpointSerializerTest {

	@Test
	public void serializeEmptyCheckpoint() throws Exception {
		final PendingSplitsCheckpoint checkpoint =
				PendingSplitsCheckpoint.fromCollectionSnapshot(Collections.emptyList());

		final PendingSplitsCheckpoint deSerialized = serializeAndDeserialize(checkpoint);

		assertCheckpointsEqual(checkpoint, deSerialized);
	}

	@Test
	public void serializeSomeSplits() throws Exception {
		final PendingSplitsCheckpoint checkpoint = PendingSplitsCheckpoint.fromCollectionSnapshot(
				Arrays.asList(testSplit1(), testSplit2(), testSplit3()));

		final PendingSplitsCheckpoint deSerialized = serializeAndDeserialize(checkpoint);

		assertCheckpointsEqual(checkpoint, deSerialized);
	}

	@Test
	public void serializeSplitsAndProcessedPaths() throws Exception {
		final PendingSplitsCheckpoint checkpoint = PendingSplitsCheckpoint.fromCollectionSnapshot(
				Arrays.asList(testSplit1(), testSplit2(), testSplit3()),
				Arrays.asList(new Path("file:/some/path"), new Path("s3://bucket/key/and/path"), new Path("hdfs://namenode:12345/path")));

		final PendingSplitsCheckpoint deSerialized = serializeAndDeserialize(checkpoint);

		assertCheckpointsEqual(checkpoint, deSerialized);
	}

	@Test
	public void repeatedSerialization() throws Exception {
		final PendingSplitsCheckpoint checkpoint = PendingSplitsCheckpoint.fromCollectionSnapshot(
			Arrays.asList(testSplit3(), testSplit1()));

		serializeAndDeserialize(checkpoint);
		serializeAndDeserialize(checkpoint);
		final PendingSplitsCheckpoint deSerialized = serializeAndDeserialize(checkpoint);

		assertCheckpointsEqual(checkpoint, deSerialized);
	}

	@Test
	public void repeatedSerializationCaches() throws Exception {
		final PendingSplitsCheckpoint checkpoint = PendingSplitsCheckpoint.fromCollectionSnapshot(
				Collections.singletonList(testSplit2()));

		final byte[] ser1 = PendingSplitsCheckpointSerializer.INSTANCE.serialize(checkpoint);
		final byte[] ser2 = PendingSplitsCheckpointSerializer.INSTANCE.serialize(checkpoint);

		assertSame(ser1, ser2);
	}

	// ------------------------------------------------------------------------
	//  test utils
	// ------------------------------------------------------------------------

	private static FileSourceSplit testSplit1() {
		return new FileSourceSplit(
				"random-id",
				new Path("hdfs://namenode:14565/some/path/to/a/file"),
				100_000_000,
				64_000_000,
				"host1", "host2", "host3");
	}

	private static FileSourceSplit testSplit2() {
		return new FileSourceSplit(
				"some-id",
				new Path("file:/some/path/to/a/file"),
				0,
				0);
	}

	private static FileSourceSplit testSplit3() {
		return new FileSourceSplit(
				"an-id",
				new Path("s3://some-bucket/key/to/the/object"),
				0,
				1234567);
	}

	private static PendingSplitsCheckpoint serializeAndDeserialize(PendingSplitsCheckpoint split) throws IOException {
		final PendingSplitsCheckpointSerializer serializer = new PendingSplitsCheckpointSerializer();
		final byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(serializer, split);
		return SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);
	}

	private static void assertCheckpointsEqual(PendingSplitsCheckpoint expected, PendingSplitsCheckpoint actual) {
		assertOrderedCollectionEquals(expected.getSplits(), actual.getSplits(), FileSourceSplitSerializerTest::assertSplitsEqual);

		assertOrderedCollectionEquals(
				expected.getAlreadyProcessedPaths(), actual.getAlreadyProcessedPaths(), Assert::assertEquals);
	}

	private static <E> void assertOrderedCollectionEquals(
			Collection<E> expected,
			Collection<E> actual,
			BiConsumer<E, E> equalityAsserter) {

		assertEquals(expected.size(), actual.size());
		final Iterator<E> expectedIter = expected.iterator();
		final Iterator<E> actualIter = actual.iterator();
		while (expectedIter.hasNext()) {
			equalityAsserter.accept(expectedIter.next(), actualIter.next());
		}
	}
}
