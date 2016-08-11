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
package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ResultPartitionWriter.class)
@PowerMockIgnore("javax.management.*")
public abstract class WriteAheadSinkTestBase<IN, S extends GenericWriteAheadSink<IN>> extends TestLogger {

	protected abstract S createSink() throws Exception;

	protected abstract TypeInformation<IN> createTypeInfo();

	protected abstract IN generateValue(int counter, int checkpointID);

	protected abstract void verifyResultsIdealCircumstances(
		OneInputStreamOperatorTestHarness<IN, IN> harness, S sink) throws Exception;

	protected abstract void verifyResultsDataPersistenceUponMissedNotify(
			OneInputStreamOperatorTestHarness<IN, IN> harness, S sink) throws Exception;

	protected abstract void verifyResultsDataDiscardingUponRestore(
		OneInputStreamOperatorTestHarness<IN, IN> harness, S sink) throws Exception;

	@Test
	public void testIdealCircumstances() throws Exception {
		S sink = createSink();

		OneInputStreamOperatorTestHarness<IN, IN> testHarness =
				new OneInputStreamOperatorTestHarness<>(sink);

		testHarness.open();

		int elementCounter = 1;
		int snapshotCount = 0;

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 0)));
			elementCounter++;
		}

		testHarness.snapshot(snapshotCount++, 0);
		testHarness.notifyOfCompletedCheckpoint(snapshotCount - 1);

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 1)));
			elementCounter++;
		}

		testHarness.snapshot(snapshotCount++, 0);
		testHarness.notifyOfCompletedCheckpoint(snapshotCount - 1);

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 2)));
			elementCounter++;
		}

		testHarness.snapshot(snapshotCount++, 0);
		testHarness.notifyOfCompletedCheckpoint(snapshotCount - 1);

		verifyResultsIdealCircumstances(testHarness, sink);
	}

	@Test
	public void testDataPersistenceUponMissedNotify() throws Exception {
		S sink = createSink();

		OneInputStreamOperatorTestHarness<IN, IN> testHarness =
				new OneInputStreamOperatorTestHarness<>(sink);

		testHarness.open();

		int elementCounter = 1;
		int snapshotCount = 0;

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 0)));
			elementCounter++;
		}

		testHarness.snapshot(snapshotCount++, 0);
		testHarness.notifyOfCompletedCheckpoint(snapshotCount - 1);

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 1)));
			elementCounter++;
		}

		testHarness.snapshot(snapshotCount++, 0);

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 2)));
			elementCounter++;
		}

		testHarness.snapshot(snapshotCount++, 0);
		testHarness.notifyOfCompletedCheckpoint(snapshotCount - 1);

		verifyResultsDataPersistenceUponMissedNotify(testHarness, sink);
	}

	@Test
	public void testDataDiscardingUponRestore() throws Exception {
		S sink = createSink();

		OneInputStreamOperatorTestHarness<IN, IN> testHarness =
				new OneInputStreamOperatorTestHarness<>(sink);

		testHarness.open();

		int elementCounter = 1;
		int snapshotCount = 0;

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 0)));
			elementCounter++;
		}

		StreamStateHandle latestSnapshot = testHarness.snapshot(snapshotCount++, 0);
		testHarness.notifyOfCompletedCheckpoint(snapshotCount - 1);

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 1)));
			elementCounter++;
		}

		testHarness.close();

		sink = createSink();

		testHarness =new OneInputStreamOperatorTestHarness<>(sink);

		testHarness.setup();
		testHarness.restore(latestSnapshot);
		testHarness.open();

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 2)));
			elementCounter++;
		}

		testHarness.snapshot(snapshotCount++, 0);
		testHarness.notifyOfCompletedCheckpoint(snapshotCount - 1);

		verifyResultsDataDiscardingUponRestore(testHarness, sink);
	}
}
