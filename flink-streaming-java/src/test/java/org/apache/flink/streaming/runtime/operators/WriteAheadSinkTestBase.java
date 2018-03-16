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
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

/**
 * Test base for {@link GenericWriteAheadSink}.
 */
public abstract class WriteAheadSinkTestBase<IN, S extends GenericWriteAheadSink<IN>> extends TestLogger {

	protected abstract S createSink() throws Exception;

	protected abstract TypeInformation<IN> createTypeInfo();

	protected abstract IN generateValue(int counter, int checkpointID);

	protected abstract void verifyResultsIdealCircumstances(S sink) throws Exception;

	protected abstract void verifyResultsDataPersistenceUponMissedNotify(S sink) throws Exception;

	protected abstract void verifyResultsDataDiscardingUponRestore(S sink) throws Exception;

	protected abstract void verifyResultsWhenReScaling(S sink, int startElementCounter, int endElementCounter) throws Exception;

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

		verifyResultsIdealCircumstances(sink);
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

		verifyResultsDataPersistenceUponMissedNotify(sink);
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

		OperatorSubtaskState latestSnapshot = testHarness.snapshot(snapshotCount++, 0);
		testHarness.notifyOfCompletedCheckpoint(snapshotCount - 1);

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 1)));
			elementCounter++;
		}

		testHarness.close();

		sink = createSink();

		testHarness = new OneInputStreamOperatorTestHarness<>(sink);

		testHarness.setup();
		testHarness.initializeState(latestSnapshot);
		testHarness.open();

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 2)));
			elementCounter++;
		}

		testHarness.snapshot(snapshotCount++, 0);
		testHarness.notifyOfCompletedCheckpoint(snapshotCount - 1);

		verifyResultsDataDiscardingUponRestore(sink);
	}

	@Test
	public void testScalingDown() throws Exception {
		S sink1 = createSink();
		OneInputStreamOperatorTestHarness<IN, IN> testHarness1 =
			new OneInputStreamOperatorTestHarness<>(sink1, 10, 2, 0);
		testHarness1.open();

		S sink2 = createSink();
		OneInputStreamOperatorTestHarness<IN, IN> testHarness2 =
			new OneInputStreamOperatorTestHarness<>(sink2, 10, 2, 1);
		testHarness2.open();

		int elementCounter = 1;
		int snapshotCount = 0;

		for (int x = 0; x < 10; x++) {
			testHarness1.processElement(new StreamRecord<>(generateValue(elementCounter, 0)));
			elementCounter++;
		}

		for (int x = 0; x < 11; x++) {
			testHarness2.processElement(new StreamRecord<>(generateValue(elementCounter, 0)));
			elementCounter++;
		}

		// snapshot at checkpoint 0 for testHarness1 and testHarness 2
		OperatorSubtaskState snapshot1 = testHarness1.snapshot(snapshotCount, 0);
		OperatorSubtaskState snapshot2 = testHarness2.snapshot(snapshotCount, 0);

		// merge the two partial states
		OperatorSubtaskState mergedSnapshot = AbstractStreamOperatorTestHarness
			.repackageState(snapshot1, snapshot2);

		testHarness1.close();
		testHarness2.close();

		// and create a third instance that operates alone but
		// has the merged state of the previous 2 instances

		S sink3 = createSink();
		OneInputStreamOperatorTestHarness<IN, IN> mergedTestHarness =
			new OneInputStreamOperatorTestHarness<>(sink3, 10, 1, 0);

		mergedTestHarness.setup();
		mergedTestHarness.initializeState(mergedSnapshot);
		mergedTestHarness.open();

		for (int x = 0; x < 12; x++) {
			mergedTestHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 0)));
			elementCounter++;
		}

		snapshotCount++;
		mergedTestHarness.snapshot(snapshotCount, 1);
		mergedTestHarness.notifyOfCompletedCheckpoint(snapshotCount);

		verifyResultsWhenReScaling(sink3, 1, 33);
		mergedTestHarness.close();
	}

	@Test
	public void testScalingUp() throws Exception {

		S sink1 = createSink();
		OneInputStreamOperatorTestHarness<IN, IN> testHarness1 =
			new OneInputStreamOperatorTestHarness<>(sink1, 10, 1, 0);

		int elementCounter = 1;
		int snapshotCount = 0;

		testHarness1.open();

		// put two more checkpoints as pending

		for (int x = 0; x < 10; x++) {
			testHarness1.processElement(new StreamRecord<>(generateValue(elementCounter, 0)));
			elementCounter++;
		}
		testHarness1.snapshot(++snapshotCount, 0);

		for (int x = 0; x < 11; x++) {
			testHarness1.processElement(new StreamRecord<>(generateValue(elementCounter, 0)));
			elementCounter++;
		}

		// this will be the state that will be split between the two new operators
		OperatorSubtaskState snapshot = testHarness1.snapshot(++snapshotCount, 0);

		testHarness1.close();

		// verify no elements are in the sink
		verifyResultsWhenReScaling(sink1, 0, -1);

		// we will create two operator instances, testHarness2 and testHarness3,
		// that will share the state of testHarness1

		++snapshotCount;

		S sink2 = createSink();
		OneInputStreamOperatorTestHarness<IN, IN> testHarness2 =
			new OneInputStreamOperatorTestHarness<>(sink2, 10, 2, 0);

		testHarness2.setup();
		testHarness2.initializeState(snapshot);
		testHarness2.open();

		testHarness2.notifyOfCompletedCheckpoint(snapshotCount);

		verifyResultsWhenReScaling(sink2, 1, 10);

		S sink3 = createSink();
		OneInputStreamOperatorTestHarness<IN, IN> testHarness3 =
			new OneInputStreamOperatorTestHarness<>(sink3, 10, 2, 1);

		testHarness3.setup();
		testHarness3.initializeState(snapshot);
		testHarness3.open();

		// add some more elements to verify that everything functions normally from now on...

		for (int x = 0; x < 10; x++) {
			testHarness3.processElement(new StreamRecord<>(generateValue(elementCounter, 0)));
			elementCounter++;
		}

		testHarness3.snapshot(snapshotCount, 1);
		testHarness3.notifyOfCompletedCheckpoint(snapshotCount);

		verifyResultsWhenReScaling(sink3, 11, 31);

		testHarness2.close();
		testHarness3.close();
	}
}
