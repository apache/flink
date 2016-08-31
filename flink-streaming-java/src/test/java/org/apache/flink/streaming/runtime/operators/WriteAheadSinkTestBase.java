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
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ResultPartitionWriter.class)
public abstract class WriteAheadSinkTestBase<IN, S extends GenericWriteAheadSink<IN>> extends TestLogger {

	protected static class OperatorExposingTask<INT> extends OneInputStreamTask<INT, INT> {
		public OneInputStreamOperator<INT, INT> getOperator() {
			return this.headOperator;
		}
	}

	protected OperatorExposingTask<IN> createTask() {
		return new OperatorExposingTask<>();
	}

	protected abstract S createSink() throws Exception;

	protected abstract TypeInformation<IN> createTypeInfo();

	protected abstract IN generateValue(int counter, int checkpointID);

	protected abstract void verifyResultsIdealCircumstances(
		OneInputStreamTaskTestHarness<IN, IN> harness, OneInputStreamTask<IN, IN> task, S sink) throws Exception;

	protected abstract void verifyResultsDataPersistenceUponMissedNotify(
		OneInputStreamTaskTestHarness<IN, IN> harness, OneInputStreamTask<IN, IN> task, S sink) throws Exception;

	protected abstract void verifyResultsDataDiscardingUponRestore(
		OneInputStreamTaskTestHarness<IN, IN> harness, OneInputStreamTask<IN, IN> task, S sink) throws Exception;

	@Test
	public void testIdealCircumstances() throws Exception {
		OperatorExposingTask<IN> task = createTask();
		TypeInformation<IN> info = createTypeInfo();
		OneInputStreamTaskTestHarness<IN, IN> testHarness = new OneInputStreamTaskTestHarness<>(task, 1, 1, info, info);
		StreamConfig streamConfig = testHarness.getStreamConfig();
		streamConfig.setCheckpointingEnabled(true);
		streamConfig.setStreamOperator(createSink());

		int elementCounter = 1;

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		ArrayList<StreamTaskState> states = new ArrayList<>();

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 0)));
			elementCounter++;
		}
		testHarness.waitForInputProcessing();

		states.add(copyTaskState(task.getOperator().snapshotOperatorState(states.size(), 0)));
		task.notifyCheckpointComplete(states.size() - 1);

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 1)));
			elementCounter++;
		}
		testHarness.waitForInputProcessing();

		states.add(copyTaskState(task.getOperator().snapshotOperatorState(states.size(), 0)));
		task.notifyCheckpointComplete(states.size() - 1);

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 2)));
			elementCounter++;
		}
		testHarness.waitForInputProcessing();

		states.add(copyTaskState(task.getOperator().snapshotOperatorState(states.size(), 0)));
		task.notifyCheckpointComplete(states.size() - 1);

		testHarness.endInput();

		states.add(copyTaskState(task.getOperator().snapshotOperatorState(states.size(), 0)));
		testHarness.waitForTaskCompletion();

		verifyResultsIdealCircumstances(testHarness, task, (S) task.getOperator());
	}

	@Test
	public void testDataPersistenceUponMissedNotify() throws Exception {
		S sink = createSink();
		OperatorExposingTask<IN> task = createTask();
		TypeInformation<IN> info = createTypeInfo();
		OneInputStreamTaskTestHarness<IN, IN> testHarness = new OneInputStreamTaskTestHarness<>(task, 1, 1, info, info);
		StreamConfig streamConfig = testHarness.getStreamConfig();
		streamConfig.setCheckpointingEnabled(true);
		streamConfig.setStreamOperator(sink);

		int elementCounter = 1;

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		ArrayList<StreamTaskState> states = new ArrayList<>();

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 0)));
			elementCounter++;
		}
		testHarness.waitForInputProcessing();
		states.add(copyTaskState(task.getOperator().snapshotOperatorState(states.size(), 0)));
		task.notifyCheckpointComplete(states.size() - 1);

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 1)));
			elementCounter++;
		}
		testHarness.waitForInputProcessing();
		states.add(copyTaskState(task.getOperator().snapshotOperatorState(states.size(), 0)));

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 2)));
			elementCounter++;
		}
		testHarness.waitForInputProcessing();
		states.add(copyTaskState(task.getOperator().snapshotOperatorState(states.size(), 0)));
		task.notifyCheckpointComplete(states.size() - 1);

		testHarness.endInput();

		states.add(copyTaskState(task.getOperator().snapshotOperatorState(states.size(), 0)));
		testHarness.waitForTaskCompletion();

		verifyResultsDataPersistenceUponMissedNotify(testHarness, task, (S) task.getOperator());
	}

	@Test
	public void testDataDiscardingUponRestore() throws Exception {
		S sink = createSink();
		OperatorExposingTask<IN> task = createTask();
		TypeInformation<IN> info = createTypeInfo();
		OneInputStreamTaskTestHarness<IN, IN> testHarness = new OneInputStreamTaskTestHarness<>(task, 1, 1, info, info);
		StreamConfig streamConfig = testHarness.getStreamConfig();
		streamConfig.setCheckpointingEnabled(true);
		streamConfig.setStreamOperator(sink);

		int elementCounter = 1;

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		ArrayList<StreamTaskState> states = new ArrayList<>();

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 0)));
			elementCounter++;
		}
		testHarness.waitForInputProcessing();
		states.add(copyTaskState(task.getOperator().snapshotOperatorState(states.size(), 0)));
		task.notifyCheckpointComplete(states.size() - 1);

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 1)));
			elementCounter++;
		}
		testHarness.waitForInputProcessing();
		
		task.getOperator().close();
		task.getOperator().open();

		task.getOperator().restoreState(states.get(states.size() - 1));

		for (int x = 0; x < 20; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 2)));
			elementCounter++;
		}
		testHarness.waitForInputProcessing();
		states.add(copyTaskState(task.getOperator().snapshotOperatorState(states.size(), 0)));
		task.notifyCheckpointComplete(states.size() - 1);

		testHarness.endInput();

		states.add(copyTaskState(task.getOperator().snapshotOperatorState(states.size(), 0)));
		testHarness.waitForTaskCompletion();

		verifyResultsDataDiscardingUponRestore(testHarness, task, (S) task.getOperator());
	}

	protected StreamTaskState copyTaskState(StreamTaskState toCopy) throws IOException, ClassNotFoundException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(toCopy);

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		ObjectInputStream ois = new ObjectInputStream(bais);
		return (StreamTaskState) ois.readObject();
	}
}
