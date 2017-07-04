/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link TwoPhaseCommitSinkFunction}.
 */
public class TwoPhaseCommitSinkFunctionTests {
	@Test
	public void testFailBeforeNotify() throws Exception {
		File tmpDirectory = Files.createTempDirectory(this.getClass().getSimpleName() + "_tmp").toFile();
		tmpDirectory.deleteOnExit();
		File targetDirectory = Files.createTempDirectory(this.getClass().getSimpleName() + "_target").toFile();
		targetDirectory.deleteOnExit();

		FileBasedSinkFunction sinkFunction = new FileBasedSinkFunction(tmpDirectory, targetDirectory);

		MemoryStateBackend backend = new MemoryStateBackend();
		StateInitializationContextImpl initializationContext = getInitializationContext(
			"failBeforeNotify",
			backend,
			true);

		sinkFunction.setRuntimeContext(getRuntimeContext());
		sinkFunction.open(new Configuration());
		sinkFunction.initializeState(initializationContext);
		sinkFunction.invoke("42");
		sinkFunction.snapshotState(getSnapshotContext(0));
		sinkFunction.invoke("43");
		sinkFunction.snapshotState(getSnapshotContext(1));

		assertTrue(tmpDirectory.setWritable(false));
		try {
			sinkFunction.invoke("44");
			sinkFunction.snapshotState(getSnapshotContext(2));
		}
		catch (FileNotFoundException ex) {
			// ignore
		}
		sinkFunction.close();

		assertTrue(tmpDirectory.setWritable(true));

		sinkFunction = new FileBasedSinkFunction(tmpDirectory, targetDirectory);
		sinkFunction.setRuntimeContext(getRuntimeContext());
		sinkFunction.open(new Configuration());
		sinkFunction.initializeState(initializationContext);
		sinkFunction.close();

		assertExactlyOnceForDirectory(targetDirectory, Arrays.asList("42", "43"));
		assertEquals(0, tmpDirectory.listFiles().length);
	}

	private StateInitializationContextImpl getInitializationContext(String operatorIdentifier, MemoryStateBackend backend, boolean restored) throws Exception {
		Environment env = new DummyEnvironment(operatorIdentifier, 1, 0);
		return new StateInitializationContextImpl(
			restored,
			backend.createOperatorStateBackend(env, operatorIdentifier),
			mock(KeyedStateStore.class),
			new ArrayList<KeyedStateHandle>(),
			new ArrayList<OperatorStateHandle>(),
			new CloseableRegistry());
	}

	private RuntimeContext getRuntimeContext() {
		StreamingRuntimeContext mockRuntimeContext = mock(StreamingRuntimeContext.class);
		when(mockRuntimeContext.isCheckpointingEnabled()).thenReturn(true); // enable checkpointing
		when(mockRuntimeContext.getNumberOfParallelSubtasks()).thenReturn(1);
		return mockRuntimeContext;
	}

	private FunctionSnapshotContext getSnapshotContext(final int checkpointId) {
		return new FunctionSnapshotContext() {
			@Override
			public long getCheckpointId() {
				return checkpointId;
			}

			@Override
			public long getCheckpointTimestamp() {
				return checkpointId;
			}
		};
	}

	private void assertExactlyOnceForDirectory(File targetDirectory, List<String> expectedValues) throws IOException {
		ArrayList<String> actualValues = new ArrayList<>();
		for (File file : targetDirectory.listFiles()) {
			actualValues.addAll(Files.readAllLines(file.toPath(), Charset.defaultCharset()));
		}
		Collections.sort(actualValues);
		Collections.sort(expectedValues);
		assertEquals(expectedValues, actualValues);
	}

	private static class FileBasedSinkFunction extends TwoPhaseCommitSinkFunction<String, FileTransaction> {
		private final File tmpDirectory;
		private final File targetDirectory;

		public FileBasedSinkFunction(File tmpDirectory, File targetDirectory) {
			super(FileTransaction.class);

			if (!tmpDirectory.isDirectory() || !targetDirectory.isDirectory()) {
				throw new IllegalArgumentException();
			}

			this.tmpDirectory = tmpDirectory;
			this.targetDirectory = targetDirectory;
		}

		@Override
		protected void invoke(FileTransaction transaction, String value) throws Exception {
			transaction.writer.write(value);
		}

		@Override
		protected FileTransaction beginTransaction() throws Exception {
			File tmpFile = new File(tmpDirectory, UUID.randomUUID().toString());
			return new FileTransaction(tmpFile);
		}

		@Override
		protected void preCommit(FileTransaction transaction) throws Exception {
			transaction.writer.flush();
			transaction.writer.close();
		}

		@Override
		protected void commit(FileTransaction transaction) {
			try {
				Files.move(transaction.tmpFile.toPath(), new File(targetDirectory, transaction.tmpFile.getName()).toPath(), ATOMIC_MOVE);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}

		@Override
		protected void abort(FileTransaction transaction) {
			try {
				transaction.writer.close();
			} catch (IOException e) {
				// ignore
			}
			transaction.tmpFile.delete();
		}
	}

	private static class FileTransaction implements Serializable {
		private final File tmpFile;
		private final transient BufferedWriter writer;

		public FileTransaction(File tmpFile) throws IOException {
			this.tmpFile = tmpFile;
			this.writer = new BufferedWriter(new FileWriter(tmpFile));
		}
	}
}
