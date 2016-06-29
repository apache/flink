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
package org.apache.flink.streaming.connectors.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.IterableIterator;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ResultPartitionWriter.class, CassandraTupleWriteAheadSink.class})
@PowerMockIgnore({"javax.management.*", "com.sun.jndi.*"})
public class CassandraConnectorUnitTest {
	@Test
	public void testAckLoopExitOnException() throws Exception {
		final AtomicReference<Runnable> callback = new AtomicReference<>();

		final ClusterBuilder clusterBuilder = new ClusterBuilder() {
			@Override
			protected Cluster buildCluster(Cluster.Builder builder) {
				try {
					BoundStatement boundStatement = mock(BoundStatement.class);
					when(boundStatement.setDefaultTimestamp(any(long.class))).thenReturn(boundStatement);

					PreparedStatement preparedStatement = mock(PreparedStatement.class);
					when(preparedStatement.bind(Matchers.anyVararg())).thenReturn(boundStatement);

					ResultSetFuture future = mock(ResultSetFuture.class);
					when(future.get()).thenThrow(new RuntimeException("Expected exception."));

					doAnswer(new Answer<Void>() {
						@Override
						public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
							callback.set((((Runnable) invocationOnMock.getArguments()[0])));
							return null;
						}
					}).when(future).addListener(any(Runnable.class), any(Executor.class));

					Session session = mock(Session.class);
					when(session.prepare(anyString())).thenReturn(preparedStatement);
					when(session.executeAsync(any(BoundStatement.class))).thenReturn(future);

					Cluster cluster = mock(Cluster.class);
					when(cluster.connect()).thenReturn(session);
					return cluster;
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		};

		final IterableIterator<Tuple0> iter = new IterableIterator<Tuple0>() {
			private boolean exhausted = false;

			@Override
			public boolean hasNext() {
				return !exhausted;
			}

			@Override
			public Tuple0 next() {
				exhausted = true;
				return new Tuple0();
			}

			@Override
			public void remove() {
			}

			@Override
			public Iterator<Tuple0> iterator() {
				return this;
			}
		};

		final AtomicReference<Boolean> exceptionCaught = new AtomicReference<>();

		Thread t = new Thread() {
			public void run() {
				try {
					CheckpointCommitter cc = mock(CheckpointCommitter.class);
					final CassandraTupleWriteAheadSink<Tuple0> sink = new CassandraTupleWriteAheadSink<>(
						"abc",
						TupleTypeInfo.of(Tuple0.class).createSerializer(new ExecutionConfig()),
						clusterBuilder,
						cc
					);

					OneInputStreamOperatorTestHarness<Tuple0, Tuple0> harness = new OneInputStreamOperatorTestHarness(sink);
					harness.getEnvironment().getTaskConfiguration().setBoolean("checkpointing", true);

					harness.setup();
					sink.open();
					boolean result = sink.sendValues(iter, 0L);
					sink.close();
					exceptionCaught.set(result == false);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		};
		t.start();

		int count = 0;
		while (t.getState() != Thread.State.WAITING && count < 100) { // 10 second timeout 10 * 10 * 100ms
			Thread.sleep(100);
			count++;
		}

		callback.get().run();

		t.join();

		Assert.assertTrue(exceptionCaught.get());
	}
}
