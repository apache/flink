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

package org.apache.flink.streaming.runtime.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.assertTrue;

public class StateBackendITCase extends StreamingMultipleProgramsTestBase {

	/**
	 * Verify that the user-specified state backend is used even if checkpointing is disabled.
	 *
	 * @throws Exception
	 */
	@Test
	public void testStateBackendWithoutCheckpointing() throws Exception {

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(1);

		see.setNumberOfExecutionRetries(0);
		see.setStateBackend(new FailingStateBackend());


		see.fromElements(new Tuple2<>("Hello", 1))
			.keyBy(0)
			.map(new RichMapFunction<Tuple2<String,Integer>, String>() {
				private static final long serialVersionUID = 1L;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					getRuntimeContext().getKeyValueState("test", String.class, "");
				}

				@Override
				public String map(Tuple2<String, Integer> value) throws Exception {
					return value.f0;
				}
			})
			.print();

		boolean caughtSuccess = false;
		try {
			see.execute();
		} catch (JobExecutionException e) {
			if (e.getCause() instanceof SuccessException) {
				caughtSuccess = true;
			} else {
				throw e;
			}
		}

		assertTrue(caughtSuccess);
	}


	public static class FailingStateBackend extends StateBackend<FailingStateBackend> {
		private static final long serialVersionUID = 1L;

		@Override
		public void initializeForJob(Environment env) throws Exception {
			throw new SuccessException();
		}

		@Override
		public void disposeAllStateForCurrentJob() throws Exception {

		}

		@Override
		public void close() throws Exception {

		}

		@Override
		public <K, V> KvState<K, V, FailingStateBackend> createKvState(String stateId,
			String stateName,
			TypeSerializer<K> keySerializer,
			TypeSerializer<V> valueSerializer,
			V defaultValue) throws Exception {
			return null;
		}

		@Override
		public CheckpointStateOutputStream createCheckpointStateOutputStream(long checkpointID,
			long timestamp) throws Exception {
			return null;
		}

		@Override
		public <S extends Serializable> StateHandle<S> checkpointStateSerializable(S state,
			long checkpointID,
			long timestamp) throws Exception {
			return null;
		}
	}

	static final class SuccessException extends Exception {
		private static final long serialVersionUID = -9218191172606739598L;
	}

}
