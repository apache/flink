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
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;

import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.fail;

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
		
		try {
			see.execute();
			fail();
		}
		catch (JobExecutionException e) {
			Throwable t = e.getCause();
			if (!(t != null && t.getCause() instanceof SuccessException)) {
				throw e;
			}
		}
	}


	public static class FailingStateBackend extends AbstractStateBackend {
		
		private static final long serialVersionUID = 1L;

		@Override
		public void initializeForJob(Environment env, String operatorIdentifier, TypeSerializer<?> keySerializer) throws Exception {
			throw new SuccessException();
		}

		@Override
		public void disposeAllStateForCurrentJob() throws Exception {}

		@Override
		public void close() throws Exception {}

		@Override
		protected <N, T> ValueState<T> createValueState(TypeSerializer<N> namespaceSerializer, ValueStateDescriptor<T> stateDesc) throws Exception {
			return null;
		}

		@Override
		protected <N, T> ListState<T> createListState(TypeSerializer<N> namespaceSerializer, ListStateDescriptor<T> stateDesc) throws Exception {
			return null;
		}

		@Override
		protected <N, T> ReducingState<T> createReducingState(TypeSerializer<N> namespaceSerializer, ReducingStateDescriptor<T> stateDesc) throws Exception {
			return null;
		}

		@Override
		protected <N, T, ACC> FoldingState<T, ACC> createFoldingState(TypeSerializer<N> namespaceSerializer,
			FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {
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
