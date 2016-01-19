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

package org.apache.flink.test.checkpointing;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/**
 * An integration test that runs an iterative streaming topology with checkpointing enabled.
 * <p/>
 * The test triggers a failure after a while and verifies "exactly-once-processing" guarantees.
 * 
 * The correctness of the final state relies on whether all records in transit through the iteration cycle
 * have been processed exactly once by the stateful operator that consumes the feedback stream.
 * 
 */
@SuppressWarnings("serial")
public class StreamIterationCheckpointingITCase extends StreamFaultToleranceTestBase {

	static volatile long FINISH_POINT = Long.MAX_VALUE; 
	
	@Override
	public void testProgram(StreamExecutionEnvironment env) {
		env.getCheckpointConfig().setCheckpointInterval(1000);
		DataStream<ADD> stream = env.addSource(new AddGenerator());
		IterativeStream.ConnectedIterativeStreams<ADD, SUBTRACT> iter = stream.iterate(2000).withFeedbackType(SUBTRACT.class);
		SplitStream<Tuple2<String, Long>> step = iter.flatMap(new LoopCounter()).disableChaining().split(new OutputSelector<Tuple2<String, Long>>() {
			@Override
			public Iterable<String> select(Tuple2<String, Long> value) {
				return Lists.newArrayList(value.f0);
			}
		});
		iter.closeWith(step.select(LoopCounter.FEEDBACK).map(new MapFunction<Tuple2<String, Long>, SUBTRACT>() {
			@Override
			public SUBTRACT map(Tuple2 value) throws Exception {
				return new SUBTRACT(1);
			}
		}));
		step.select(LoopCounter.FORWARD).map(new MapFunction<Tuple2<String, Long>, Long>() {
			@Override
			public Long map(Tuple2<String, Long> value) throws Exception {
				return value.f1;
			}
		}).disableChaining().addSink(new OnceFailingSink());

	}

	@Override
	public void postSubmit() {
		Long[] counters = new Long[PARALLELISM];
		Long[] states = new Long[PARALLELISM];

		Arrays.fill(counters, FINISH_POINT);
		Arrays.fill(states, FINISH_POINT);

		assertTrue(OnceFailingSink.hasFailed);
		assertArrayEquals(counters, AddGenerator.counts);
		assertArrayEquals(states, LoopCounter.stateVals);
		assertArrayEquals(counters, OnceFailingSink.stateVals);

	}

	// --------------------------------------------------------------------------------------------
	//  Custom Functions
	// --------------------------------------------------------------------------------------------


	private static class AddGenerator extends RichSourceFunction<ADD>
		implements ParallelSourceFunction<ADD>, Checkpointed<Long> {

		private volatile boolean isRunning = true;
		static final Long[] counts = new Long[PARALLELISM];
		private int index;
		private long state;
		

		@Override
		public void open(Configuration parameters) throws IOException {
			index = getRuntimeContext().getIndexOfThisSubtask();
		}

		@Override
		public void close() throws Exception {
			counts[index] = state;
		}

		@Override
		public void run(SourceContext<ADD> ctx) throws Exception {
			final Object lockingObject = ctx.getCheckpointLock();

			while (isRunning && state < FINISH_POINT) {
				synchronized (lockingObject) {
					counts[index] = ++state;
					ctx.collect(new ADD(2));
					Thread.sleep(2);
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public Long snapshotState(long checkpointId, long checkpointTimestamp) {
			return state;
		}

		@Override
		public void restoreState(Long state) {
			this.state = state;
		}
	}


	private static class LoopCounter extends RichCoFlatMapFunction<ADD, SUBTRACT, Tuple2<String, Long>> implements Checkpointed<Long> {

		public static final String FEEDBACK = "FEEDBACK";
		public static final String FORWARD = "FORWARD";
		static final Long[] stateVals = new Long[PARALLELISM];
		private long state;
		private int index;

		@Override
		public void open(Configuration parameters) throws Exception {
			this.index = getRuntimeContext().getIndexOfThisSubtask();
		}

		@Override
		public void close() throws Exception {
			stateVals[index] = state;
		}

		@Override
		public void flatMap1(ADD toAdd, Collector<Tuple2<String, Long>> out) throws Exception {
			state += toAdd.val;
			out.collect(new Tuple2<>(FEEDBACK, state));
		}

		@Override
		public void flatMap2(SUBTRACT toSubtract, Collector<Tuple2<String, Long>> out) throws Exception {
			state -= toSubtract.val;
			out.collect(new Tuple2<>(FORWARD, state));
		}

		@Override
		public Long snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return state;
		}

		@Override
		public void restoreState(Long recState) throws Exception {
			state = recState;
		}
	}
	
	private static class OnceFailingSink extends RichSinkFunction<Long>
		implements Checkpointed<Long>, CheckpointListener {

		static final Long[] stateVals = new Long[PARALLELISM];
		private long state;
		private static volatile boolean hasFailed = false;
		private int index;
		private boolean failNow = false;

		@Override
		public void open(Configuration parameters) throws IOException {
			index = getRuntimeContext().getIndexOfThisSubtask();
		}

		@Override
		public void close() throws Exception {
			stateVals[index] = state;
		}

		@Override
		public Long snapshotState(long checkpointId, long checkpointTimestamp) {
			return state;
		}

		@Override
		public void restoreState(Long toRestore) {
			state = toRestore;
		}

		@Override
		public void invoke(Long value) throws Exception {
			state++;
			if(failNow){
				long max = Collections.max(Arrays.asList(AddGenerator.counts));
				FINISH_POINT = (long) (max * 1.5);
				hasFailed = true;
				throw new Exception("Intended Exception");
			}
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			synchronized (OnceFailingSink.class){
				if(!hasFailed){
					failNow = true;
					hasFailed = true;
				}	
			}
		}
	}

}

class ADD implements Serializable {
	public long val;

	public ADD() {
		this(0);
	}

	public void setVal(long val) {
		
		this.val = val;
	}

	public ADD(int val) {
		this.val = val;
	}
}

class SUBTRACT implements Serializable {
	public long val;

	public SUBTRACT() {
		this(0);
	}

	public void setVal(long val) {
		this.val = val;
	}

	public SUBTRACT(int val) {
		this.val = val;
	}
}

