/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.api.streamcomponent;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.faulttolerance.FaultToleranceType;
import eu.stratosphere.streaming.util.ClusterUtil;
import eu.stratosphere.streaming.util.LogUtils;

public class StreamComponentTest {

	private static Map<Integer, Integer> data = new HashMap<Integer, Integer>();

	public static class MySource extends UserSourceInvokable {
		private static final long serialVersionUID = 1L;
		StreamRecord record = new StreamRecord(new Tuple1<Integer>());
		String out;

		public MySource() {
		}

		public MySource(String string) {
			out = string;
		}

		@Override
		public void invoke() throws Exception {
			for (int i = 0; i < 100; i++) {
				record.setField(0, i);
				emit(record);
			}
		}

		@Override
		public String getResult() {
			return out;
		}

	}

	public static class MyTask extends UserTaskInvokable {
		private static final long serialVersionUID = 1L;
		String out;

		public MyTask() {

		}

		public MyTask(String string) {
			out = string;
		}

		@Override
		public void invoke(StreamRecord record) throws Exception {

			Integer i = record.getInteger(0);
			emit(new StreamRecord(new Tuple2<Integer, Integer>(i, i + 1)));
		}

		@Override
		public String getResult() {
			return out;
		}
	}

	public static class MyOtherTask extends UserTaskInvokable {
		private static final long serialVersionUID = 1L;
		String out;

		public MyOtherTask() {

		}

		public MyOtherTask(String string) {
			out = string;
		}

		@Override
		public void invoke(StreamRecord record) throws Exception {

			Integer i = record.getInteger(0);
			emit(new StreamRecord(new Tuple2<Integer, Integer>(-i - 1, -i - 2)));
		}

		@Override
		public String getResult() {
			return out;
		}
	}

	public static class MySink extends UserSinkInvokable {

		private static final long serialVersionUID = 1L;

		String out;

		public MySink(String out) {
			this.out = out;
		}

		@Override
		public void invoke(StreamRecord record) throws Exception {
			Integer k = record.getInteger(0);
			Integer v = record.getInteger(1);
			data.put(k, v);
		}

		@Override
		public String getResult() {
			return out;
		}
	}

	@BeforeClass
	public static void runStream() {
		LogUtils.initializeDefaultConsoleLogger(Level.DEBUG, Level.INFO);

		JobGraphBuilder graphBuilder = new JobGraphBuilder("testGraph", FaultToleranceType.NONE);
		graphBuilder.setSource("MySource", new MySource("source"));
		graphBuilder.setTask("MyTask", new MyTask("task"), 1, 1);
		graphBuilder.setTask("MyOtherTask", new MyOtherTask("otherTask"), 1, 1);
		graphBuilder.setSink("MySink", new MySink("sink"));

		graphBuilder.shuffleConnect("MySource", "MyTask");
		graphBuilder.shuffleConnect("MySource", "MyOtherTask");
		graphBuilder.shuffleConnect("MyOtherTask", "MySink");
		graphBuilder.shuffleConnect("MyTask", "MySink");

		ClusterUtil.runOnMiniCluster(graphBuilder.getJobGraph());
	}

	@Test
	public void test() {
		assertEquals(200, data.keySet().size());

		for (Integer k : data.keySet()) {
			if (k < 0) {
				assertEquals((Integer) (k - 1), data.get(k));
			} else {
				assertEquals((Integer) (k + 1), data.get(k));
			}
		}
	}
}
