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

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;

import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.client.program.Client;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.types.IntValue;

public class StreamComponentTest {
	
	private static Map<Integer, Integer> data = new HashMap<Integer, Integer>();
	
	public static class MySource extends UserSourceInvokable {
		public MySource() {
		}
		
		@Override
		public void invoke() throws Exception {
			StreamRecord record = new StreamRecord(new IntValue(-1));
			for (int i = 0; i < 1000; i++) {
				record.setField(0, new IntValue(i));
				emit(record);
			}
		}
	}
	
	public static class MyTask extends UserTaskInvokable {
		public MyTask() {
		}
		
		@Override
		public void invoke(StreamRecord record) throws Exception {
			IntValue val = (IntValue) record.getField(0);
			Integer i = val.getValue();
			emit(new StreamRecord(new IntValue(i), new IntValue(i+1)));
		}
	}
	
	public static class MySink extends UserSinkInvokable {
		public MySink() {
		}
		
		@Override
		public void invoke(StreamRecord record) throws Exception {
			IntValue k = (IntValue) record.getField(0);
			IntValue v = (IntValue) record.getField(1);
			data.put(k.getValue(), v.getValue());
		}
		
		@Override
		public String getResult() {
			return "";
		}
	}
	
	@Test
	public void test() {
		Logger root = Logger.getRootLogger();
		root.removeAllAppenders();
		root.addAppender(new ConsoleAppender());
		root.setLevel(Level.OFF);
		
		JobGraphBuilder graphBuilder = new JobGraphBuilder("testGraph");
		graphBuilder.setSource("MySource", StreamComponentTest.MySource.class);
		graphBuilder.setTask("MyTask", MyTask.class, 1);
		graphBuilder.setSink("MySink", MySink.class);

		graphBuilder.shuffleConnect("MySource", "MyTask");
		graphBuilder.shuffleConnect("MyTask", "MySink");
		
		JobGraph jG = graphBuilder.getJobGraph();
		Configuration configuration = jG.getJobConfiguration();
		
		NepheleMiniCluster exec = new NepheleMiniCluster();
		try {
			exec.start();
			Client client = new Client(new InetSocketAddress("localhost",
					6498), configuration);

			client.run(jG, true);

			exec.stop();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		assertEquals(1000, data.keySet().size());
		
		for (Integer k : data.keySet()) {
			assertEquals((Integer) (k+1), data.get(k));
		}
	}
}
