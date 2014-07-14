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
package eu.stratosphere.streaming.examples.basictopology;

import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.streaming.api.DataStream;
import eu.stratosphere.streaming.api.SourceFunction;
import eu.stratosphere.streaming.api.StreamExecutionEnvironment;
import eu.stratosphere.util.Collector;

public class BasicTopology {

	public static class BasicSource extends SourceFunction<Tuple1<String>> {

		private static final long serialVersionUID = 1L;
		Tuple1<String> tuple =  new Tuple1<String>("streaming");

		@Override
		public void invoke(Collector<Tuple1<String>> collector) throws Exception {
			// emit continuously a tuple
			while (true) {
				collector.collect(tuple);
			}
		}
	}

	public static class BasicMap extends MapFunction<Tuple1<String>, Tuple1<String>> {
		private static final long serialVersionUID = 1L;

		// map to the same tuple
		@Override
		public Tuple1<String> map(Tuple1<String> value) throws Exception {
			return value;
		}

	}

	public static void main(String[] args) {
		StreamExecutionEnvironment context = new StreamExecutionEnvironment();
		
		DataStream<Tuple1<String>> stream = context.addSource(new BasicSource()).map(new BasicMap()).addDummySink();
		
		context.execute();
	}
}
