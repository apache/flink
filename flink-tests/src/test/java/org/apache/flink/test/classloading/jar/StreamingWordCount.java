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

package org.apache.flink.test.classloading.jar;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Implements the "WordCount" program that computes a simple word occurrence
 * histogram over text files in a streaming fashion.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>write a simple Flink Streaming program,
 * <li>use tuple data types,
 * <li>write and use user-defined functions.
 * </ul>
 */
public class StreamingWordCount {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get resource config
		ResourceSpec resourceSpec = null;
		if (params.has("resource")) {
			String resConfig = params.get("resource");
			if (resConfig != null && !resConfig.isEmpty()) {
				String[] resConfigArr = resConfig.split(",");
				double cpuCores = 0.0;
				int heapMemory = 0;
				for (String strResource : resConfigArr) {
					String[] keyAndVal = strResource.split(":");
					if (keyAndVal != null && keyAndVal.length == 2) {
						switch (keyAndVal[0]) {
							case "vcores":
								cpuCores = Double.valueOf(keyAndVal[1]);
								break;
							case "memory":
								heapMemory = Integer.valueOf(keyAndVal[1]);
								break;
							default:
								throw new RuntimeException("Unknown resource configuration: " + keyAndVal[0]);
						}
					}
				}
				resourceSpec = ResourceSpec.newBuilder().setCpuCores(cpuCores).setHeapMemoryInMB(heapMemory).build();
				System.out.println("cpuCores: " + cpuCores + ", heapMemory: " + heapMemory);
			}
		}

		int parallelism = 1;
		if (params.has("parallelism")) {
			parallelism = Integer.valueOf(params.get("parallelism"));
		}

		// get input data
		DataStream<String> text;
		if (params.has("input")) {
			// read the text file from given input path
			text = env.readTextFile(params.get("input"));
		} else {
			System.out.println("Executing WordCount example with default input data set.");
			System.out.println("Use --input to specify file input.");
			// get default test text data
			text = env.fromElements(WordCountData.WORDS);
		}

		DataStream<Tuple2<String, Integer>> counts = null;
		// split up the lines in pairs (2-tuples) containing: (word,1)
		if (resourceSpec != null) {
			System.out.println("Set resource spec: " + resourceSpec.toString());
			counts = ((SingleOutputStreamOperator<String>) text).setResources(resourceSpec)
				.flatMap(new Tokenizer()).setParallelism(parallelism).setResources(resourceSpec)
				// group by the tuple field "0" and sum up tuple field "1"
				.keyBy(0).sum(1).setParallelism(parallelism).setResources(resourceSpec);
		} else {
			counts = text.flatMap(new Tokenizer()).setParallelism(parallelism)
				// group by the tuple field "0" and sum up tuple field "1"
				.keyBy(0).sum(1).setParallelism(parallelism);
		}

		// emit result
		if (params.has("output")) {
			if (resourceSpec != null) {
				counts.writeAsText(params.get("output")).setResources(resourceSpec);
			} else {
				counts.writeAsText(params.get("output"));
			}
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			counts.print();
		}

		// execute program
		env.execute("Streaming WordCount");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out)
				throws Exception {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}

	private static class WordCountData {

		private static final String[] WORDS = new String[] {
				"To be, or not to be,--that is the question:--",
				"Whether 'tis nobler in the mind to suffer",
				"The slings and arrows of outrageous fortune",
				"Or to take arms against a sea of troubles,",
				"And by opposing end them?--To die,--to sleep,--",
				"No more; and by a sleep to say we end",
				"The heartache, and the thousand natural shocks",
				"That flesh is heir to,--'tis a consummation",
				"Devoutly to be wish'd. To die,--to sleep;--",
				"To sleep! perchance to dream:--ay, there's the rub;",
				"For in that sleep of death what dreams may come,",
				"When we have shuffled off this mortal coil,",
				"Must give us pause: there's the respect",
				"That makes calamity of so long life;",
				"For who would bear the whips and scorns of time,",
				"The oppressor's wrong, the proud man's contumely,",
				"The pangs of despis'd love, the law's delay,",
				"The insolence of office, and the spurns",
				"That patient merit of the unworthy takes,",
				"When he himself might his quietus make",
				"With a bare bodkin? who would these fardels bear,",
				"To grunt and sweat under a weary life,",
				"But that the dread of something after death,--",
				"The undiscover'd country, from whose bourn",
				"No traveller returns,--puzzles the will,",
				"And makes us rather bear those ills we have",
				"Than fly to others that we know not of?",
				"Thus conscience does make cowards of us all;",
				"And thus the native hue of resolution",
				"Is sicklied o'er with the pale cast of thought;",
				"And enterprises of great pith and moment,",
				"With this regard, their currents turn awry,",
				"And lose the name of action.--Soft you now!",
				"The fair Ophelia!--Nymph, in thy orisons",
				"Be all my sins remember'd."
		};
	}

}
