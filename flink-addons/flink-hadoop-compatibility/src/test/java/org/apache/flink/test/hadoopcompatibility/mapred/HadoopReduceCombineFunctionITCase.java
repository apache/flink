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

package org.apache.flink.test.hadoopcompatibility.mapred;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoopcompatibility.mapred.HadoopReduceCombineFunction;
import org.apache.flink.hadoopcompatibility.mapred.HadoopReduceFunction;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class HadoopReduceCombineFunctionITCase extends JavaProgramTestBase {

	private static int NUM_PROGRAMS = 4;
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;
	
	public HadoopReduceCombineFunctionITCase(Configuration config) {
		super(config);	
	}
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = ReducerProgs.runProgram(curProgId, resultPath);
	}
	
	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}
	
	@Override
	protected boolean skipCollectionExecution() {
		if (this.curProgId == 3) {
			return true;
		}
		return false;
	}
	
	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		for(int i=1; i <= NUM_PROGRAMS; i++) {
			Configuration config = new Configuration();
			config.setInteger("ProgramId", i);
			tConfigs.add(config);
		}
		
		return toParameterList(tConfigs);
	}
	
	public static class ReducerProgs {
		
		public static String runProgram(int progId, String resultPath) throws Exception {
			
			switch(progId) {
			case 1: {
				/*
				 * Test standard counting with combiner
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple2<IntWritable, IntWritable>> ds = HadoopTestData.getKVPairDataSet(env).
						map(new MapFunction<Tuple2<IntWritable, Text>, Tuple2<IntWritable, IntWritable>>() {
							private static final long serialVersionUID = 1L;
							Tuple2<IntWritable,IntWritable> outT = new Tuple2<IntWritable,IntWritable>();
							@Override
							public Tuple2<IntWritable, IntWritable> map(Tuple2<IntWritable, Text> v)
									throws Exception {
								outT.f0 = new IntWritable(v.f0.get() / 6);
								outT.f1 = new IntWritable(1);
								return outT;
							}
						});
						
				DataSet<Tuple2<IntWritable, IntWritable>> counts = ds.
						groupBy(0).
						reduceGroup(new HadoopReduceCombineFunction<IntWritable, IntWritable, IntWritable, IntWritable>(
								new SumReducer(), new SumReducer()));
				
				counts.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return 	"(0,5)\n"+
						"(1,6)\n" +
						"(2,6)\n" +
						"(3,4)\n";
			}
			case 2: {
				/*
				 * Test ungrouped Hadoop reducer
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple2<IntWritable, IntWritable>> ds = HadoopTestData.getKVPairDataSet(env).
						map(new MapFunction<Tuple2<IntWritable, Text>, Tuple2<IntWritable, IntWritable>>() {
							private static final long serialVersionUID = 1L;
							Tuple2<IntWritable,IntWritable> outT = new Tuple2<IntWritable,IntWritable>();
							@Override
							public Tuple2<IntWritable, IntWritable> map(Tuple2<IntWritable, Text> v)
									throws Exception {
								outT.f0 = new IntWritable(0);
								outT.f1 = v.f0;
								return outT;
							}
						});
						
				DataSet<Tuple2<IntWritable, IntWritable>> sum = ds.
						reduceGroup(new HadoopReduceCombineFunction<IntWritable, IntWritable, IntWritable, IntWritable>(
								new SumReducer(), new SumReducer()));
				
				sum.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return 	"(0,231)\n";
			}
			case 3: {
				/* Test combiner */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<Tuple2<IntWritable, IntWritable>> ds = HadoopTestData.getKVPairDataSet(env).
						map(new MapFunction<Tuple2<IntWritable, Text>, Tuple2<IntWritable, IntWritable>>() {
							private static final long serialVersionUID = 1L;
							Tuple2<IntWritable,IntWritable> outT = new Tuple2<IntWritable,IntWritable>();
							@Override
							public Tuple2<IntWritable, IntWritable> map(Tuple2<IntWritable, Text> v)
									throws Exception {
								outT.f0 = v.f0;
								outT.f1 = new IntWritable(1);
								return outT;
							}
						});
						
				DataSet<Tuple2<IntWritable, IntWritable>> counts = ds.
						groupBy(0).
						reduceGroup(new HadoopReduceCombineFunction<IntWritable, IntWritable, IntWritable, IntWritable>(
								new SumReducer(), new KeyChangingReducer()));
				
				counts.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return 	"(0,5)\n"+
						"(1,6)\n" +
						"(2,5)\n" +
						"(3,5)\n";
			}
			case 4: {
				/*
				 * Test configuration via JobConf
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				JobConf conf = new JobConf();
				conf.set("my.cntPrefix", "Hello");
				
				DataSet<Tuple2<IntWritable, Text>> ds = HadoopTestData.getKVPairDataSet(env).
						map(new MapFunction<Tuple2<IntWritable, Text>, Tuple2<IntWritable, Text>>() {
							private static final long serialVersionUID = 1L;
							@Override
							public Tuple2<IntWritable, Text> map(Tuple2<IntWritable, Text> v)
									throws Exception {
								v.f0 = new IntWritable(v.f0.get() % 5);
								return v;
							}
						});
						
				DataSet<Tuple2<IntWritable, IntWritable>> hellos = ds.
						groupBy(0).
						reduceGroup(new HadoopReduceFunction<IntWritable, Text, IntWritable, IntWritable>(
								new ConfigurableCntReducer(), conf));
				
				hellos.writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return 	"(0,0)\n"+
						"(1,0)\n" +
						"(2,1)\n" +
						"(3,1)\n" +
						"(4,1)\n";
			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
			
		}
	
	}
	
	public static class SumReducer implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		@Override
		public void reduce(IntWritable k, Iterator<IntWritable> v, OutputCollector<IntWritable, IntWritable> out, Reporter r)
				throws IOException {
			
			int sum = 0;
			while(v.hasNext()) {
				sum += v.next().get();
			}
			out.collect(k, new IntWritable(sum));
		}
		
		@Override
		public void configure(JobConf arg0) { }

		@Override
		public void close() throws IOException { }
	}
	
	public static class KeyChangingReducer implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		@Override
		public void reduce(IntWritable k, Iterator<IntWritable> v, OutputCollector<IntWritable, IntWritable> out, Reporter r)
				throws IOException {
			while(v.hasNext()) {
				out.collect(new IntWritable(k.get() % 4), v.next());
			}
		}
		
		@Override
		public void configure(JobConf arg0) { }

		@Override
		public void close() throws IOException { }
	}
	
	public static class ConfigurableCntReducer implements Reducer<IntWritable, Text, IntWritable, IntWritable> {
		private String countPrefix;
		
		@Override
		public void reduce(IntWritable k, Iterator<Text> vs, OutputCollector<IntWritable, IntWritable> out, Reporter r)
				throws IOException {
			int commentCnt = 0;
			while(vs.hasNext()) {
				String v = vs.next().toString();
				if(v.startsWith(this.countPrefix)) {
					commentCnt++;
				}
			}
			out.collect(k, new IntWritable(commentCnt));
		}
		
		@Override
		public void configure(final JobConf c) { 
			this.countPrefix = c.get("my.cntPrefix");
		}

		@Override
		public void close() throws IOException { }
	}
}
