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

package org.apache.flink.contrib.operatorstatistics;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.Random;

@SuppressWarnings("serial")
public class OperatorStatsAccumulatorTest extends AbstractTestBase {

	private static final Logger LOG = LoggerFactory.getLogger(OperatorStatsAccumulatorTest.class);

	private static final String ACCUMULATOR_NAME = "op-stats";

	public OperatorStatsAccumulatorTest(){
		super(new Configuration());
	}
	
	public static class StringToInt extends RichFlatMapFunction<String, Tuple1<Integer>> {

		// Is instantiated later since the runtime context is not yet initialized
		private Accumulator<Object, Serializable> globalAccumulator;
		private Accumulator<Object,Serializable> localAccumulator;
		OperatorStatisticsConfig accumulatorConfig;

		public StringToInt(OperatorStatisticsConfig config){
			accumulatorConfig = config;
		}

		@Override
		public void open(Configuration parameters) {
			// Add globalAccumulator using convenience function

			globalAccumulator = getRuntimeContext().getAccumulator(ACCUMULATOR_NAME);
			if (globalAccumulator==null){
				getRuntimeContext().addAccumulator(ACCUMULATOR_NAME, new OperatorStatisticsAccumulator(accumulatorConfig));
				globalAccumulator = getRuntimeContext().getAccumulator(ACCUMULATOR_NAME);
			}

			int subTaskIndex = getRuntimeContext().getIndexOfThisSubtask();
			localAccumulator = getRuntimeContext().getAccumulator(ACCUMULATOR_NAME+"-"+subTaskIndex);
			if (localAccumulator==null){
				getRuntimeContext().addAccumulator(ACCUMULATOR_NAME+"-"+subTaskIndex, new OperatorStatisticsAccumulator(accumulatorConfig));
				localAccumulator = getRuntimeContext().getAccumulator(ACCUMULATOR_NAME+"-"+subTaskIndex);
			}
		}

		@Override
		public void flatMap(String value, Collector<Tuple1<Integer>> out) throws Exception {
			int intValue;
			try {
				intValue = Integer.parseInt(value);
				localAccumulator.add(intValue);
				out.collect(new Tuple1<>(intValue));
			} catch (NumberFormatException ignored) {}
		}

		@Override
		public void close(){
			globalAccumulator.merge(localAccumulator);
		}
	}

	@Test
	public void testAccumulatorAllStatistics() throws Exception {

		String input = "";

		Random rand = new Random();

		for (int i = 1; i < 1000; i++) {
			if(rand.nextDouble()<0.2){
				input+=String.valueOf(rand.nextInt(4))+"\n";
			}else{
				input+=String.valueOf(rand.nextInt(100))+"\n";
			}
		}

		String inputFile = createTempFile("datapoints.txt", input);

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();

		OperatorStatisticsConfig operatorStatisticsConfig =
				new OperatorStatisticsConfig(OperatorStatisticsConfig.CountDistinctAlgorithm.HYPERLOGLOG,
											OperatorStatisticsConfig.HeavyHitterAlgorithm.LOSSY_COUNTING);

		env.readTextFile(inputFile).
				flatMap(new StringToInt(operatorStatisticsConfig)).
				output(new DiscardingOutputFormat<Tuple1<Integer>>());

		JobExecutionResult result = env.execute();

		OperatorStatistics globalStats = result.getAccumulatorResult(ACCUMULATOR_NAME);
//		System.out.println("Global Stats");
//		System.out.println(globalStats.toString());

		OperatorStatistics merged = null;

		Map<String,Object> accResults = result.getAllAccumulatorResults();
		for (String accumulatorName:accResults.keySet()){
			if (accumulatorName.contains(ACCUMULATOR_NAME+"-")){
				OperatorStatistics localStats = (OperatorStatistics) accResults.get(accumulatorName);
				LOG.debug("Local Stats: " + accumulatorName);
				LOG.debug(localStats.toString());
				if (merged == null){
					merged = localStats.clone();
				}else {
					merged.merge(localStats);
				}
			}
		}

		LOG.debug("Local Stats Merged: \n");
		LOG.debug(merged.toString());

		Assert.assertEquals("Global cardinality should be 999", 999, globalStats.cardinality);
		Assert.assertEquals("Count distinct estimate should be around 100 and is "+globalStats.estimateCountDistinct()
				, 100.0, (double)globalStats.estimateCountDistinct(),5.0);
		Assert.assertTrue("The total number of heavy hitters should be between 0 and 5."
				, globalStats.getHeavyHitters().size() > 0 && globalStats.getHeavyHitters().size() <= 5);
		Assert.assertEquals("Min when merging the local accumulators should correspond with min" +
				"of the global accumulator",merged.getMin(),globalStats.getMin());
		Assert.assertEquals("Max resulting from merging the local accumulators should correspond to" +
				"max of the global accumulator",merged.getMax(),globalStats.getMax());
		Assert.assertEquals("Count distinct when merging the local accumulators should correspond to " +
				"count distinct in the global accumulator",merged.estimateCountDistinct(),globalStats.estimateCountDistinct());
		Assert.assertEquals("The number of heavy hitters when merging the local accumulators should correspond " +
				"to the number of heavy hitters in the global accumulator",merged.getHeavyHitters().size(),globalStats.getHeavyHitters().size());
	}

	@Test
	public void testAccumulatorMinMax() throws Exception {

		String input = "";

		Random rand = new Random();

		for (int i = 1; i < 1000; i++) {
			if (rand.nextDouble() < 0.2) {
				input += String.valueOf(rand.nextInt(4)) + "\n";
			} else {
				input += String.valueOf(rand.nextInt(100)) + "\n";
			}
		}

		String inputFile = createTempFile("datapoints.txt", input);

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();

		OperatorStatisticsConfig operatorStatisticsConfig =
				new OperatorStatisticsConfig(false);
		operatorStatisticsConfig.collectMax = true;
		operatorStatisticsConfig.collectMin = true;

		env.readTextFile(inputFile).
				flatMap(new StringToInt(operatorStatisticsConfig)).
				output(new DiscardingOutputFormat<Tuple1<Integer>>());

		JobExecutionResult result = env.execute();

		OperatorStatistics globalStats = result.getAccumulatorResult(ACCUMULATOR_NAME);
//		System.out.println("Global Stats");
//		System.out.println(globalStats.toString());

		Assert.assertTrue("Min value for accumulator should not be null",globalStats.getMin()!=null);
		Assert.assertTrue("Max value for accumulator should not be null",globalStats.getMax()!=null);
	}

	@Test
	public void testAccumulatorCountDistinctLinearCounting() throws Exception {

		String input = "";

		Random rand = new Random();

		for (int i = 1; i < 1000; i++) {
			if (rand.nextDouble() < 0.2) {
				input += String.valueOf(rand.nextInt(4)) + "\n";
			} else {
				input += String.valueOf(rand.nextInt(100)) + "\n";
			}
		}

		String inputFile = createTempFile("datapoints.txt", input);

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();

		OperatorStatisticsConfig operatorStatisticsConfig =
				new OperatorStatisticsConfig(false);
		operatorStatisticsConfig.collectCountDistinct = true;
		operatorStatisticsConfig.countDistinctAlgorithm = OperatorStatisticsConfig.CountDistinctAlgorithm.LINEAR_COUNTING;
		operatorStatisticsConfig.setCountDbitmap(10000);

		env.readTextFile(inputFile).
				flatMap(new StringToInt(operatorStatisticsConfig)).
				output(new DiscardingOutputFormat<Tuple1<Integer>>());

		JobExecutionResult result = env.execute();

		OperatorStatistics globalStats = result.getAccumulatorResult(ACCUMULATOR_NAME);
//		System.out.println("Global Stats");
//		System.out.println(globalStats.toString());

		Assert.assertTrue("Count Distinct for accumulator should not be null",globalStats.countDistinct!=null);
	}

	@Test
	public void testAccumulatorHeavyHitterCountMinSketch() throws Exception {

		String input = "";

		Random rand = new Random();

		for (int i = 1; i < 1000; i++) {
			if (rand.nextDouble() < 0.2) {
				input += String.valueOf(rand.nextInt(4)) + "\n";
			} else {
				input += String.valueOf(rand.nextInt(100)) + "\n";
			}
		}

		String inputFile = createTempFile("datapoints.txt", input);

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();

		OperatorStatisticsConfig operatorStatisticsConfig =
				new OperatorStatisticsConfig(false);
		operatorStatisticsConfig.collectHeavyHitters = true;
		operatorStatisticsConfig.heavyHitterAlgorithm = OperatorStatisticsConfig.HeavyHitterAlgorithm.COUNT_MIN_SKETCH;

		env.readTextFile(inputFile).
				flatMap(new StringToInt(operatorStatisticsConfig)).
				output(new DiscardingOutputFormat<Tuple1<Integer>>());

		JobExecutionResult result = env.execute();

		OperatorStatistics globalStats = result.getAccumulatorResult(ACCUMULATOR_NAME);
//		System.out.println("Global Stats");
//		System.out.println(globalStats.toString());

		Assert.assertTrue("Count Distinct for accumulator should not be null",globalStats.heavyHitter!=null);
	}

}
