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

package org.apache.flink.test.compiler.examples;

import java.util.Arrays;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.distributions.SimpleDistribution;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.api.java.record.io.TextInputFormat;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.test.recordJobs.wordcount.WordCount;
import org.apache.flink.test.recordJobs.wordcount.WordCount.CountWords;
import org.apache.flink.test.recordJobs.wordcount.WordCount.TokenizeLine;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.StringValue;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class WordCountCompilerTest extends CompilerTestBase {
	
	/**
	 * This method tests the simple word count.
	 */
	@Test
	public void testWordCount() {
		checkWordCount(true);
		checkWordCount(false);
	}
	
	private void checkWordCount(boolean estimates) {
		try {
			WordCount wc = new WordCount();
			ExecutionConfig ec = new ExecutionConfig();
			Plan p = wc.getPlan(DEFAULT_PARALLELISM_STRING, IN_FILE, OUT_FILE);
			p.setExecutionConfig(ec);

			OptimizedPlan plan;
			if (estimates) {
				FileDataSource source = getContractResolver(p).getNode("Input Lines");
				setSourceStatistics(source, 1024*1024*1024*1024L, 24f);
				plan = compileWithStats(p);
			} else {
				plan = compileNoStats(p);
			}
			
			// get the optimizer plan nodes
			OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(plan);
			SinkPlanNode sink = resolver.getNode("Word Counts");
			SingleInputPlanNode reducer = resolver.getNode("Count Words");
			SingleInputPlanNode mapper = resolver.getNode("Tokenize Lines");
			
			// verify the strategies
			Assert.assertEquals(ShipStrategyType.FORWARD, mapper.getInput().getShipStrategy());
			Assert.assertEquals(ShipStrategyType.PARTITION_HASH, reducer.getInput().getShipStrategy());
			Assert.assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
			
			Channel c = reducer.getInput();
			Assert.assertEquals(LocalStrategy.COMBININGSORT, c.getLocalStrategy());
			FieldList l = new FieldList(0);
			Assert.assertEquals(l, c.getShipStrategyKeys());
			Assert.assertEquals(l, c.getLocalStrategyKeys());
			Assert.assertTrue(Arrays.equals(c.getLocalStrategySortOrder(), reducer.getSortOrders(0)));
			
			// check the combiner
			SingleInputPlanNode combiner = (SingleInputPlanNode) reducer.getPredecessor();
			Assert.assertEquals(DriverStrategy.SORTED_GROUP_COMBINE, combiner.getDriverStrategy());
			Assert.assertEquals(l, combiner.getKeys(0));
			Assert.assertEquals(ShipStrategyType.FORWARD, combiner.getInput().getShipStrategy());
			
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	/**
	 * This method tests that with word count and a range partitioned sink, the range partitioner is pushed down.
	 */
	@Test
	public void testWordCountWithSortedSink() {
		checkWordCountWithSortedSink(true);
		checkWordCountWithSortedSink(false);
	}
	
	private void checkWordCountWithSortedSink(boolean estimates) {
		try {
			FileDataSource sourceNode = new FileDataSource(new TextInputFormat(), IN_FILE, "Input Lines");
			MapOperator mapNode = MapOperator.builder(new TokenizeLine())
				.input(sourceNode)
				.name("Tokenize Lines")
				.build();
			ReduceOperator reduceNode = ReduceOperator.builder(new CountWords(), StringValue.class, 0)
				.input(mapNode)
				.name("Count Words")
				.build();
			FileDataSink out = new FileDataSink(new CsvOutputFormat(), OUT_FILE, reduceNode, "Word Counts");
			CsvOutputFormat.configureRecordFormat(out)
				.recordDelimiter('\n')
				.fieldDelimiter(' ')
				.lenient(true)
				.field(StringValue.class, 0)
				.field(IntValue.class, 1);
			
			Ordering ordering = new Ordering(0, StringValue.class, Order.DESCENDING);
			out.setGlobalOrder(ordering, new SimpleDistribution(new StringValue[] {new StringValue("N")}));

			ExecutionConfig ec = new ExecutionConfig();
			Plan p = new Plan(out, "WordCount Example");
			p.setDefaultParallelism(DEFAULT_PARALLELISM);
			p.setExecutionConfig(ec);
	
			OptimizedPlan plan;
			if (estimates) {
				setSourceStatistics(sourceNode, 1024*1024*1024*1024L, 24f);
				plan = compileWithStats(p);
			} else {
				plan = compileNoStats(p);
			}
			
			OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(plan);
			SinkPlanNode sink = resolver.getNode("Word Counts");
			SingleInputPlanNode reducer = resolver.getNode("Count Words");
			SingleInputPlanNode mapper = resolver.getNode("Tokenize Lines");
			
			Assert.assertEquals(ShipStrategyType.FORWARD, mapper.getInput().getShipStrategy());
			Assert.assertEquals(ShipStrategyType.PARTITION_RANGE, reducer.getInput().getShipStrategy());
			Assert.assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
			
			Channel c = reducer.getInput();
			Assert.assertEquals(LocalStrategy.COMBININGSORT, c.getLocalStrategy());
			FieldList l = new FieldList(0);
			Assert.assertEquals(l, c.getShipStrategyKeys());
			Assert.assertEquals(l, c.getLocalStrategyKeys());
			
			// check that the sort orders are descending
			Assert.assertFalse(c.getShipStrategySortOrder()[0]);
			Assert.assertFalse(c.getLocalStrategySortOrder()[0]);
			
			// check the combiner
			SingleInputPlanNode combiner = (SingleInputPlanNode) reducer.getPredecessor();
			Assert.assertEquals(DriverStrategy.SORTED_GROUP_COMBINE, combiner.getDriverStrategy());
			Assert.assertEquals(l, combiner.getKeys(0));
			Assert.assertEquals(l, combiner.getKeys(1));
			Assert.assertEquals(ShipStrategyType.FORWARD, combiner.getInput().getShipStrategy());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
}
