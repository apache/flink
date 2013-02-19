/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.pact.compiler;

import java.util.Arrays;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SinkPlanNode;
import eu.stratosphere.pact.compiler.util.MockDataDistribution;
import eu.stratosphere.pact.example.wordcount.WordCount;
import eu.stratosphere.pact.example.wordcount.WordCount.CountWords;
import eu.stratosphere.pact.example.wordcount.WordCount.TokenizeLine;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;

/**
 *
 */
public class ExampleProgramsWithEstimatesTest extends CompilerTestBase {
	
	/**
	 * This method tests the simple word count.
	 */
	@Test
	public void testWordCount() {
		WordCount wc = new WordCount();
		Plan p = wc.getPlan(String.valueOf(DEFAULT_PARALLELISM), IN_FILE_1, OUT_FILE_1);
		
		OptimizedPlan plan = compile(p);
		
		SinkPlanNode sink = plan.getDataSinks().iterator().next();
		SingleInputPlanNode reducer = (SingleInputPlanNode) sink.getPredecessor();
		SingleInputPlanNode mapper = (SingleInputPlanNode) reducer.getPredecessor();
		
		Assert.assertEquals(ShipStrategyType.FORWARD, mapper.getInput().getShipStrategy());
		Assert.assertEquals(ShipStrategyType.PARTITION_HASH, reducer.getInput().getShipStrategy());
		Assert.assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
		
		Channel c = reducer.getInput();
		Assert.assertEquals(LocalStrategy.COMBININGSORT, c.getLocalStrategy());
		FieldList l = new FieldList(0);
		Assert.assertEquals(l, c.getShipStrategyKeys());
		Assert.assertEquals(l, c.getLocalStrategyKeys());
		Assert.assertTrue(Arrays.equals(c.getLocalStrategySortOrder(), reducer.getSortOrders()));
	}
	
	/**
	 * This method tests that with word count and a range partitioned sink, the range partitioner is pushed down.
	 */
	@Test
	public void testWordCountWithSortedSink() {
		
		FileDataSource sourceNode = new FileDataSource(TextInputFormat.class, IN_FILE_1, "Input Lines");
		MapContract mapNode = MapContract.builder(TokenizeLine.class)
			.input(sourceNode)
			.name("Tokenize Lines")
			.build();
		ReduceContract reduceNode = new ReduceContract.Builder(CountWords.class, PactString.class, 0)
			.input(mapNode)
			.name("Count Words")
			.build();
		FileDataSink out = new FileDataSink(RecordOutputFormat.class, OUT_FILE_1, reduceNode, "Word Counts");
		RecordOutputFormat.configureRecordFormat(out)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.lenient(true)
			.field(PactString.class, 0)
			.field(PactInteger.class, 1);
		
		out.setGlobalOrder(new Ordering(0, PactString.class, Order.DESCENDING), new MockDataDistribution());
		
		Plan p = new Plan(out, "WordCount Example");
		p.setDefaultParallelism(DEFAULT_PARALLELISM);

		OptimizedPlan plan = compile(p);
		
		SinkPlanNode sink = plan.getDataSinks().iterator().next();
		SingleInputPlanNode reducer = (SingleInputPlanNode) sink.getPredecessor();
		SingleInputPlanNode mapper = (SingleInputPlanNode) reducer.getPredecessor();
		
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
	}
}
