/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.compiler.plan;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.common.operators.OperatorInformation;
import eu.stratosphere.api.common.operators.base.GenericDataSourceBase;
import eu.stratosphere.api.java.io.TextInputFormat;
import eu.stratosphere.api.java.typeutils.BasicTypeInfo;
import eu.stratosphere.compiler.dag.DataSourceNode;
import eu.stratosphere.core.fs.Path;

public class ChannelTest {
	
	@Test
	public void testGetEstimatesNoReplicationFactor() {
		final long NUM_RECORD = 1001;
		final long SIZE = 467131;
		
		DataSourceNode source = getSourceNode();
		SourcePlanNode planNode = new SourcePlanNode(source, "test node");
		Channel channel = new Channel(planNode);

		// no estimates here
		Assert.assertEquals(-1, channel.getEstimatedOutputSize());
		Assert.assertEquals(-1, channel.getEstimatedNumRecords());
		
		// set estimates
		source.setEstimatedNumRecords(NUM_RECORD);
		source.setEstimatedOutputSize(SIZE);
		Assert.assertEquals(SIZE, channel.getEstimatedOutputSize());
		Assert.assertEquals(NUM_RECORD, channel.getEstimatedNumRecords());
	}
	
	@Test
	public void testGetEstimatesWithReplicationFactor() {
		final long NUM_RECORD = 1001;
		final long SIZE = 467131;
		
		final int REPLICATION = 23;
		
		DataSourceNode source = getSourceNode();
		SourcePlanNode planNode = new SourcePlanNode(source, "test node");
		Channel channel = new Channel(planNode);
		channel.setReplicationFactor(REPLICATION);

		// no estimates here
		Assert.assertEquals(-1, channel.getEstimatedOutputSize());
		Assert.assertEquals(-1, channel.getEstimatedNumRecords());
		
		// set estimates
		source.setEstimatedNumRecords(NUM_RECORD);
		source.setEstimatedOutputSize(SIZE);
		Assert.assertEquals(SIZE * REPLICATION, channel.getEstimatedOutputSize());
		Assert.assertEquals(NUM_RECORD * REPLICATION, channel.getEstimatedNumRecords());
	}
	
	
//	private static final OptimizerNode getSingleInputNode() {
//		return new MapNode(new MapOperatorBase<String, String, GenericMap<String,String>>(
//				new IdentityMapper<String>(),
//				new UnaryOperatorInformation<String, String>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO),
//				"map"));
//	}
	
	private static final DataSourceNode getSourceNode() {
		return new DataSourceNode(new GenericDataSourceBase<String, TextInputFormat>(
				new TextInputFormat(new Path("/ignored")), 
				new OperatorInformation<String>(BasicTypeInfo.STRING_TYPE_INFO),
				"source"));
	}
}
