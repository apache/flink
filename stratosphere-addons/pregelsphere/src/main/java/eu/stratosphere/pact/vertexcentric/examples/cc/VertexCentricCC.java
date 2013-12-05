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
package eu.stratosphere.pact.vertexcentric.examples.cc;

import java.util.Iterator;

import eu.stratosphere.pact.client.LocalExecutor;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.vertexcentric.MessagingFunction;
import eu.stratosphere.pact.vertexcentric.VertexCentricIteration;
import eu.stratosphere.pact.vertexcentric.VertexUpdateFunction;


public class VertexCentricCC implements PlanAssembler, PlanAssemblerDescription {
	
	public static void main(String[] args) throws Exception {
		LocalExecutor.execute(new VertexCentricCC(), args);
	}
	
	@Override
	public Plan getPlan(String... args) {
		final int dop = args.length > 0 ? Integer.parseInt(args[0]) : 1;
		final String verticesPath = args.length > 1 ? args[1] : "file:///data/demodata/cc/vertices";
		final String edgesPath = args.length > 2 ? args[2] : "file:///data/demodata/cc/edges";
		final String resultPath = args.length > 3 ? args[3] : "file:///tmp";
		final int maxIterations = args.length > 4 ? Integer.parseInt(args[4]) : 10;
		
		FileDataSource initialVertices = new FileDataSource(DuplicateLongInputFormat.class, verticesPath, "Vertices");
		FileDataSource edges = new FileDataSource(LongLongInputFormat.class, edgesPath, "Edges");
		// create DataSinkContract for writing the new cluster positions
		FileDataSink result = new FileDataSink(RecordOutputFormat.class, resultPath, "Result");
		RecordOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactLong.class, 0)
			.field(PactLong.class, 1);
		
		VertexCentricIteration iteration = new VertexCentricIteration(
			new CCMessager(), new CCUpdater(), "Connected Components Iteration.");
		iteration.setVertexInput(initialVertices);
		iteration.setEdgesInput(edges);
		iteration.setNumberOfIterations(maxIterations);
		result.setInput(iteration.getOutput());
		
		Plan p = new Plan(result);
		p.setDefaultParallelism(dop);
		return p;
	}
	
	
	public static final class CCUpdater extends VertexUpdateFunction<PactLong, PactLong, PactLong> {

		private static final long serialVersionUID = 1L;

		@Override
		public void updateVertex(PactLong vertexKey, PactLong vertexValue, Iterator<PactLong> inMessages) {
			long min = Long.MAX_VALUE;
			while (inMessages.hasNext()) {
				long next = inMessages.next().getValue();
				min = Math.min(min, next);
			}
			if (min < vertexValue.getValue()) {
				setNewVertexValue(new PactLong(min));
			}
		}
		
	}
	
	public static final class CCMessager extends MessagingFunction<PactLong, PactLong, PactLong, PactNull> {

		private static final long serialVersionUID = 1L;

		@Override
		public void sendMessages(PactLong vertexId, PactLong componentId) {
			sendMessageToAllTargets(componentId);
		}
	}

	@Override
	public String getDescription() {
		return "<dop> <vertices> <edges> <result> <maxIterations>";
	}

}
