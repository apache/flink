/***********************************************************************************************************************
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
 **********************************************************************************************************************/
package eu.stratosphere.spargel.java.examples.connectedcomponents;

import java.util.Iterator;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.spargel.java.MessagingFunction;
import eu.stratosphere.spargel.java.SpargelIteration;
import eu.stratosphere.spargel.java.VertexUpdateFunction;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.NullValue;

public class SpargelConnectedComponents implements Program, ProgramDescription {
	
	public static void main(String[] args) throws Exception {
		LocalExecutor.execute(new SpargelConnectedComponents(), args);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Plan getPlan(String... args) {
		final int dop = args.length > 0 ? Integer.parseInt(args[0]) : 1;
		final String verticesPath = args.length > 1 ? args[1] : "";
		final String edgesPath = args.length > 2 ? args[2] : "";
		final String resultPath = args.length > 3 ? args[3] : "";
		final int maxIterations = args.length > 4 ? Integer.parseInt(args[4]) : 1;
		
		FileDataSource initialVertices = new FileDataSource(DuplicateLongInputFormat.class, verticesPath, "Vertices");
		FileDataSource edges = new FileDataSource(new CsvInputFormat(' ', LongValue.class, LongValue.class), edgesPath, "Edges");
		
		FileDataSink result = new FileDataSink(CsvOutputFormat.class, resultPath, "Result");
		CsvOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(LongValue.class, 0)
			.field(LongValue.class, 1);
		
		SpargelIteration iteration = new SpargelIteration(
			new CCMessager(), new CCUpdater(), "Connected Components (Spargel API)");
		iteration.setVertexInput(initialVertices);
		iteration.setEdgesInput(edges);
		iteration.setNumberOfIterations(maxIterations);
		result.setInput(iteration.getOutput());

		Plan p = new Plan(result);
		p.setDefaultParallelism(dop);
		return p;
	}
	
	
	public static final class CCUpdater extends VertexUpdateFunction<LongValue, LongValue, LongValue> {

		private static final long serialVersionUID = 1L;

		@Override
		public void updateVertex(LongValue vertexKey, LongValue vertexValue, Iterable<LongValue> inMessages) {
			long min = Long.MAX_VALUE;
			for (LongValue msg : inMessages) {
				long next = msg.getValue();
				min = Math.min(min, next);
			}
			if (min < vertexValue.getValue()) {
				setNewVertexValue(new LongValue(min));
			}
		}
		
	}
	
	public static final class CCMessager extends MessagingFunction<LongValue, LongValue, LongValue, NullValue> {

		private static final long serialVersionUID = 1L;

		@Override
		public void sendMessages(LongValue vertexId, LongValue componentId) {
			sendMessageToAllNeighbors(componentId);
		}

	}

	@Override
	public String getDescription() {
		return "<dop> <vertices> <edges> <result> <maxIterations>";
	}
}
