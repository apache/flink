/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.test.broadcastvars;

import java.io.BufferedReader;
import java.util.Collection;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;

import eu.stratosphere.api.common.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordSerializerFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.test.iterative.nephele.JobGraphUtils;
import eu.stratosphere.test.util.TestBase2;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class BroadcastVarsNepheleITCase extends TestBase2 {

	private static final long SEED_POINTS = 0xBADC0FFEEBEEFL;

	private static final long SEED_MODELS = 0x39134230AFF32L;

	private static final int NUM_POINTS = 10000;

	private static final int NUM_MODELS = 42;

	private static final int NUM_FEATURES = 3;

	protected String pointsPath;

	protected String modelsPath;

	protected String resultPath;


	

	public static final String getInputPoints(int numPoints, int numDimensions, long seed) {
		if (numPoints < 1 || numPoints > 1000000)
			throw new IllegalArgumentException();

		Random r = new Random();

		StringBuilder bld = new StringBuilder(3 * (1 + numDimensions) * numPoints);
		for (int i = 1; i <= numPoints; i++) {
			bld.append(i);
			bld.append(' ');

			r.setSeed(seed + 1000 * i);
			for (int j = 1; j <= numDimensions; j++) {
				bld.append(r.nextInt(1000));
				bld.append(' ');
			}
			bld.append('\n');
		}
		return bld.toString();
	}

	public static final String getInputModels(int numModels, int numDimensions, long seed) {
		if (numModels < 1 || numModels > 100)
			throw new IllegalArgumentException();

		Random r = new Random();

		StringBuilder bld = new StringBuilder(3 * (1 + numDimensions) * numModels);
		for (int i = 1; i <= numModels; i++) {
			bld.append(i);
			bld.append(' ');

			r.setSeed(seed + 1000 * i);
			for (int j = 1; j <= numDimensions; j++) {
				bld.append(r.nextInt(100));
				bld.append(' ');
			}
			bld.append('\n');
		}
		return bld.toString();
	}

	@Override
	protected void preSubmit() throws Exception {
		this.pointsPath = createTempFile("points.txt", getInputPoints(NUM_POINTS, NUM_FEATURES, SEED_POINTS));
		this.modelsPath = createTempFile("models.txt", getInputModels(NUM_MODELS, NUM_FEATURES, SEED_MODELS));
		this.resultPath = getTempFilePath("results");
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {
		return createJobGraphV1(this.pointsPath, this.modelsPath, this.resultPath, 4);
	}

	@Override
	protected void postSubmit() throws Exception {
		final Random randPoints = new Random();
		final Random randModels = new Random();
		final Pattern p = Pattern.compile("(\\d+) (\\d+) (\\d+)");
		
		long [][] results = new long[NUM_POINTS][NUM_MODELS];
		boolean [][] occurs = new boolean[NUM_POINTS][NUM_MODELS];
		for (int i = 0; i < NUM_POINTS; i++) {
			for (int j = 0; j < NUM_MODELS; j++) {
				long actDotProd = 0;
				randPoints.setSeed(SEED_POINTS + 1000 * (i+1));
				randModels.setSeed(SEED_MODELS + 1000 * (j+1));
				for (int z = 1; z <= NUM_FEATURES; z++) {
					actDotProd += randPoints.nextInt(1000) * randModels.nextInt(100);
				}
				results[i][j] = actDotProd;
				occurs[i][j] = false;
			}
		}

		for (BufferedReader reader : getResultReader(this.resultPath)) {
			String line = null;
			while (null != (line = reader.readLine())) {
				final Matcher m = p.matcher(line);
				Assert.assertTrue(m.matches());

				int modelId = Integer.parseInt(m.group(1));
				int pointId = Integer.parseInt(m.group(2));
				long expDotProd = Long.parseLong(m.group(3));

				Assert.assertFalse("Dot product for record (" + pointId + ", " + modelId + ") occurs more than once", occurs[pointId-1][modelId-1]);
				Assert.assertEquals(String.format("Bad product for (%04d, %04d)", pointId, modelId), expDotProd, results[pointId-1][modelId-1]);

				occurs[pointId-1][modelId-1] = true;
			}
		}

		for (int i = 0; i < NUM_POINTS; i++) {
			for (int j = 0; j < NUM_MODELS; j++) {
				Assert.assertTrue("Dot product for record (" + (i+1) + ", " + (j+1) + ") does not occur", occurs[i][j]);
			}
		}
	}

	// -------------------------------------------------------------------------------------------------------------
	// UDFs
	// -------------------------------------------------------------------------------------------------------------

	public static final class DotProducts extends MapFunction {

		private static final long serialVersionUID = 1L;

		private final Record result = new Record(3);

		private final LongValue lft = new LongValue();

		private final LongValue rgt = new LongValue();

		private final LongValue prd = new LongValue();

		private Collection<Record> models;

		@Override
		public void open(Configuration parameters) throws Exception {
			this.models = this.getRuntimeContext().getBroadcastVariable("models");
		}

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {

			for (Record model : this.models) {
				// compute dot product between model and pair
				long product = 0;
				for (int i = 1; i <= NUM_FEATURES; i++) {
					product += model.getField(i, this.lft).getValue() * record.getField(i, this.rgt).getValue();
				}
				this.prd.setValue(product);

				// construct result
				this.result.copyFrom(model, new int[] { 0 }, new int[] { 0 });
				this.result.copyFrom(record, new int[] { 0 }, new int[] { 1 });
				this.result.setField(2, this.prd);

				// emit result
				out.collect(this.result);
			}
		}
	}

	// -------------------------------------------------------------------------------------------------------------
	// Job vertex builder methods
	// -------------------------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private static JobInputVertex createPointsInput(JobGraph jobGraph, String pointsPath, int numSubTasks, TypeSerializerFactory<?> serializer) {
		CsvInputFormat pointsInFormat = new CsvInputFormat(' ', LongValue.class, LongValue.class, LongValue.class, LongValue.class);
		JobInputVertex pointsInput = JobGraphUtils.createInput(pointsInFormat, pointsPath, "Input[Points]", jobGraph, numSubTasks, numSubTasks);

		{
			TaskConfig taskConfig = new TaskConfig(pointsInput.getConfiguration());
			taskConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			taskConfig.setOutputSerializer(serializer);
		}

		return pointsInput;
	}

	@SuppressWarnings("unchecked")
	private static JobInputVertex createModelsInput(JobGraph jobGraph, String pointsPath, int numSubTasks, TypeSerializerFactory<?> serializer) {
		CsvInputFormat modelsInFormat = new CsvInputFormat(' ', LongValue.class, LongValue.class, LongValue.class, LongValue.class);
		JobInputVertex modelsInput = JobGraphUtils.createInput(modelsInFormat, pointsPath, "Input[Models]", jobGraph, numSubTasks, numSubTasks);

		{
			TaskConfig taskConfig = new TaskConfig(modelsInput.getConfiguration());
			taskConfig.addOutputShipStrategy(ShipStrategyType.BROADCAST);
			taskConfig.setOutputSerializer(serializer);
		}

		return modelsInput;
	}

	private static JobTaskVertex createMapper(JobGraph jobGraph, int numSubTasks, TypeSerializerFactory<?> serializer) {
		JobTaskVertex pointsInput = JobGraphUtils.createTask(RegularPactTask.class, "Map[DotProducts]", jobGraph, numSubTasks, numSubTasks);

		{
			TaskConfig taskConfig = new TaskConfig(pointsInput.getConfiguration());

			taskConfig.setStubWrapper(new UserCodeClassWrapper<DotProducts>(DotProducts.class));
			taskConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			taskConfig.setOutputSerializer(serializer);
			taskConfig.setDriver(MapDriver.class);
			taskConfig.setDriverStrategy(DriverStrategy.MAP);

			taskConfig.addInputToGroup(0);
			taskConfig.setInputLocalStrategy(0, LocalStrategy.NONE);
			taskConfig.setInputSerializer(serializer, 0);

			taskConfig.setBroadcastInputName("models", 0);
			taskConfig.addBroadcastInputToGroup(0);
			taskConfig.setBroadcastInputSerializer(serializer, 0);
		}

		return pointsInput;
	}

	private static JobOutputVertex createOutput(JobGraph jobGraph, String resultPath, int numSubTasks, TypeSerializerFactory<?> serializer) {
		JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "Output", numSubTasks, numSubTasks);

		{
			TaskConfig taskConfig = new TaskConfig(output.getConfiguration());
			taskConfig.addInputToGroup(0);
			taskConfig.setInputSerializer(serializer, 0);

			@SuppressWarnings("unchecked")
			CsvOutputFormat outFormat = new CsvOutputFormat("\n", " ", LongValue.class, LongValue.class, LongValue.class);
			outFormat.setOutputFilePath(new Path(resultPath));
			
			taskConfig.setStubWrapper(new UserCodeObjectWrapper<CsvOutputFormat>(outFormat));
		}

		return output;
	}

	// -------------------------------------------------------------------------------------------------------------
	// Unified solution set and workset tail update
	// -------------------------------------------------------------------------------------------------------------

	private JobGraph createJobGraphV1(String pointsPath, String centersPath, String resultPath, int numSubTasks) throws JobGraphDefinitionException {

		// -- init -------------------------------------------------------------------------------------------------
		final TypeSerializerFactory<?> serializer = RecordSerializerFactory.get();

		JobGraph jobGraph = new JobGraph("Distance Builder");

		// -- vertices ---------------------------------------------------------------------------------------------
		JobInputVertex points = createPointsInput(jobGraph, pointsPath, numSubTasks, serializer);
		JobInputVertex models = createModelsInput(jobGraph, centersPath, numSubTasks, serializer);
		JobTaskVertex mapper = createMapper(jobGraph, numSubTasks, serializer);
		JobOutputVertex output = createOutput(jobGraph, resultPath, numSubTasks, serializer);

		// -- edges ------------------------------------------------------------------------------------------------
		JobGraphUtils.connect(points, mapper, ChannelType.NETWORK, DistributionPattern.POINTWISE);
		JobGraphUtils.connect(models, mapper, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		JobGraphUtils.connect(mapper, output, ChannelType.NETWORK, DistributionPattern.POINTWISE);

		// -- instance sharing -------------------------------------------------------------------------------------
		points.setVertexToShareInstancesWith(output);
		models.setVertexToShareInstancesWith(output);
		mapper.setVertexToShareInstancesWith(output);

		return jobGraph;
	}
}
