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

package org.apache.flink.test.broadcastvars;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.common.typeutils.record.RecordSerializerFactory;
import org.apache.flink.api.java.record.functions.MapFunction;
import org.apache.flink.api.java.record.io.CsvInputFormat;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputFormatVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.OutputFormatVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.CollectorMapDriver;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.RegularPactTask;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.test.iterative.nephele.JobGraphUtils;
import org.apache.flink.test.util.RecordAPITestBase;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.junit.Assert;

@SuppressWarnings("deprecation")
public class BroadcastVarsNepheleITCase extends RecordAPITestBase {

	private static final long SEED_POINTS = 0xBADC0FFEEBEEFL;

	private static final long SEED_MODELS = 0x39134230AFF32L;

	private static final int NUM_POINTS = 10000;

	private static final int NUM_MODELS = 42;

	private static final int NUM_FEATURES = 3;

	private static final int parallelism = 4;

	protected String pointsPath;

	protected String modelsPath;

	protected String resultPath;

	public BroadcastVarsNepheleITCase(){
		setTaskManagerNumSlots(parallelism);
	}
	

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
		return createJobGraphV1(this.pointsPath, this.modelsPath, this.resultPath, parallelism);
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
			List<Record> shared = this.getRuntimeContext().getBroadcastVariable("models");
			this.models = new ArrayList<Record>(shared.size());
			synchronized (shared) {
				for (Record r : shared) {
					this.models.add(r.createCopy());
				}
			}
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
	private static InputFormatVertex createPointsInput(JobGraph jobGraph, String pointsPath, int numSubTasks, TypeSerializerFactory<?> serializer) {
		CsvInputFormat pointsInFormat = new CsvInputFormat(' ', LongValue.class, LongValue.class, LongValue.class, LongValue.class);
		InputFormatVertex pointsInput = JobGraphUtils.createInput(pointsInFormat, pointsPath, "Input[Points]", jobGraph, numSubTasks);

		{
			TaskConfig taskConfig = new TaskConfig(pointsInput.getConfiguration());
			taskConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			taskConfig.setOutputSerializer(serializer);
		}

		return pointsInput;
	}

	@SuppressWarnings("unchecked")
	private static InputFormatVertex createModelsInput(JobGraph jobGraph, String pointsPath, int numSubTasks, TypeSerializerFactory<?> serializer) {
		CsvInputFormat modelsInFormat = new CsvInputFormat(' ', LongValue.class, LongValue.class, LongValue.class, LongValue.class);
		InputFormatVertex modelsInput = JobGraphUtils.createInput(modelsInFormat, pointsPath, "Input[Models]", jobGraph, numSubTasks);

		{
			TaskConfig taskConfig = new TaskConfig(modelsInput.getConfiguration());
			taskConfig.addOutputShipStrategy(ShipStrategyType.BROADCAST);
			taskConfig.setOutputSerializer(serializer);
		}

		return modelsInput;
	}

	private static AbstractJobVertex createMapper(JobGraph jobGraph, int numSubTasks, TypeSerializerFactory<?> serializer) {
		AbstractJobVertex pointsInput = JobGraphUtils.createTask(RegularPactTask.class, "Map[DotProducts]", jobGraph, numSubTasks);

		{
			TaskConfig taskConfig = new TaskConfig(pointsInput.getConfiguration());

			taskConfig.setStubWrapper(new UserCodeClassWrapper<DotProducts>(DotProducts.class));
			taskConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			taskConfig.setOutputSerializer(serializer);
			taskConfig.setDriver(CollectorMapDriver.class);
			taskConfig.setDriverStrategy(DriverStrategy.COLLECTOR_MAP);

			taskConfig.addInputToGroup(0);
			taskConfig.setInputLocalStrategy(0, LocalStrategy.NONE);
			taskConfig.setInputSerializer(serializer, 0);

			taskConfig.setBroadcastInputName("models", 0);
			taskConfig.addBroadcastInputToGroup(0);
			taskConfig.setBroadcastInputSerializer(serializer, 0);
		}

		return pointsInput;
	}

	private static OutputFormatVertex createOutput(JobGraph jobGraph, String resultPath, int numSubTasks, TypeSerializerFactory<?> serializer) {
		OutputFormatVertex output = JobGraphUtils.createFileOutput(jobGraph, "Output", numSubTasks);

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

	private JobGraph createJobGraphV1(String pointsPath, String centersPath, String resultPath, int numSubTasks) {

		// -- init -------------------------------------------------------------------------------------------------
		final TypeSerializerFactory<?> serializer = RecordSerializerFactory.get();

		JobGraph jobGraph = new JobGraph("Distance Builder");

		// -- vertices ---------------------------------------------------------------------------------------------
		InputFormatVertex points = createPointsInput(jobGraph, pointsPath, numSubTasks, serializer);
		InputFormatVertex models = createModelsInput(jobGraph, centersPath, numSubTasks, serializer);
		AbstractJobVertex mapper = createMapper(jobGraph, numSubTasks, serializer);
		OutputFormatVertex output = createOutput(jobGraph, resultPath, numSubTasks, serializer);

		// -- edges ------------------------------------------------------------------------------------------------
		JobGraphUtils.connect(points, mapper, DistributionPattern.POINTWISE);
		JobGraphUtils.connect(models, mapper, DistributionPattern.ALL_TO_ALL);
		JobGraphUtils.connect(mapper, output, DistributionPattern.POINTWISE);

		// -- instance sharing -------------------------------------------------------------------------------------
		
		SlotSharingGroup sharing = new SlotSharingGroup();
		
		points.setSlotSharingGroup(sharing);
		models.setSlotSharingGroup(sharing);
		mapper.setSlotSharingGroup(sharing);
		output.setSlotSharingGroup(sharing);

		return jobGraph;
	}
}
