/**
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
 */

package eu.stratosphere.pact4s.tests.perf.plainJava;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextDoubleParser;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextIntParser;
import eu.stratosphere.pact.common.util.FieldSet;

public class KMeans implements PlanAssembler, PlanAssemblerDescription
{
	@ConstantFieldsFirst(fields = { 0, 1, 2 })
	@OutCardBounds(lowerBound = 1, upperBound = 1)
	public static class ComputeDistance extends CrossStub
	{
		private final PactInteger cId = new PactInteger();
		private final PactDouble cx = new PactDouble();
		private final PactDouble cy = new PactDouble();
		private final PactDouble px = new PactDouble();
		private final PactDouble py = new PactDouble();
		private final PactDouble dist = new PactDouble();

		@Override
		public void cross(PactRecord dataPoint, PactRecord clusterCenter, Collector<PactRecord> out)
		{
			PactInteger cId = clusterCenter.getField(0, this.cId);
			
			double cx = clusterCenter.getField(1, this.cx).getValue();
			double cy = clusterCenter.getField(2, this.cy).getValue();

			double px = dataPoint.getField(1, this.px).getValue();
			double py = dataPoint.getField(2, this.py).getValue();

			double distance = Math.sqrt(Math.pow(px - cx, 2) + Math.pow(py - cy, 2));
			this.dist.setValue(distance);

			dataPoint.setField(3, cId);
			dataPoint.setField(4, this.dist);

			out.collect(dataPoint);
		}
	}

	@ConstantFields(fields = { 1, 2 })
	@OutCardBounds(lowerBound = 1, upperBound = 1)
	@Combinable
	public static class FindNearestCenter extends ReduceStub
	{
		private final PactInteger cId = new PactInteger();
		private final PactDouble px = new PactDouble();
		private final PactDouble py = new PactDouble();
		private final PactDouble dist = new PactDouble();
		private final PactInteger one = new PactInteger(1);
		private final PactRecord result = new PactRecord(3);
		private final PactRecord nearest = new PactRecord();

		@Override
		public void reduce(Iterator<PactRecord> pointsWithDistance, Collector<PactRecord> out)
		{
			double nearestDistance = Double.MAX_VALUE;
			int nearestClusterId = 0;	

			while (pointsWithDistance.hasNext())
			{
				PactRecord res = pointsWithDistance.next();

				double distance = res.getField(4, this.dist).getValue();

				if (distance < nearestDistance) {
					nearestDistance = distance;
					nearestClusterId = res.getField(3, this.cId).getValue();
					res.getFieldInto(1, this.px);
					res.getFieldInto(2, this.py);
				}
			}
			
			this.cId.setValue(nearestClusterId);
			this.result.setField(0, this.cId);
			this.result.setField(1, this.px);
			this.result.setField(2, this.py);
			this.result.setField(3, this.one);

			out.collect(this.result);
		}

		@Override
		public void combine(Iterator<PactRecord> pointsWithDistance, Collector<PactRecord> out)
		{
			double nearestDistance = Double.MAX_VALUE;

			while (pointsWithDistance.hasNext())
			{
				PactRecord res = pointsWithDistance.next();
				
				double distance = res.getField(4, this.dist).getValue();

				if (distance < nearestDistance) {
					nearestDistance = distance;
					res.copyTo(this.nearest);
				}
			}

			out.collect(this.nearest);
		}
	}

	@ConstantFields(fields = { 0 })
	@OutCardBounds(lowerBound = 1, upperBound = 1)
	@Combinable
	public static class RecomputeClusterCenter extends ReduceStub
	{
		private final PactDouble x = new PactDouble();
		private final PactDouble y = new PactDouble();
		private final PactInteger count = new PactInteger();

		@Override
		public void reduce(Iterator<PactRecord> dataPoints, Collector<PactRecord> out)
		{
			PactRecord next = null;
			double sumX = 0, sumY = 0;
			int count = 0;

			while (dataPoints.hasNext())
			{
				next = dataPoints.next();

				sumX += next.getField(1, this.x).getValue();
				sumY += next.getField(2, this.y).getValue();
				count += next.getField(3, this.count).getValue();
			}
			
			this.x.setValue(Math.round((sumX / count) * 100.0) / 100.0);
			this.y.setValue(Math.round((sumY / count) * 100.0) / 100.0);

			next.setField(1, this.x);
			next.setField(2, this.y);
			next.setNull(3);

			out.collect(next);
		}

		@Override
		public void combine(Iterator<PactRecord> dataPoints, Collector<PactRecord> out)
		{
			PactRecord next = null;
			double sumX = 0, sumY = 0;
			int count = 0;

			while (dataPoints.hasNext())
			{
				next = dataPoints.next();

				sumX += next.getField(1, this.x).getValue();
				sumY += next.getField(2, this.y).getValue();
				count += next.getField(3, this.count).getValue();
			}

			this.x.setValue(sumX);
			this.y.setValue(sumY);
			this.count.setValue(count);
			
			next.setField(1, this.x);
			next.setField(2, this.y);
			next.setField(3, this.count);

			out.collect(next);
		}
	}

	@Override
	public Plan getPlan(String... args)
	{
		int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataPointInput = (args.length > 1 ? args[1] : "");
		String clusterInput = (args.length > 2 ? args[2] : "");
		String output = (args.length > 3 ? args[3] : "");

		FileDataSource dataPoints = new FileDataSource(RecordInputFormat.class, dataPointInput, "Data Points");
		RecordInputFormat.configureRecordFormat(dataPoints)
			.recordDelimiter('\n').fieldDelimiter('|')
			.field(DecimalTextIntParser.class, 0)
			.field(DecimalTextDoubleParser.class, 1)
			.field(DecimalTextDoubleParser.class, 2);
		dataPoints.getCompilerHints().setUniqueField(new FieldSet(0));

		FileDataSource clusterPoints = new FileDataSource(RecordInputFormat.class, clusterInput, "Centers");
		RecordInputFormat.configureRecordFormat(clusterPoints)
			.recordDelimiter('\n').fieldDelimiter('|')
			.field(DecimalTextIntParser.class, 0)
			.field(DecimalTextDoubleParser.class, 1)
			.field(DecimalTextDoubleParser.class, 2);
		clusterPoints.setDegreeOfParallelism(1);
		clusterPoints.getCompilerHints().setUniqueField(new FieldSet(0));

		CrossContract computeDistance = CrossContract.builder(ComputeDistance.class)
			.input1(dataPoints)
			.input2(clusterPoints)
			.name("Compute Distances")
			.build();
		computeDistance.getCompilerHints().setAvgBytesPerRecord(48);

		ReduceContract findNearestClusterCenters = new ReduceContract.Builder(FindNearestCenter.class,
			PactInteger.class, 0)
			.input(computeDistance)
			.name("Find Nearest Centers")
			.build();
		findNearestClusterCenters.getCompilerHints().setAvgBytesPerRecord(40);

		ReduceContract recomputeClusterCenter = new ReduceContract.Builder(RecomputeClusterCenter.class,
			PactInteger.class, 0)
			.input(findNearestClusterCenters)
			.name("Recompute Center Positions")
			.build();
		recomputeClusterCenter.getCompilerHints().setAvgBytesPerRecord(36);

		FileDataSink newClusterPoints = new FileDataSink(RecordOutputFormat.class, output, recomputeClusterCenter, "New Center Positions");
		RecordOutputFormat.configureRecordFormat(newClusterPoints)
			.recordDelimiter('\n').fieldDelimiter('|').lenient(true)
			.field(PactInteger.class, 0)
			.field(PactDouble.class, 1)
			.field(PactDouble.class, 2);

		Plan plan = new Plan(newClusterPoints, "KMeans Iteration");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks] [dataPoints] [clusterCenters] [output]";
	}
}
