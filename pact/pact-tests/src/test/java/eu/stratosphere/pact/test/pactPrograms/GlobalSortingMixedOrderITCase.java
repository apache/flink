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

package eu.stratosphere.pact.test.pactPrograms;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.distributions.DataDistribution;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.test.util.TestBase;

@RunWith(Parameterized.class)
public class GlobalSortingMixedOrderITCase extends TestBase {

	private static final Log LOG = LogFactory.getLog(GlobalSortingMixedOrderITCase.class);
	
	private static final int RANGE_I1 = 100;
	private static final int RANGE_I2 = 20;
	private static final int RANGE_I3 = 20;
	
	private String recordsPath = null;
	private String resultPath = null;

	private ArrayList<TripleInt> records;

	public GlobalSortingMixedOrderITCase(Configuration config) {
		super(config);
	}

	@Override
	protected void preSubmit() throws Exception {
		
		this.recordsPath = getFilesystemProvider().getTempDirPath() + "/records";
		this.resultPath = getFilesystemProvider().getTempDirPath() + "/result";
		
		this.records = new ArrayList<TripleInt>();
		
		//Generate records
		final Random rnd = new Random(1988);
		final int numRecordsPerSplit = 1000;
		
		getFilesystemProvider().createDir(this.recordsPath);
		
		final int numSplits = 4;
		for (int i = 0; i < numSplits; i++) {
			StringBuilder sb = new StringBuilder(numSplits*2);
			for (int j = 0; j < numRecordsPerSplit; j++) {
				final TripleInt val = new TripleInt(rnd.nextInt(RANGE_I1), rnd.nextInt(RANGE_I2), rnd.nextInt(RANGE_I3));
				this.records.add(val);
				sb.append(val);
				sb.append('\n');
			}
			getFilesystemProvider().createFile(recordsPath + "/part_" + i + ".txt", sb.toString());
			
			if (LOG.isDebugEnabled())
				LOG.debug("Records Part " + (i + 1) + ":\n>" + sb.toString() + "<");
		}

	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		GlobalSort globalSort = new GlobalSort();
		Plan plan = globalSort.getPlan(
				config.getString("GlobalSortingTest#NoSubtasks", "1"), 
				getFilesystemProvider().getURIPrefix()+recordsPath,
				getFilesystemProvider().getURIPrefix()+resultPath);

		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	@Override
	protected void postSubmit() throws Exception {
		//Construct expected result
		Collections.sort(this.records);
		
		// Test results
		compareResultsByLinesInMemoryStrictOrder(this.records, this.resultPath);
	}
	
	@Override
	public void stopCluster() throws Exception {
		getFilesystemProvider().delete(recordsPath, true);
		getFilesystemProvider().delete(resultPath, true);
		super.stopCluster();
	}
	

	@Parameters
	public static Collection<Object[]> getConfigurations() {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		Configuration config = new Configuration();
		config.setInteger("GlobalSortingTest#NoSubtasks", 4);
		tConfigs.add(config);

		return toParameterList(tConfigs);
	}
	
	public static class TripleIntDistribution implements DataDistribution {
		
		private static final long serialVersionUID = 1L;
		
		private boolean ascendingI1, ascendingI2, ascendingI3;
		
		public TripleIntDistribution(Order orderI1, Order orderI2, Order orderI3) {
			this.ascendingI1 = orderI1 != Order.DESCENDING;
			this.ascendingI2 = orderI2 != Order.DESCENDING;
			this.ascendingI3 = orderI3 != Order.DESCENDING;
		}
		
		public TripleIntDistribution() {}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeBoolean(this.ascendingI1);
			out.writeBoolean(this.ascendingI2);
			out.writeBoolean(this.ascendingI3);
		}

		@Override
		public void read(DataInput in) throws IOException {
			this.ascendingI1 = in.readBoolean();
			this.ascendingI2 = in.readBoolean();
			this.ascendingI3 = in.readBoolean();
		}

		@Override
		public Key[] getBucketBoundary(int bucketNum, int totalNumBuckets) {
			
			final float bucketWidth = ((float) RANGE_I1) / totalNumBuckets;
			int boundVal = (int) ((bucketNum + 1) * bucketWidth);
			if (!this.ascendingI1) {
				boundVal = RANGE_I1 - boundVal;
			}
			
			return new Key[] { new PactInteger(boundVal), new PactInteger(RANGE_I2), new PactInteger(RANGE_I3) };
		}

		@Override
		public int getNumberOfFields() {
			return 3;
		}
		
	}
	
	private static class GlobalSort implements PlanAssembler {
		
		@Override
		public Plan getPlan(String... args) throws IllegalArgumentException {
			// parse program parameters
			final int numSubtasks     = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
			final String recordsPath = (args.length > 1 ? args[1] : "");
			final String output      = (args.length > 2 ? args[2] : "");
			
			@SuppressWarnings("unchecked")
			FileDataSource source = new FileDataSource(new RecordInputFormat(',', PactInteger.class, PactInteger.class, PactInteger.class), recordsPath);
			
			FileDataSink sink = new FileDataSink(RecordOutputFormat.class, output);
			RecordOutputFormat.configureRecordFormat(sink)
				.recordDelimiter('\n')
				.fieldDelimiter(',')
				.lenient(true)
				.field(PactInteger.class, 0)
				.field(PactInteger.class, 1)
				.field(PactInteger.class, 2);
			
			sink.setGlobalOrder(
				new Ordering(0, PactInteger.class, Order.DESCENDING)
					.appendOrdering(1, PactInteger.class, Order.ASCENDING)
					.appendOrdering(2, PactInteger.class, Order.DESCENDING),
				new TripleIntDistribution(Order.DESCENDING, Order.ASCENDING, Order.DESCENDING));
			sink.setInput(source);
			
			Plan p = new Plan(sink);
			p.setDefaultParallelism(numSubtasks);
			return p;
		}
	}
	
	/**
	 * Three integers sorting descending, ascending, descending.
	 */
	static final class TripleInt implements Comparable<TripleInt>
	{
		private final int i1, i2, i3;

		
		TripleInt(int i1, int i2, int i3) {
			this.i1 = i1;
			this.i2 = i2;
			this.i3 = i3;
		}

		public int getI1() {
			return i1;
		}

		public int getI2() {
			return i2;
		}

		public int getI3() {
			return i3;
		}
		
		@Override
		public String toString() {
			StringBuilder bld = new StringBuilder(32);
			bld.append(this.i1);
			bld.append(',');
			bld.append(this.i2);
			bld.append(',');
			bld.append(this.i3);
			return bld.toString();
		}

		@Override
		public int compareTo(TripleInt o) {
			return this.i1 < o.i1 ? 1 : this.i1 > o.i1 ? -1 :
				this.i2 < o.i2 ? -1 : this.i2 > o.i2 ? 1 :
				this.i3 < o.i3 ? 1 : this.i3 > o.i3 ? -1 : 0;
			
		}
	}
}
