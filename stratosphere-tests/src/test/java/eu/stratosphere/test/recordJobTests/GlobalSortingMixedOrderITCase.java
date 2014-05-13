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

package eu.stratosphere.test.recordJobTests;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.distributions.DataDistribution;
import eu.stratosphere.api.java.record.operators.FileDataSink;
import eu.stratosphere.api.java.record.operators.FileDataSource;
import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.api.common.operators.Ordering;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.test.util.RecordAPITestBase;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Key;

public class GlobalSortingMixedOrderITCase extends RecordAPITestBase {
	
	private static final int NUM_RECORDS = 100000;
	
	private static final int RANGE_I1 = 100;
	private static final int RANGE_I2 = 20;
	private static final int RANGE_I3 = 20;
	
	private String recordsPath;
	private String resultPath;

	private String sortedRecords;



	@Override
	protected void preSubmit() throws Exception {
		
		ArrayList<TripleInt> records = new ArrayList<TripleInt>();
		
		//Generate records
		final Random rnd = new Random(1988);
		final StringBuilder sb = new StringBuilder(NUM_RECORDS * 7);
		
		
		for (int j = 0; j < NUM_RECORDS; j++) {
			TripleInt val = new TripleInt(rnd.nextInt(RANGE_I1), rnd.nextInt(RANGE_I2), rnd.nextInt(RANGE_I3));
			records.add(val);
			sb.append(val);
			sb.append('\n');
		}
		
		
		this.recordsPath = createTempFile("records", sb.toString());
		this.resultPath = getTempDirPath("result");

		// create the sorted result;
		Collections.sort(records);
		
		sb.setLength(0);
		for (TripleInt val : records) {
			sb.append(val);
			sb.append('\n');
		}
		this.sortedRecords = sb.toString();
	}

	@Override
	protected Plan getTestJob() {
		GlobalSort globalSort = new GlobalSort();
		return globalSort.getPlan("4", recordsPath, resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		// Test results
		compareResultsByLinesInMemoryWithStrictOrder(this.sortedRecords, this.resultPath);
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
		public Key<?>[] getBucketBoundary(int bucketNum, int totalNumBuckets) {
			
			final float bucketWidth = ((float) RANGE_I1) / totalNumBuckets;
			int boundVal = (int) ((bucketNum + 1) * bucketWidth);
			if (!this.ascendingI1) {
				boundVal = RANGE_I1 - boundVal;
			}
			
			return new Key[] { new IntValue(boundVal), new IntValue(RANGE_I2), new IntValue(RANGE_I3) };
		}

		@Override
		public int getNumberOfFields() {
			return 3;
		}
		
	}
	
	private static class GlobalSort implements Program {
		
		private static final long serialVersionUID = 1L;

		@Override
		public Plan getPlan(String... args) throws IllegalArgumentException {
			// parse program parameters
			final int numSubtasks     = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
			final String recordsPath = (args.length > 1 ? args[1] : "");
			final String output      = (args.length > 2 ? args[2] : "");
			
			@SuppressWarnings("unchecked")
			FileDataSource source = new FileDataSource(new CsvInputFormat(',', IntValue.class, IntValue.class, IntValue.class), recordsPath);
			
			FileDataSink sink = new FileDataSink(CsvOutputFormat.class, output);
			CsvOutputFormat.configureRecordFormat(sink)
				.recordDelimiter('\n')
				.fieldDelimiter(',')
				.lenient(true)
				.field(IntValue.class, 0)
				.field(IntValue.class, 1)
				.field(IntValue.class, 2);
			
			sink.setGlobalOrder(
				new Ordering(0, IntValue.class, Order.DESCENDING)
					.appendOrdering(1, IntValue.class, Order.ASCENDING)
					.appendOrdering(2, IntValue.class, Order.DESCENDING),
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
	private static final class TripleInt implements Comparable<TripleInt> {
		
		private final int i1, i2, i3;

		
		private TripleInt(int i1, int i2, int i3) {
			this.i1 = i1;
			this.i2 = i2;
			this.i3 = i3;
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
