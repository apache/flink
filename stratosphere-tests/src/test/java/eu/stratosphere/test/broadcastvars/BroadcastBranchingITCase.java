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

import java.util.Collection;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.operators.io.ContractITCaseIOFormats.ContractITCaseOutputFormat;
import eu.stratosphere.test.util.RecordAPITestBase;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

public class BroadcastBranchingITCase extends RecordAPITestBase {

	private static final String SC1_ID_ABC = "1 61 6 29\n2 7 13 10\n3 8 13 27\n";

	private static final String SC2_ID_X = "1 5\n2 3\n3 6";

	private static final String SC3_ID_Y = "1 2\n2 3\n3 7";

	private static final String RESULT = "2 112\n";

	private String sc1Path;
	private String sc2Path;
	private String sc3Path;
	private String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		sc1Path = createTempFile("broadcastBranchingInput/map_id_abc.txt", SC1_ID_ABC);
		sc2Path = createTempFile("broadcastBranchingInput/map_id_x.txt", SC2_ID_X);
		sc3Path = createTempFile("broadcastBranchingInput/map_id_y.txt", SC3_ID_Y);
		resultPath = getTempDirPath("result");
	}

	//              Sc1(id,a,b,c) --
	//                              \
	//    Sc2(id,x) --------         Jn2(id) -- Mp2 -- Sk
	//                      \        /          / <=BC
	//                       Jn1(id) -- Mp1 ----
	//                      /
	//    Sc3(id,y) --------
	@Override
	protected Plan getTestJob() {
		// Sc1 generates M parameters a,b,c for second degree polynomials P(x) = ax^2 + bx + c identified by id
		FileDataSource sc1 = new FileDataSource(new CsvInputFormat(), sc1Path);
		CsvInputFormat.configureRecordFormat(sc1).fieldDelimiter(' ').field(StringValue.class, 0).field(IntValue.class, 1)
				.field(IntValue.class, 2).field(IntValue.class, 3);

		// Sc2 generates N x values to be evaluated with the polynomial identified by id
		FileDataSource sc2 = new FileDataSource(new CsvInputFormat(), sc2Path);
		CsvInputFormat.configureRecordFormat(sc2).fieldDelimiter(' ').field(StringValue.class, 0).field(IntValue.class, 1);

		// Sc3 generates N y values to be evaluated with the polynomial identified by id
		FileDataSource sc3 = new FileDataSource(new CsvInputFormat(), sc3Path);
		CsvInputFormat.configureRecordFormat(sc3).fieldDelimiter(' ').field(StringValue.class, 0).field(IntValue.class, 1);

		// Jn1 matches x and y values on id and emits (id, x, y) triples
		JoinOperator jn1 = JoinOperator.builder(Jn1.class, StringValue.class, 0, 0).input1(sc2).input2(sc3).build();

		// Jn2 matches polynomial and arguments by id, computes p = min(P(x),P(y)) and emits (id, p) tuples
		JoinOperator jn2 = JoinOperator.builder(Jn2.class, StringValue.class, 0, 0).input1(jn1).input2(sc1).build();

		// Mp1 selects (id, x, y) triples where x = y and broadcasts z (=x=y) to Mp2
		MapOperator mp1 = MapOperator.builder(Mp1.class).input(jn1).build();

		// Mp2 filters out all p values which can be divided by z
		MapOperator mp2 = MapOperator.builder(Mp2.class).setBroadcastVariable("z", mp1).input(jn2).build();

		FileDataSink output = new FileDataSink(new ContractITCaseOutputFormat(), resultPath);
		output.setDegreeOfParallelism(1);
		output.setInput(mp2);

		return new Plan(output);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(RESULT, resultPath);
	}

	public static class Jn1 extends JoinFunction {
		private static final long serialVersionUID = 1L;

		@Override
		public void join(Record sc2, Record sc3, Collector<Record> out) throws Exception {
			Record r = new Record(3);
			r.setField(0, sc2.getField(0, StringValue.class));
			r.setField(1, sc2.getField(1, IntValue.class));
			r.setField(2, sc3.getField(1, IntValue.class));
			out.collect(r);
		}
	}

	public static class Jn2 extends JoinFunction {
		private static final long serialVersionUID = 1L;

		private static int p(int x, int a, int b, int c) {
			return a * x * x + b * x + c;
		}

		@Override
		public void join(Record jn1, Record sc1, Collector<Record> out) throws Exception {
			int x = jn1.getField(1, IntValue.class).getValue();
			int y = jn1.getField(2, IntValue.class).getValue();
			int a = sc1.getField(1, IntValue.class).getValue();
			int b = sc1.getField(2, IntValue.class).getValue();
			int c = sc1.getField(3, IntValue.class).getValue();

			int p_x = p(x, a, b, c);
			int p_y = p(y, a, b, c);
			int min = Math.min(p_x, p_y);
			out.collect(new Record(jn1.getField(0, StringValue.class), new IntValue(min)));
		}
	}

	public static class Mp1 extends MapFunction {
		private static final long serialVersionUID = 1L;

		@Override
		public void map(Record jn1, Collector<Record> out) throws Exception {
			if (jn1.getField(1, IntValue.class).getValue() == jn1.getField(2, IntValue.class).getValue()) {
				out.collect(new Record(jn1.getField(0, StringValue.class), jn1.getField(1, IntValue.class)));
			}
		}
	}

	public static class Mp2 extends MapFunction {
		private static final long serialVersionUID = 1L;

		private Collection<Record> zs;

		@Override
		public void open(Configuration parameters) throws Exception {
			this.zs = getRuntimeContext().getBroadcastVariable("z");
		}

		@Override
		public void map(Record jn2, Collector<Record> out) throws Exception {
			int p = jn2.getField(1, IntValue.class).getValue();

			for (Record z : zs) {
				if (z.getField(0, StringValue.class).getValue().equals(jn2.getField(0, StringValue.class).getValue())) {
					if (p % z.getField(1, IntValue.class).getValue() != 0) {
						out.collect(jn2);
					}
				}
			}
		}
	}

}
