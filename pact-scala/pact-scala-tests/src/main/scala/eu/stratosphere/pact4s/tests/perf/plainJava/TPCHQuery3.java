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

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextDoubleParser;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextIntParser;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextLongParser;
import eu.stratosphere.pact.common.type.base.parser.VarLengthStringParser;
import eu.stratosphere.pact.common.util.FieldSet;

public class TPCHQuery3 implements PlanAssembler, PlanAssemblerDescription {

	public static final String YEAR_FILTER = "parameter.YEAR_FILTER";
	public static final String PRIO_FILTER = "parameter.PRIO_FILTER";

	@ConstantFields(fields = { 0, 1 })
	@OutCardBounds(upperBound = 1, lowerBound = 0)
	public static class FilterO extends MapStub
	{
		private String prioFilter;
		private int yearFilter;

		private final PactString status = new PactString();
		private final PactString date = new PactString();
		private final PactString priority = new PactString();

		@Override
		public void open(Configuration parameters) {
			this.yearFilter = parameters.getInteger(YEAR_FILTER, 1990);
			this.prioFilter = parameters.getString(PRIO_FILTER, "0");
		}

		@Override
		public void map(PactRecord record, Collector<PactRecord> out)
		{
			final String status = record.getField(2, this.status).getValue();
			if (status.equals("F"))
			{
				final int year = Integer.parseInt(record.getField(3, this.date).getValue().substring(0, 4));
				if (year > this.yearFilter)
				{
					final String priority = record.getField(4, this.priority).getValue();
					if (priority.startsWith(this.prioFilter))
					{
						record.setNull(2);
						record.setNull(3);
						record.setNull(4);

						out.collect(record);
					}
				}
			}
		}
	}

	@ConstantFieldsFirst(fields = { 0, 1 })
	@OutCardBounds(lowerBound = 1, upperBound = 1)
	public static class JoinLiO extends MatchStub
	{
		private final PactDouble extendedPrice = new PactDouble();

		@Override
		public void match(PactRecord order, PactRecord lineItem, Collector<PactRecord> out)
		{
			order.setField(2, lineItem.getField(1, this.extendedPrice));
			out.collect(order);
		}
	}

	@ConstantFields(fields = { 0, 1 })
	@OutCardBounds(upperBound = 1, lowerBound = 1)
	@Combinable
	public static class AggLiO extends ReduceStub
	{
		private final PactDouble revenue = new PactDouble();

		@Override
		public void reduce(Iterator<PactRecord> orders, Collector<PactRecord> out)
		{
			PactRecord next = null;
			double revenue = 0;

			while (orders.hasNext()) {
				next = orders.next();
				revenue += next.getField(2, this.revenue).getValue();
			}

			this.revenue.setValue(revenue);
			next.setField(2, this.revenue);

			out.collect(next);
		}

		@Override
		public void combine(Iterator<PactRecord> values, Collector<PactRecord> out)
		{
			reduce(values, out);
		}
	}

	@Override
	public Plan getPlan(final String... args)
	{
		final int numSubTasks      = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String ordersPath    = (args.length > 1 ? args[1] : "");
		final String lineItemsPath = (args.length > 2 ? args[2] : "");
		final String output        = (args.length > 3 ? args[3] : "");

		FileDataSource orders = new FileDataSource(RecordInputFormat.class, ordersPath, "Orders");
		RecordInputFormat.configureRecordFormat(orders)
			.recordDelimiter('\n').fieldDelimiter('|')
			.field(DecimalTextLongParser.class, 0) // order id
			.field(DecimalTextIntParser.class, 7) // ship prio
			.field(VarLengthStringParser.class, 2) // order status
			.field(VarLengthStringParser.class, 4) // order date
			.field(VarLengthStringParser.class, 5); // order prio
		orders.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(0), 1);
		orders.getCompilerHints().setAvgBytesPerRecord(16);
		orders.getCompilerHints().setUniqueField(new FieldSet(0));

		FileDataSource lineitems = new FileDataSource(RecordInputFormat.class, lineItemsPath, "LineItems");
		RecordInputFormat.configureRecordFormat(lineitems)
			.recordDelimiter('\n').fieldDelimiter('|')
			.field(DecimalTextLongParser.class, 0) // order id
			.field(DecimalTextDoubleParser.class, 5); // extended price
		lineitems.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(0), 4);
		lineitems.getCompilerHints().setAvgBytesPerRecord(20);

		MapContract filterO = MapContract.builder(FilterO.class)
			.input(orders).name("FilterO").build();
		filterO.setParameter(YEAR_FILTER, 1993);
		filterO.setParameter(PRIO_FILTER, "5");
		filterO.getCompilerHints().setAvgBytesPerRecord(16);
		filterO.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.05f);
		filterO.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(0), 1);

		MatchContract joinLiO = MatchContract.builder(JoinLiO.class, PactLong.class, 0, 0)
			.input1(filterO).input2(lineitems).name("JoinLiO").build();
		joinLiO.getCompilerHints().setAvgBytesPerRecord(24);
		joinLiO.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(new int[] { 0, 1 }), 4);

		ReduceContract aggLiO = ReduceContract.builder(AggLiO.class)
			.keyField(PactLong.class, 0).keyField(PactString.class, 1)
			.input(joinLiO).name("AggLio").build();
		aggLiO.getCompilerHints().setAvgBytesPerRecord(30);
		aggLiO.getCompilerHints().setAvgRecordsEmittedPerStubCall(1.0f);
		aggLiO.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(new int[] { 0, 1 }), 1);

		FileDataSink result = new FileDataSink(RecordOutputFormat.class, output, aggLiO, "Output");
		RecordOutputFormat.configureRecordFormat(result).lenient(true)
			.recordDelimiter('\n').fieldDelimiter('|')
			.field(PactLong.class, 0)
			.field(PactInteger.class, 1)
			.field(PactDouble.class, 5);

		Plan plan = new Plan(result, "TPCH Q3");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks], [orders], [lineItems], [output]";
	}
}
