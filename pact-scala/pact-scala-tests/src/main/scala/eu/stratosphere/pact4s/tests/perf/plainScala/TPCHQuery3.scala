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

package eu.stratosphere.pact4s.tests.perf.plainScala

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
import eu.stratosphere.pact.common.`type`.PactRecord;
import eu.stratosphere.pact.common.`type`.base.PactDouble;
import eu.stratosphere.pact.common.`type`.base.PactInteger;
import eu.stratosphere.pact.common.`type`.base.PactLong;
import eu.stratosphere.pact.common.`type`.base.PactString;
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextDoubleParser;
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextIntParser;
import eu.stratosphere.pact.common.`type`.base.parser.DecimalTextLongParser;
import eu.stratosphere.pact.common.`type`.base.parser.VarLengthStringParser;
import eu.stratosphere.pact.common.util.FieldSet;

class TPCHQuery3 extends PlanAssembler with PlanAssemblerDescription {

  import TPCHQuery3._

  override def getPlan(args: String*): Plan = {
    
    val numSubTasks   = if (args.length > 0) args(0).toInt else 1
    val ordersPath    = if (args.length > 1) args(1) else ""
    val lineItemsPath = if (args.length > 2) args(2) else ""
    val output        = if (args.length > 3) args(3) else ""


		val orders = new FileDataSource(classOf[RecordInputFormat], ordersPath, "Orders")
		RecordInputFormat.configureRecordFormat(orders)
			.recordDelimiter('\n').fieldDelimiter('|')
			.field(classOf[DecimalTextLongParser], 0) // order id
			.field(classOf[DecimalTextIntParser], 7) // ship prio
			.field(classOf[VarLengthStringParser], 2) // order status
			.field(classOf[VarLengthStringParser], 4) // order date
			.field(classOf[VarLengthStringParser], 5) // order prio
		orders.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(0), 1)
		orders.getCompilerHints().setAvgBytesPerRecord(16)
		orders.getCompilerHints().setUniqueField(new FieldSet(0))

		val lineitems = new FileDataSource(classOf[RecordInputFormat], lineItemsPath, "LineItems")
		RecordInputFormat.configureRecordFormat(lineitems)
			.recordDelimiter('\n').fieldDelimiter('|')
			.field(classOf[DecimalTextLongParser], 0) // order id
			.field(classOf[DecimalTextDoubleParser], 5) // extended price
		lineitems.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(0), 4)
		lineitems.getCompilerHints().setAvgBytesPerRecord(20)

		val filterO = MapContract.builder(classOf[FilterO])
			.input(orders).name("FilterO").build()
		filterO.setParameter(YEAR_FILTER, 1993)
		filterO.setParameter(PRIO_FILTER, "5")
		filterO.getCompilerHints().setAvgBytesPerRecord(16)
		filterO.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.05f)
		filterO.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(0), 1)

		val joinLiO = MatchContract.builder(classOf[JoinLiO], classOf[PactLong], 0, 0)
			.input1(filterO).input2(lineitems).name("JoinLiO").build()
		joinLiO.getCompilerHints().setAvgBytesPerRecord(24)
		joinLiO.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(Array(0, 1)), 4)

		val aggLiO = ReduceContract.builder(classOf[AggLiO])
			.keyField(classOf[PactLong], 0).keyField(classOf[PactString], 1)
			.input(joinLiO).name("AggLio").build()
		aggLiO.getCompilerHints().setAvgBytesPerRecord(30)
		aggLiO.getCompilerHints().setAvgRecordsEmittedPerStubCall(1.0f)
		aggLiO.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(Array(0, 1)), 1)

		val result = new FileDataSink(classOf[RecordOutputFormat], output, aggLiO, "Output")
		RecordOutputFormat.configureRecordFormat(result).lenient(true)
			.recordDelimiter('\n').fieldDelimiter('|')
			.field(classOf[PactLong], 0)
			.field(classOf[PactInteger], 1)
			.field(classOf[PactDouble], 5)

		val plan = new Plan(result, "TPCH Q3")
		plan.setDefaultParallelism(numSubTasks)
		plan
  }

  override def getDescription() = "Parameters: [numSubStasks], [orders], [lineItems], [output]"
}

object TPCHQuery3 {

  val YEAR_FILTER = "parameter.YEAR_FILTER";
  val PRIO_FILTER = "parameter.PRIO_FILTER";

  @ConstantFields(fields = Array(0, 1))
  @OutCardBounds(upperBound = 1, lowerBound = 0)
  class FilterO extends MapStub {

    private var yearFilter: Int = _
    private var prioFilter: String = _

    private val status = new PactString();
    private val date = new PactString();
    private val priority = new PactString();

    override def open(parameters: Configuration) {
      this.yearFilter = parameters.getInteger(YEAR_FILTER, 1990);
      this.prioFilter = parameters.getString(PRIO_FILTER, "0");
    }

    override def map(record: PactRecord, out: Collector[PactRecord]) {

      val status = record.getField(2, this.status).getValue()
      if (status.equals("F")) {

        val year = Integer.parseInt(record.getField(3, this.date).getValue().substring(0, 4))
        if (year > this.yearFilter) {

          val priority = record.getField(4, this.priority).getValue()
          if (priority.startsWith(this.prioFilter)) {

            record.setNull(2)
            record.setNull(3)
            record.setNull(4)

            out.collect(record)
          }
        }
      }
    }
  }

  @ConstantFieldsFirst(fields = Array(0, 1))
  @OutCardBounds(lowerBound = 1, upperBound = 1)
  class JoinLiO extends MatchStub {

    private val extendedPrice = new PactDouble()

    override def `match`(order: PactRecord, lineItem: PactRecord, out: Collector[PactRecord]) {

      order.setField(2, lineItem.getField(1, this.extendedPrice))
      out.collect(order)
    }
  }

  @ConstantFields(fields = Array(0, 1))
  @OutCardBounds(upperBound = 1, lowerBound = 1)
  @Combinable
  class AggLiO extends ReduceStub {

    private val revenue = new PactDouble()

    override def reduce(orders: Iterator[PactRecord], out: Collector[PactRecord]) {

      var next: PactRecord = null
      var revenue = 0d

      while (orders.hasNext()) {
        next = orders.next();
        revenue += next.getField(2, this.revenue).getValue()
      }

      this.revenue.setValue(revenue)
      next.setField(2, this.revenue)

      out.collect(next)
    }

    override def combine(orders: Iterator[PactRecord], out: Collector[PactRecord]) {
      reduce(orders, out)
    }
  }
}