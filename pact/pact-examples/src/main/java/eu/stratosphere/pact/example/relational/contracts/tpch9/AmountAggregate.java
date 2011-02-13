package eu.stratosphere.pact.example.relational.contracts.tpch9;


import java.util.Iterator;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.*;
import eu.stratosphere.pact.example.relational.types.tpch9.*;


public class AmountAggregate extends ReduceStub<StringIntPair, PactString, StringIntPair, PactString> {
	
	/**
	 * Aggregate "amount":
	 * 
	 * sum(amount)
	 * GROUP BY nation, year
	 * 
	 * Output Schema:
	 *  Key: (nation, year)
	 *  Value: amount
	 *
	 */
	
	@Override
	public void reduce(StringIntPair key, Iterator<PactString> values, Collector<StringIntPair, PactString> out) {

		float amount = 0;

		PactString value = null;
		while (values.hasNext()) {
			value = values.next();
			amount += Float.parseFloat(value.toString());
		}

		if (value != null) {
			PactString outValue = new PactString("" + amount);
			out.collect(key, outValue);
		}

	}

	/**
	 * Creates partial sums of "amount" for each data batch:
	 */
	@Override
	public void combine(StringIntPair key, Iterator<PactString> values, Collector<StringIntPair, PactString> out) {

		float amount = 0;

		PactString value = null;
		while (values.hasNext()) {
			value = values.next();
			amount += Float.parseFloat(value.toString());
		}

		if (value != null) {
			PactString outValue = new PactString("" + amount);
			out.collect(key, outValue);
		}

	}

}
