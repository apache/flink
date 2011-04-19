/***********************************************************************************************************************
 *
 * Copyright (C) 2011 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.pact.test.testPrograms.tpch9;


import java.util.Iterator;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.*;

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
