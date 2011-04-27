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

package eu.stratosphere.pact.test.testPrograms.tpch1;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.relational.util.Tuple;

/**
 * Filters the line item tuples according to the filter condition
 * l_shipdate <= date '1998-12-01' - interval '[DELTA]' day (3)
 * TODO: add parametrisation; first version uses a static interval = 90
 * 
 * In prepration of the following reduce step (see {@link GroupByReturnFlag}) the key has to be set to &quot;return flag&quot;
 * 
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 */
public class LineItemFilter extends MapStub<PactInteger, Tuple, PactString, Tuple> {

	// used later on when the interval will be randomized
	// private static final String DATE_CONSTANT = "1998-12-01";

	private static final Logger LOGGER = Logger.getLogger(LineItemFilter.class);
	private static final String DATE_CONSTANT = "1998-09-03";
	
	private static final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	
	
	private final Date constantDate;
	

	public LineItemFilter() {
		try {
			this.constantDate = format.parse(DATE_CONSTANT);
		}
		catch (ParseException e) {
			LOGGER.error("Date constant could not be parsed.", e);
			throw new RuntimeException("Date constant could not be parsed.");
		}
	}

	@Override
	public void map(PactInteger key, Tuple value, Collector<PactString, Tuple> out) {
		if (value != null && value.getNumberOfColumns() >= 11) {
			String shipDateString = value.getStringValueAt(10);
			
			try {
				Date shipDate = format.parse(shipDateString);

				if (shipDate.before(constantDate)) {	
					String returnFlag = value.getStringValueAt(8);
					out.collect(new PactString(returnFlag), value);
				}
			}
			catch (ParseException e) {
				LOGGER.error(e);
			}

		}
	}

}
