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

package org.apache.flink.test.recordJobs.relational.query1Util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.api.java.record.functions.MapFunction;
import org.apache.flink.test.recordJobs.util.Tuple;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filters the line item tuples according to the filter condition
 * l_shipdate <= date '1998-12-01' - interval '[DELTA]' day (3)
 * TODO: add parametrisation; first version uses a static interval = 90
 * 
 * In prepration of the following reduce step (see {@link GroupByReturnFlag}) the key has to be set to &quot;return flag&quot;
 */
@SuppressWarnings("deprecation")
public class LineItemFilter extends MapFunction {

	private static final long serialVersionUID = 1L;
	
	private static final Logger LOG = LoggerFactory.getLogger(LineItemFilter.class);
	private static final String DATE_CONSTANT = "1998-09-03";
	
	private static final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	
	
	private final Date constantDate;
	

	public LineItemFilter() {
		try {
			this.constantDate = format.parse(DATE_CONSTANT);
		}
		catch (ParseException e) {
			LOG.error("Date constant could not be parsed.", e);
			throw new RuntimeException("Date constant could not be parsed.");
		}
	}

	@Override
	public void map(Record record, Collector<Record> out) throws Exception {
		Tuple value = record.getField(1, Tuple.class);
		
		if (value != null && value.getNumberOfColumns() >= 11) {
			String shipDateString = value.getStringValueAt(10);
			
			try {
				Date shipDate = format.parse(shipDateString);

				if (shipDate.before(constantDate)) {	
					String returnFlag = value.getStringValueAt(8);
					
					record.setField(0, new StringValue(returnFlag));
					out.collect(record);
				}
			}
			catch (ParseException e) {
				LOG.warn("ParseException while parsing the shipping date.", e);
			}

		}
	}

}
