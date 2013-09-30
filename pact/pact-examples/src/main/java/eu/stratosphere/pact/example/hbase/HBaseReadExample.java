package eu.stratosphere.pact.example.hbase;

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

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.io.HBaseKey;
import eu.stratosphere.pact.common.io.HBaseResult;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TableInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

/**
* Implements a word count which takes the input file and counts the number of
* the occurrences of each word in the file.
*/
public class HBaseReadExample implements PlanAssembler, PlanAssemblerDescription {
	
	public static class MyTableInputFormat extends  TableInputFormat {
		
		private final byte[] META_FAMILY = "meta".getBytes();
		
		private final byte[] USER_COLUMN = "user".getBytes();
		
		private final byte[] TIMESTAMP_COLUMN = "timestamp".getBytes();
		
		private final byte[] TEXT_FAMILY = "text".getBytes();
		
		private final byte[] TWEET_COLUMN = "tweet".getBytes();
		
		public MyTableInputFormat() {
			super();
			
		}
		
		@Override
		protected HTable createTable(Configuration parameters) {
			return super.createTable(parameters);
		}
		
		@Override
		protected Scan createScanner(Configuration parameters) {
			Scan scan = new Scan ();
			scan.addColumn (META_FAMILY, USER_COLUMN);
			scan.addColumn (META_FAMILY, TIMESTAMP_COLUMN);
			scan.addColumn (TEXT_FAMILY, TWEET_COLUMN);
			return scan;
		}
		
		PactString row_string = new PactString();
		PactString user_string = new PactString();
		PactString timestamp_string = new PactString();
		PactString tweet_string = new PactString();
		
		@Override
		public void mapResultToPactRecord(PactRecord record, HBaseKey key,
				HBaseResult result) {
			Result res = result.getResult();
			res.getRow();
			record.setField(0, toString(row_string, res.getRow()));
			record.setField(1, toString (user_string, res.getValue(META_FAMILY, USER_COLUMN)));
			record.setField(2, toString (timestamp_string, res.getValue(META_FAMILY, TIMESTAMP_COLUMN)));
			record.setField(3, toString (tweet_string, res.getValue(TEXT_FAMILY, TWEET_COLUMN)));
		}
		
		private final PactString toString (PactString string, byte[] bytes) {
			string.setValueAscii(bytes, 0, bytes.length);
			return string;
		}
		
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int numSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String output    = (args.length > 1 ? args[1] : "");

		GenericDataSource<TableInputFormat> source = new GenericDataSource<TableInputFormat>(new MyTableInputFormat(), "HBase Input");
		source.setParameter(TableInputFormat.INPUT_TABLE, "twitter");
		source.setParameter(TableInputFormat.CONFIG_LOCATION, "/etc/hbase/conf/hbase-site.xml");
		FileDataSink out = new FileDataSink(new RecordOutputFormat(), output, source, "HBase String dump");
		RecordOutputFormat.configureRecordFormat(out)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactString.class, 0)
			.field(PactString.class, 1)
			.field(PactString.class, 2)
			.field(PactString.class, 3);
		
		Plan plan = new Plan(out, "HBase access Example");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks] [input] [output]";
	}
}
