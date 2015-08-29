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
package org.apache.flink.hcatalog.java;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.hcatalog.HCatOutputFormatBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/*
 * A OutputFormat to write to HCatalog tables
 * The outputFormat supports write to table and partitions through partitionValue map.
 *
 * Flink {@link org.apache.flink.api.java.tuple.Tuple} and {@link HCatRecord} are accepted
 * as data source. If user want to write other type of data to HCatalog, a preceding Map function
 * is required to convert the user data type to {@link HCatRecord}
 *
 * The constructor provides type checking if the user also pass a typeinfo object for the tuple type
 */
public class HCatOutputFormat<T> extends HCatOutputFormatBase<T> {


	/**
	 * Create HCatOutputFormat with default conf
	 * @param database The database schema for the hcatalog table
	 * @param table
	 * @param partitionValues Map of partition values, if null, whole table is used
	 * @throws IOException
	 */
	public HCatOutputFormat(String database, String table, Map<String, String> partitionValues
							) throws IOException
	{
		super(database, table, partitionValues);
	}

	/**
	 * Create HCatOutputFormat with given conf
	 * @param database The database schema for the hcatalog table
	 * @param table
	 * @param partitionValues Map of partition values, if null, whole table is used
	 * @throws IOException
	 */
	public HCatOutputFormat(String database, String table, Map<String, String> partitionValues,
							Configuration conf) throws IOException
	{
		super(database, table, partitionValues, conf);
	}

	/**
	 * Create HCatOutputFormat with type checking
	 * @param database
	 * @param table
	 * @param partitionValues
	 * @param conf
	 * @param typeInfo a typeInfo to be matched by the HCatSchema
	 * @throws IOException
	 */
	public HCatOutputFormat(String database, String table, Map<String, String> partitionValues,
							Configuration conf, TypeInformation<T> typeInfo) throws IOException
	{
		super(database, table, partitionValues, conf);
		if(!typeInfo.equals(this.reqType))
		{
			throw new IOException("tuple has different types from the table's columns");
		}

	}

	@Override
	protected HCatRecord TupleToHCatRecord(T record) throws IOException{
		if(record instanceof  Tuple) {
			Tuple tuple = (Tuple) record;
			if (tuple.getArity() != this.reqType.getArity()) {
				throw new IOException("tuple has different arity from the table's column numbers");
			}
			List<Object> fieldList = new ArrayList<>();
			for (int i = 0; i < this.reqType.getArity(); i++) {
				Object o = tuple.getField(i);

				if (((TupleTypeInfo<Tuple>) reqType).getTypeAt(i).getTypeClass().isInstance(o)) {
					fieldList.add(tuple.getField(i));
				} else {
					throw new IOException("field has different type from required");
				}

			}
			return new DefaultHCatRecord(fieldList);
		}
		else if(record instanceof HCatRecord){
			//assume the user has done the conversion in previous map step
			return (HCatRecord)record;
		}
		else {
			throw new IOException("the record should be either Tuple or HCatRecord");
		}
	}

	@Override
	protected int getMaxFlinkTupleSize() {
		return Tuple.MAX_ARITY;
	}
}
