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

package org.apache.flink.api.java.lib;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DataSetUtilTest {

	// TUPLE DATA
	
	private final List<Tuple5<Integer, Long, String, Long, Integer>> emptyTupleData = 
			new ArrayList<Tuple5<Integer, Long, String, Long, Integer>>();
	
	private final TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>> tupleTypeInfo = new 
			TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>>(
					BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.LONG_TYPE_INFO,
					BasicTypeInfo.STRING_TYPE_INFO,
					BasicTypeInfo.LONG_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO
			);
	
	@Test
	public void testSingleFieldExtraction() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			DataSetUtil.extractSingleField(tupleDs, 0, Integer.class);
		} catch(Exception e) {
			Assert.fail();
		}
		
		// should not work: index out of bounds of input tuple
		try {
			DataSetUtil.extractSingleField(tupleDs, 5, Integer.class);
			Assert.fail();
		} catch(IndexOutOfBoundsException ioobe) {
			// we're good here
		} catch(Exception e) {
			Assert.fail();
		}

		// should not work: index out of bounds of input tuple
		try {
			DataSetUtil.extractSingleField(tupleDs, -1, Integer.class);
			Assert.fail();
		} catch(IndexOutOfBoundsException ioobe) {
			// we're good here
		} catch(Exception e) {
			Assert.fail();
		}

		// should not work: wrong output class type
		try {
			DataSetUtil.extractSingleField(tupleDs, 0, Long.class);
			Assert.fail();
		} catch(IllegalArgumentException iae) {
			// we're good here
		} catch(Exception e) {
			Assert.fail();
		}
		
	}	
	
}
