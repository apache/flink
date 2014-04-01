/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.api.java.operator;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.tuple.Tuple5;
import eu.stratosphere.api.java.typeutils.BasicTypeInfo;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;

public class ProjectionOperatorTest {

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
	
	// LONG DATA
	
	private final List<Long> emptyLongData = new ArrayList<Long>();
	
	@Test
	public void testFieldsProjection() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			tupleDs.project(0);
		} catch(Exception e) {
			Assert.fail();
		}
		
		// should not work: too many fields
		try {
			tupleDs.project(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22);
			Assert.fail();
		} catch(IllegalArgumentException iae) {
			// we're good here
		} catch(Exception e) {
			Assert.fail();
		}
		
		// should not work: index out of bounds of input tuple
		try {
			tupleDs.project(0,5,2);
			Assert.fail();
		} catch(IndexOutOfBoundsException ioobe) {
			// we're good here
		} catch(Exception e) {
			Assert.fail();
		}
		
		// should not work: not applied to tuple dataset
		DataSet<Long> longDs = env.fromCollection(emptyLongData, BasicTypeInfo.LONG_TYPE_INFO);
		try {
			longDs.project(0);
			Assert.fail();
		} catch(UnsupportedOperationException uoe) {
			// we're good here
		} catch(Exception e) {
			Assert.fail();
		}
		
	}
	
	@Test
	public void testFieldMaskProjection() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = 
				env.fromCollection(emptyTupleData, tupleTypeInfo);
		
		// should work
		try {
			tupleDs.project("TFTTF");
		} catch(Exception e) {
			Assert.fail();
		}
		
		// should work
		try {
			tupleDs.project("tfftf");
		} catch(Exception e) {
			Assert.fail();
		}
		
		// should work
		try {
			tupleDs.project("01101");
		} catch(Exception e) {
			Assert.fail();
		}
		
		// should work
		try {
			tupleDs.project("101");
		} catch(Exception e) {
			Assert.fail();
		}
		
		// should not work: too many fields
		try {
			tupleDs.project("TTTTTTTTTTTTTTTTTTTTTTT");
			Assert.fail();
		} catch(IllegalArgumentException iae) {
			// we're good here
		} catch(Exception e) {
			Assert.fail();
		}
		
		// should not work: too few fields
		try {
			tupleDs.project("");
			Assert.fail();
		} catch(IllegalArgumentException iae) {
			// we're good here
		} catch(Exception e) {
			Assert.fail();
		}
		
		// should not work: no selected field
		try {
			tupleDs.project("FFFFF");
			Assert.fail();
		} catch(IllegalArgumentException iae) {
			// we're good here
		} catch(Exception e) {
			Assert.fail();
		}
		
		// should not work: illegal character
		try {
			tupleDs.project("TFXFT");
			Assert.fail();
		} catch(IllegalArgumentException iae) {
			// we're good here
		} catch(Exception e) {
			Assert.fail();
		}
		
		// should not work: not applied to tuple dataset
		DataSet<Long> longDs = env.fromCollection(emptyLongData, BasicTypeInfo.LONG_TYPE_INFO);
		try {
			longDs.project("T");
			Assert.fail();
		} catch(UnsupportedOperationException uoe) {
			// we're good here
		} catch(Exception e) {
			Assert.fail();
		}
		
	}
	
	@Test
	public void testFieldFlagsProjection() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = 
				env.fromCollection(emptyTupleData, tupleTypeInfo);
		
		// should work
		try {
			tupleDs.project(true, true, false, true, false);
		} catch(Exception e) {
			Assert.fail();
		}
		
		// should work
		try {
			tupleDs.project(false, true);
		} catch(Exception e) {
			Assert.fail();
		}
		
		// should not work: too many fields
		try {
			tupleDs.project(
					true, true, true, true, true, true, true, true, true, true, 
					true, true, true, true, true, true, true, true, true, true,
					true, true, true);
			
			Assert.fail();
		} catch(IllegalArgumentException iae) {
			// we're good here
		} catch(Exception e) {
			Assert.fail();
		}
		
		// should not work: no selected field
		try {
			tupleDs.project(false, false, false, false, false);
			
			Assert.fail();
		} catch(IllegalArgumentException iae) {
			// we're good here
		} catch(Exception e) {
			Assert.fail();
		}
		
		// should not work: not applied to tuple dataset
		DataSet<Long> longDs = env.fromCollection(emptyLongData, BasicTypeInfo.LONG_TYPE_INFO);
		try {
			longDs.project(false, true);
			Assert.fail();
		} catch(UnsupportedOperationException uoe) {
			// we're good here
		} catch(Exception e) {
			Assert.fail();
		}
		
	}
	
	@Test
	public void testProjectionTypes() {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple5<Integer, Long, String, Long, Integer>> tupleDs = env.fromCollection(emptyTupleData, tupleTypeInfo);

		// should work
		try {
			tupleDs.project(0).types(Integer.class);
		} catch(Exception e) {
			Assert.fail();
		}
		
		// should not work: too few types
		try {
			tupleDs.project(2,1,4).types(String.class, Long.class);
			Assert.fail();
		} catch(IllegalArgumentException iae) {
			// we're good here
		} catch(Exception e) {
			Assert.fail();
		}
		
		// should not work: given types do not match input types
		try {
			tupleDs.project(2,1,4).types(String.class, Long.class, Long.class);
			Assert.fail();
		} catch(IllegalArgumentException iae) {
			// we're good here
		} catch(Exception e) {
			Assert.fail();
		}
		
	}
	
}
