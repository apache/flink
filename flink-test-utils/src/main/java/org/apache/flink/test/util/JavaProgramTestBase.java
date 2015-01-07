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

package org.apache.flink.test.util;

import java.util.Comparator;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.apache.flink.api.java.tuple.Tuple;


public abstract class JavaProgramTestBase extends AbstractTestBase {

	private static final int DEFAULT_DEGREE_OF_PARALLELISM = 4;
	
	private JobExecutionResult latestExecutionResult;
	
	private int degreeOfParallelism = DEFAULT_DEGREE_OF_PARALLELISM;
	
	private boolean isCollectionExecution;
	
	
	public JavaProgramTestBase() {
		this(new Configuration());
	}
	
	public JavaProgramTestBase(Configuration config) {
		super(config);
		setTaskManagerNumSlots(degreeOfParallelism);
	}
	
	
	public void setDegreeOfParallelism(int degreeOfParallelism) {
		this.degreeOfParallelism = degreeOfParallelism;
		setTaskManagerNumSlots(degreeOfParallelism);
	}
	
	public int getDegreeOfParallelism() {
		return isCollectionExecution ? 1 : degreeOfParallelism;
	}
	
	public JobExecutionResult getLatestExecutionResult() {
		return this.latestExecutionResult;
	}
	
	public boolean isCollectionExecution() {
		return isCollectionExecution;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Methods to create the test program and for pre- and post- test work
	// --------------------------------------------------------------------------------------------

	protected abstract void testProgram() throws Exception;
	

	protected void preSubmit() throws Exception {}
	
	protected void postSubmit() throws Exception {}
	
	protected boolean skipCollectionExecution() {
		return false;
	};

	// --------------------------------------------------------------------------------------------
	//  Test entry point
	// --------------------------------------------------------------------------------------------

	@Test
	public void testJobWithObjectReuse() throws Exception {
		isCollectionExecution = false;
		
		startCluster();
		try {
			// pre-submit
			try {
				preSubmit();
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				Assert.fail("Pre-submit work caused an error: " + e.getMessage());
			}
			
			// prepare the test environment
			TestEnvironment env = new TestEnvironment(this.executor, this.degreeOfParallelism);
			env.getConfig().enableObjectReuse();
			env.setAsContext();
			
			// call the test program
			try {
				testProgram();
				this.latestExecutionResult = env.latestResult;
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				Assert.fail("Error while calling the test program: " + e.getMessage());
			}
			
			Assert.assertNotNull("The test program never triggered an execution.", this.latestExecutionResult);
			
			// post-submit
			try {
				postSubmit();
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				Assert.fail("Post-submit work caused an error: " + e.getMessage());
			}
		} finally {
			stopCluster();
		}
	}

	@Test
	public void testJobWithoutObjectReuse() throws Exception {
		isCollectionExecution = false;

		startCluster();
		try {
			// pre-submit
			try {
				preSubmit();
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				Assert.fail("Pre-submit work caused an error: " + e.getMessage());
			}

			// prepare the test environment
			TestEnvironment env = new TestEnvironment(this.executor, this.degreeOfParallelism);
			env.getConfig().disableObjectReuse();
			env.setAsContext();

			// call the test program
			try {
				testProgram();
				this.latestExecutionResult = env.latestResult;
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				Assert.fail("Error while calling the test program: " + e.getMessage());
			}

			Assert.assertNotNull("The test program never triggered an execution.", this.latestExecutionResult);

			// post-submit
			try {
				postSubmit();
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				Assert.fail("Post-submit work caused an error: " + e.getMessage());
			}
		} finally {
			stopCluster();
		}
	}
	
	@Test
	public void testJobCollectionExecution() throws Exception {
		
		// check if collection execution should be skipped.
		if(this.skipCollectionExecution()) {
			return;
		}
		
		isCollectionExecution = true;
		
		// pre-submit
		try {
			preSubmit();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Pre-submit work caused an error: " + e.getMessage());
		}
		
		// prepare the test environment
		CollectionTestEnvironment env = new CollectionTestEnvironment();
		env.setAsContext();
		
		// call the test program
		try {
			testProgram();
			this.latestExecutionResult = env.latestResult;
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Error while calling the test program: " + e.getMessage());
		}
		
		Assert.assertNotNull("The test program never triggered an execution.", this.latestExecutionResult);
		
		// post-submit
		try {
			postSubmit();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Post-submit work caused an error: " + e.getMessage());
		}
	}
	
	public static class TupleComparator<T extends Tuple> implements Comparator<T> {

		@SuppressWarnings("unchecked")
		@Override
		public int compare(T o1, T o2) {
			int a1 = o1.getArity();
			int a2 = o2.getArity();
			
			if (a1 < a2) {
				return -1;
			} else if (a2 < a1) {
				return 1;
			} else {
				for (int i = 0; i < a1; i++) {
					Object obj1 = o1.getField(i);
					Object obj2 = o2.getField(i);
					
					if (!(obj1 instanceof Comparable && obj2 instanceof Comparable)) {
						Assert.fail("Cannot compare tuple fields");
					}
					
					int cmp = ((Comparable<Object>) obj1).compareTo((Comparable<Object>) obj2);
					if (cmp != 0) {
						return cmp;
					}
				}
				
				return 0;
			}
		}
	}
}
