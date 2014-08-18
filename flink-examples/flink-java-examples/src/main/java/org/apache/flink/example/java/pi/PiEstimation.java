/**
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

package org.apache.flink.example.java.pi;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FilterFunction;
import org.apache.flink.api.java.functions.MapFunction;
import org.apache.flink.api.java.functions.ReduceFunction;

/** 
 * Estimates the value of Pi using the Monte Carlo method.
 * The area of a circle is Pi * R^2, R being the radius of the circle 
 * The area of a square is 4 * R^2, where the length of the square's edge is 2*R.
 * 
 * Thus Pi = 4 * (area of circle / area of square).
 * 
 * The idea is to find a way to estimate the circle to square area ratio.
 * The Monte Carlo method suggests collecting random points (within the square)
 * ```
 * x = Math.random() * 2 - 1
 * y = Math.random() * 2 - 1
 * ```
 * then counting the number of points that fall within the circle 
 * ```
 * x * x + y * y < 1
 * ```
 */
public class PiEstimation {
	
	public static void main(String[] args) throws Exception {

		//Sets up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		//Sets the degree of parallelism
		int degOfParal = (env.getDegreeOfParallelism() > 0) ? env.getDegreeOfParallelism() : 2;
		
		int n = 100000 * degOfParal;
		
		DataSet<Integer> dataSet = env.generateSequence(0l, n)
				.map(new MapFunction<Long, Integer>() {
					private static final long serialVersionUID = 1L;
					
					//Converts from Long to Integer, explicitly choosing "1" as the returned value. 
					//(Will later be used by the mapper for summation purposes.)
					@Override
					public Integer map(Long value) throws Exception {
						return 1;
					}
				});

		DataSet<Double> count = dataSet
				.filter(new PiFilter())
				.setParallelism(degOfParal)
				.reduce(new PiReducer())
				.map(new PiMapper(n));

		System.out.println("We estimate Pi to be:");
		count.print();

		env.execute();
	}

	//*************************************************************************
	//     USER FUNCTIONS
	//*************************************************************************
	
	// FilterFunction that filters out all Integers smaller than zero.
	
	/** 
	 * PiFilter randomly emits points that fall within a square of edge 2*x = 2*y = 2.
	 * It calculates the distance to the center of a virtually centered circle of radius x = y = 1
	 * If the distance is less than 1, then and only then does it return a value (in this case 1) - later used by PiMapper.
	 */
	public static class PiFilter extends FilterFunction<Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(Integer value) throws Exception{
			double x = Math.random() * 2 - 1;
			double y = Math.random() * 2 - 1;
			return (x * x + y * y) < 1;
		}
	}

	
	/** 
	 * PiReducer takes over the filter. It goes through the selected 1s and returns the sum.
	 */
	public static final class PiReducer extends ReduceFunction<Integer>{
		private static final long serialVersionUID = 1L;

		@Override
		public Integer reduce(Integer value1, Integer value2) throws Exception {
			return value1 + value2;
		}
	}
	
	
	/** 
	 * The PiMapper's role is to apply one final operation on the count thus returning the estimated Pi value.
	 */
	public static final class PiMapper extends MapFunction<Integer,Double> {
		private static final long serialVersionUID = 1L;
		private int n;
		
		public PiMapper(int n) {
			this.n = n;
		}
		
		@Override
		public Double map(Integer intSum) throws Exception {
			return intSum*4.0 / this.n;
		}
	}
	
}