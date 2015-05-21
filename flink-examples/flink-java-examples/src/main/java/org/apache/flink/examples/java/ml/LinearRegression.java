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


package org.apache.flink.examples.java.ml;

import java.io.Serializable;
import java.util.Collection;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.examples.java.ml.util.LinearRegressionData;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

/**
 * This example implements a basic Linear Regression  to solve the y = theta0 + theta1*x problem using batch gradient descent algorithm.
 *
 * <p>
 * Linear Regression with BGD(batch gradient descent) algorithm is an iterative clustering algorithm and works as follows:<br>
 * Giving a data set and target set, the BGD try to find out the best parameters for the data set to fit the target set.
 * In each iteration, the algorithm computes the gradient of the cost function and use it to update all the parameters.
 * The algorithm terminates after a fixed number of iterations (as in this implementation)
 * With enough iteration, the algorithm can minimize the cost function and find the best parameters
 * This is the Wikipedia entry for the <a href = "http://en.wikipedia.org/wiki/Linear_regression">Linear regression</a> and <a href = "http://en.wikipedia.org/wiki/Gradient_descent">Gradient descent algorithm</a>.
 * 
 * <p>
 * This implementation works on one-dimensional data. And find the two-dimensional theta.<br>
 * It find the best Theta parameter to fit the target.
 * 
 * <p>
 * Input files are plain text files and must be formatted as follows:
 * <ul>
 * <li>Data points are represented as two double values separated by a blank character. The first one represent the X(the training data) and the second represent the Y(target).
 * Data points are separated by newline characters.<br>
 * For example <code>"-0.02 -0.04\n5.3 10.6\n"</code> gives two data points (x=-0.02, y=-0.04) and (x=5.3, y=10.6).
 * </ul>
 * 
 * <p>
 * This example shows how to use:
 * <ul>
 * <li> Bulk iterations
 * <li> Broadcast variables in bulk iterations
 * <li> Custom Java objects (PoJos)
 * </ul>
 */
@SuppressWarnings("serial")
public class LinearRegression {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception{

		if(!parseParameters(args)) {
			return;
		}

		// set up execution environment

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input x data from elements
		DataSet<Data> data = getDataSet(env);

		// get the parameters from elements
		DataSet<Params> parameters = getParamsDataSet(env);

		// set number of bulk iterations for SGD linear Regression
		IterativeDataSet<Params> loop = parameters.iterate(numIterations);

		DataSet<Params> new_parameters = data
				// compute a single step using every sample
				.map(new SubUpdate()).withBroadcastSet(loop, "parameters")
				// sum up all the steps
				.reduce(new UpdateAccumulator())
				// average the steps and update all parameters
				.map(new Update());

		// feed new parameters back into next iteration
		DataSet<Params> result = loop.closeWith(new_parameters);

		// emit result
		if(fileOutput) {
			result.writeAsText(outputPath);
			// execute program
			env.execute("Linear Regression example");
		} else {
			result.print();
		}



	}

	// *************************************************************************
	//     DATA TYPES
	// *************************************************************************

	/**
	 * A simple data sample, x means the input, and y means the target.
	 */
	public static class Data implements Serializable{
		public double x,y;

		public Data() {};

		public Data(double x ,double y){
			this.x = x;
			this.y = y;
		}

		@Override
		public String toString() {
			return "(" + x + "|" + y + ")";
		}

	}

	/**
	 * A set of parameters -- theta0, theta1.
	 */
	public static class Params implements Serializable{

		private double theta0,theta1;

		public Params(){};

		public Params(double x0, double x1){
			this.theta0 = x0;
			this.theta1 = x1;
		}

		@Override
		public String toString() {
			return theta0 + " " + theta1;
		}

		public double getTheta0() {
			return theta0;
		}

		public double getTheta1() {
			return theta1;
		}

		public void setTheta0(double theta0) {
			this.theta0 = theta0;
		}

		public void setTheta1(double theta1) {
			this.theta1 = theta1;
		}

		public Params div(Integer a){
			this.theta0 = theta0 / a ;
			this.theta1 = theta1 / a ;
			return this;
		}

	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/** Converts a Tuple2<Double,Double> into a Data. */
	@ForwardedFields("0->x; 1->y")
	public static final class TupleDataConverter implements MapFunction<Tuple2<Double, Double>, Data> {

		@Override
		public Data map(Tuple2<Double, Double> t) throws Exception {
			return new Data(t.f0, t.f1);
		}
	}

	/** Converts a Tuple2<Double,Double> into a Params. */
	@ForwardedFields("0->theta0; 1->theta1")
	public static final class TupleParamsConverter implements MapFunction<Tuple2<Double, Double>,Params> {

		@Override
		public Params map(Tuple2<Double, Double> t)throws Exception {
			return new Params(t.f0,t.f1);
		}
	}

	/**
	 * Compute a single BGD type update for every parameters.
	 */
	public static class SubUpdate extends RichMapFunction<Data,Tuple2<Params,Integer>> {

		private Collection<Params> parameters; 

		private Params parameter;

		private int count = 1;

		/** Reads the parameters from a broadcast variable into a collection. */
		@Override
		public void open(Configuration parameters) throws Exception {
			this.parameters = getRuntimeContext().getBroadcastVariable("parameters");
		}

		@Override
		public Tuple2<Params, Integer> map(Data in) throws Exception {

			for(Params p : parameters){
				this.parameter = p; 
			}

			double theta_0 = parameter.theta0 - 0.01*((parameter.theta0 + (parameter.theta1*in.x)) - in.y);
			double theta_1 = parameter.theta1 - 0.01*(((parameter.theta0 + (parameter.theta1*in.x)) - in.y) * in.x);

			return new Tuple2<Params,Integer>(new Params(theta_0,theta_1),count);
		}
	}

	/**  
	 * Accumulator all the update.
	 * */
	public static class UpdateAccumulator implements ReduceFunction<Tuple2<Params, Integer>> {

		@Override
		public Tuple2<Params, Integer> reduce(Tuple2<Params, Integer> val1, Tuple2<Params, Integer> val2) {

			double new_theta0 = val1.f0.theta0 + val2.f0.theta0;
			double new_theta1 = val1.f0.theta1 + val2.f0.theta1;
			Params result = new Params(new_theta0,new_theta1);
			return new Tuple2<Params, Integer>( result, val1.f1 + val2.f1);

		}
	}

	/**
	 * Compute the final update by average them.
	 */
	public static class Update implements MapFunction<Tuple2<Params, Integer>,Params> {

		@Override
		public Params map(Tuple2<Params, Integer> arg0) throws Exception {

			return arg0.f0.div(arg0.f1);

		}

	}
	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String dataPath = null;
	private static String outputPath = null;
	private static int numIterations = 10;

	private static boolean parseParameters(String[] programArguments) {

		if(programArguments.length > 0) {
			// parse input arguments
			fileOutput = true;
			if(programArguments.length == 3) {
				dataPath = programArguments[0];
				outputPath = programArguments[1];
				numIterations = Integer.parseInt(programArguments[2]);
			} else {
				System.err.println("Usage: LinearRegression <data path> <result path> <num iterations>");
				return false;
			}
		} else {
			System.out.println("Executing Linear Regression example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  We provide a data generator to create synthetic input files for this program.");
			System.out.println("  Usage: LinearRegression <data path> <result path> <num iterations>");
		}
		return true;
	}

	private static DataSet<Data> getDataSet(ExecutionEnvironment env) {
		if(fileOutput) {
			// read data from CSV file
			return env.readCsvFile(dataPath)
					.fieldDelimiter(" ")
					.includeFields(true, true)
					.types(Double.class, Double.class)
					.map(new TupleDataConverter());
		} else {
			return LinearRegressionData.getDefaultDataDataSet(env);
		}
	}

	private static DataSet<Params> getParamsDataSet(ExecutionEnvironment env) {

		return LinearRegressionData.getDefaultParamsDataSet(env);

	}

}

