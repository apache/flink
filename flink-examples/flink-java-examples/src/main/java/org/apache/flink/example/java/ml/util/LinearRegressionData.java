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


package org.apache.flink.example.java.ml.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.example.java.ml.LinearRegression.Data;
import org.apache.flink.example.java.ml.LinearRegression.Params;

/**
 * Provides the default data sets used for the Linear Regression example program.
 * The default data sets are used, if no parameters are given to the program.
 *
 */
public class LinearRegressionData{

	public static DataSet<Params> getDefaultParamsDataSet(ExecutionEnvironment env){

		return env.fromElements(
				new Params(0.0,0.0)
				);
	}

	public static DataSet<Data> getDefaultDataDataSet(ExecutionEnvironment env){

		return env.fromElements(
				new Data(0.5,1.0),
				new Data(1.0,2.0),
				new Data(2.0,4.0),
				new Data(3.0,6.0),
				new Data(4.0,8.0),
				new Data(5.0,10.0),
				new Data(6.0,12.0),
				new Data(7.0,14.0),
				new Data(8.0,16.0),
				new Data(9.0,18.0),
				new Data(10.0,20.0),
				new Data(-0.08,-0.16),
				new Data(0.13,0.26),
				new Data(-1.17,-2.35),
				new Data(1.72,3.45),
				new Data(1.70,3.41),
				new Data(1.20,2.41),
				new Data(-0.59,-1.18),
				new Data(0.28,0.57),
				new Data(1.65,3.30),
				new Data(-0.55,-1.08)
				);
	}

}