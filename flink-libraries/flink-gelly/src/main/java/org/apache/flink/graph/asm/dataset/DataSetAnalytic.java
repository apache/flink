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

package org.apache.flink.graph.asm.dataset;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;

/**
 * A {@code DataSetAnalytic} is similar to a {@link CustomUnaryOperation} but
 * is terminal and results are retrieved via accumulators. A Flink program has
 * a single point of execution. A {@code DataSetAnalytic} defers execution to
 * the user to allow composing multiple analytics and algorithms into a single
 * program.
 *
 * @param <T> element type
 * @param <R> the return type
 */
public interface DataSetAnalytic<T, R> {

	/**
	 * All {@code DataSetAnalytic} processing must be terminated by an
	 * {@link OutputFormat} and obtained via accumulators rather than
	 * returned by a {@link DataSet}.
	 *
	 * @param input input dataset
	 * @return this
	 * @throws Exception
	 */

	DataSetAnalytic<T, R> run(DataSet<T> input) throws Exception;

	/**
	 * Execute the program and return the result.
	 *
	 * @return the result
	 * @throws Exception
	 */
	R execute() throws Exception;

	/**
	 * Execute the program and return the result.
	 *
	 * @param jobName the name to assign to the job
	 * @return the result
	 * @throws Exception
	 */
	R execute(String jobName) throws Exception;

	/**
	 * This method must be called after the program has executed.
	 *  1) "run" analytics and algorithms
	 *  2) call ExecutionEnvironment.execute()
	 *  3) get analytic results
	 *
	 * @return the result
	 */
	R getResult();
}
