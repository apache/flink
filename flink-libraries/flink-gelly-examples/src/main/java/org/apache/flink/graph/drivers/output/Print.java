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

package org.apache.flink.graph.drivers.output;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.asm.dataset.Collect;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.drivers.parameter.BooleanParameter;

import java.io.PrintStream;
import java.util.List;

/**
 * Print algorithm output.
 *
 * @param <T> result Type
 */
public class Print<T>
extends OutputBase<T> {

	private BooleanParameter printExecutionPlan = new BooleanParameter(this, "__print_execution_plan");

	@Override
	public void write(String executionName, PrintStream out, DataSet<T> data) throws Exception {
		Collect<T> collector = new Collect<T>().run(data);

		if (printExecutionPlan.getValue()) {
			System.out.println(data.getExecutionEnvironment().getExecutionPlan());
		}

		List<T> results = collector.execute(executionName);

		if (results.size() == 0) {
			return;
		}

		if (results.get(0) instanceof PrintableResult) {
			for (Object result : results) {
				System.out.println(((PrintableResult) result).toPrintableString());
			}
		} else {
			for (Object result : results) {
				System.out.println(result);
			}
		}
	}
}
