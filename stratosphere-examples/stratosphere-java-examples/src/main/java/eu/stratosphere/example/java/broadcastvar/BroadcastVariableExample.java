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
package eu.stratosphere.example.java.broadcastvar;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.configuration.Configuration;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@SuppressWarnings("serial")
public class BroadcastVariableExample {

	public static class ToUppercaseMapper extends MapFunction<String, String> {
		// Lookup table for Strings to uppercase
		private Set<String> toUppercase;

		@Override
		public void open(Configuration parameters) throws Exception {
			// You can access broadcast variables via `getRuntimeContext().getBroadcastVariable(String)`.
			//
			// The broadcasted variable is registered under the previously provided name and the data set is accessed
			// as a Collection<T> over the broadcasted data set (where T is the type of the broadcasted DataSet<T>).
			Collection<String> broadcastedData = getRuntimeContext().getBroadcastVariable("toUppercase");

			this.toUppercase = new HashSet<String>(broadcastedData);
		}

		@Override
		public String map(String value) throws Exception {
			return this.toUppercase.contains(value) ? value.toUpperCase() : value;
		}
	}

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// This example program takes the data set `lorem` and uppercases every String, which is also included in the
		// `toUppercase` data set.
		//
		// The `toUppercase` data set is BROADCASTED to the map operator, which creates a lookup table from it. The
		// lookup tables is then used in the map method to decide whether to uppercase a given String or not.

		DataSet<String> toUppercase = env.fromElements("lorem", "ipsum");

		DataSet<String> lorem = env.fromElements("lorem", "ipsum", "dolor", "sit", "amet");

		lorem.map(new ToUppercaseMapper())
				// You can broadcast a data set to an operator via `withBroadcastSet(DataSet<T>, String)`.
				//
				// The broadcast variable will be registered at the operator under the provided name.
				.withBroadcastSet(toUppercase, "toUppercase")
				.print();

		env.execute("Broadcast Variable Example");
	}
}
