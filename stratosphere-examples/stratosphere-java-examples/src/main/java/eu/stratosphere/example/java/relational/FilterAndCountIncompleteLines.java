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
package eu.stratosphere.example.java.relational;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.accumulators.Accumulator;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FilterFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.configuration.Configuration;

/**
 * This program filters lines from a CSV file with empty fields. In doing so, it counts the number of empty fields per
 * column within a CSV file using a custom accumulator for vectors. In this context, empty fields are those, that at
 * most contain whitespace characters like space and tab.
 * <p>
 * The input file is a plain text CSV file with the semicolon as field separator and double quotes as field delimiters
 * and three columns. See {@link #getDataSet(ExecutionEnvironment)} for configuration.
 * <p>
 * Usage: <code>FilterAndCountIncompleteLines [&lt;input file path&gt; [&lt;result path&gt;]]</code> <br>
 * <p>
 * This example shows how to use:
 * <ul>
 * <li>custom accumulators
 * <li>tuple data types
 * <li>inline-defined functions
 * </ul>
 */
@SuppressWarnings("serial")
public class FilterAndCountIncompleteLines {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	private static final String EMPTY_FIELD_ACCUMULATOR = "empty-fields";

	public static void main(final String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get the data set
		final DataSet<Tuple> file = getDataSet(env);

		// filter lines with empty fields
		final DataSet<Tuple> filteredLines = file.filter(new FilterFunction<Tuple>() {

			// create a new accumulator in each filter function instance
			// accumulators can be merged later on
			private final VectorAccumulator emptyFieldCounter = new VectorAccumulator();

			@Override
			public void open(final Configuration parameters) throws Exception {
				super.open(parameters);

				// register the accumulator instance
				getRuntimeContext().addAccumulator(EMPTY_FIELD_ACCUMULATOR,
						this.emptyFieldCounter);
			}

			@Override
			public boolean filter(final Tuple t) {
				boolean containsEmptyFields = false;

				// iterate over the tuple fields looking for empty ones
				for (int pos = 0; pos < t.getArity(); pos++) {

					final String field = t.getField(pos);
					if (field == null || field.trim().isEmpty()) {
						containsEmptyFields = true;

						// if an empty field is encountered, update the
						// accumulator
						this.emptyFieldCounter.add(pos);
					}
				}

				return !containsEmptyFields;
			}
		});

		// Here, we could do further processing with the filtered lines...
		if (outputPath == null) {
			filteredLines.print();
		} else {
			filteredLines.writeAsCsv(outputPath);
		}

		// execute program
		final JobExecutionResult result = env.execute("Accumulator example");

		// get the accumulator result via its registration key
		final List<Integer> emptyFields = result.getAccumulatorResult(EMPTY_FIELD_ACCUMULATOR);
		System.out.format("Number of detected empty fields per column: %s\n", emptyFields);

	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static String filePath;
	private static String outputPath;

	private static boolean parseParameters(final String[] programArguments) {

		if (programArguments.length >= 3) {
			System.err.println("Usage: FilterAndCountIncompleteLines [<input file path> [<result path>]]");
			return false;
		}

		if (programArguments.length >= 1) {
			filePath = programArguments[0];
			if (programArguments.length == 2) {
				outputPath = programArguments[1];
			}
		}

		return true;
	}

	@SuppressWarnings("unchecked")
	private static DataSet<Tuple> getDataSet(final ExecutionEnvironment env) {

		DataSet<? extends Tuple> source;
		if (filePath == null) {
			source = env.fromCollection(getExampleInputTuples());

		} else {
			source = env
					.readCsvFile(filePath)
					.fieldDelimiter(';')
					.types(String.class, String.class, String.class);

		}

		return (DataSet<Tuple>) source;
	}

	private static Collection<Tuple3<String, String, String>> getExampleInputTuples() {
		Collection<Tuple3<String, String, String>> inputTuples = new ArrayList<Tuple3<String, String, String>>();
		inputTuples.add(new Tuple3<String, String, String>("John", "Doe", "Foo Str."));
		inputTuples.add(new Tuple3<String, String, String>("Joe", "Johnson", ""));
		inputTuples.add(new Tuple3<String, String, String>(null, "Kate Morn", "Bar Blvd."));
		inputTuples.add(new Tuple3<String, String, String>("Tim", "Rinny", ""));
		inputTuples.add(new Tuple3<String, String, String>("Alicia", "Jackson", "  "));
		return inputTuples;
	}

	/**
	 * This accumulator lets you increase vector components distributedly. The {@link #add(Integer)} method lets you
	 * increase the <i>n</i>-th vector component by 1, whereat <i>n</i> is the methods parameter. The size of the vector
	 * is automatically managed.
	 */
	public static class VectorAccumulator implements Accumulator<Integer, List<Integer>> {

		/** Stores the accumulated vector components. */
		private final List<Integer> resultVector = new ArrayList<Integer>();

		/**
		 * Increases the result vector component at the specified position by 1.
		 */
		@Override
		public void add(final Integer position) {
			updateResultVector(position, 1);
		}

		/**
		 * Increases the result vector component at the specified position by the specified delta.
		 */
		private void updateResultVector(final int position, final int delta) {
			// inflate the vector to contain the given position
			while (this.resultVector.size() <= position) {
				this.resultVector.add(0);
			}

			// increment the component value
			final int component = this.resultVector.get(position);
			this.resultVector.set(position, component + delta);
		}

		@Override
		public List<Integer> getLocalValue() {
			return this.resultVector;
		}

		@Override
		public void resetLocal() {
			// clear the result vector if the accumulator instance shall be reused
			this.resultVector.clear();
		}

		@Override
		public void merge(final Accumulator<Integer, List<Integer>> other) {
			// merge two vector accumulators by adding their up their vector components
			final List<Integer> otherVector = other.getLocalValue();
			for (int index = 0; index < otherVector.size(); index++) {
				updateResultVector(index, otherVector.get(index));
			}
		}

		@Override
		public void write(final DataOutput out) throws IOException {
			// binary serialization of the result vector:
			// [number of components, component 0, component 1, ...]
			out.writeInt(this.resultVector.size());
			for (final Integer component : this.resultVector) {
				out.writeInt(component);
			}
		}

		@Override
		public void read(final DataInput in) throws IOException {
			// binary deserialization of the result vector
			final int size = in.readInt();
			for (int numReadComponents = 0; numReadComponents < size; numReadComponents++) {
				final int component = in.readInt();
				this.resultVector.add(component);
			}
		}

	}
}
