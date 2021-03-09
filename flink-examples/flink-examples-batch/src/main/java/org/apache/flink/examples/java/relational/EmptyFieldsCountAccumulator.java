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

package org.apache.flink.examples.java.relational;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This program filters lines from a CSV file with empty fields. In doing so, it counts the number
 * of empty fields per column within a CSV file using a custom accumulator for vectors. In this
 * context, empty fields are those, that at most contain whitespace characters like space and tab.
 *
 * <p>The input file is a plain text CSV file with the semicolon as field separator and double
 * quotes as field delimiters and three columns. See {@link #getDataSet(ExecutionEnvironment,
 * ParameterTool)} for configuration.
 *
 * <p>Usage: <code>EmptyFieldsCountAccumulator --input &lt;path&gt; --output &lt;path&gt;</code>
 * <br>
 *
 * <p>This example shows how to use:
 *
 * <ul>
 *   <li>custom accumulators
 *   <li>tuple data types
 *   <li>inline-defined functions
 *   <li>naming large tuple types
 * </ul>
 */
@SuppressWarnings("serial")
public class EmptyFieldsCountAccumulator {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    private static final String EMPTY_FIELD_ACCUMULATOR = "empty-fields";

    public static void main(final String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get the data set
        final DataSet<StringTriple> file = getDataSet(env, params);

        // filter lines with empty fields
        final DataSet<StringTriple> filteredLines = file.filter(new EmptyFieldFilter());

        // Here, we could do further processing with the filtered lines...
        JobExecutionResult result;
        // output the filtered lines
        if (params.has("output")) {
            filteredLines.writeAsCsv(params.get("output"));
            // execute program
            result = env.execute("Accumulator example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            filteredLines.print();
            result = env.getLastJobExecutionResult();
        }

        // get the accumulator result via its registration key
        final List<Integer> emptyFields = result.getAccumulatorResult(EMPTY_FIELD_ACCUMULATOR);
        System.out.format("Number of detected empty fields per column: %s\n", emptyFields);
    }

    // *************************************************************************
    // UTIL METHODS
    // *************************************************************************

    @SuppressWarnings("unchecked")
    private static DataSet<StringTriple> getDataSet(
            ExecutionEnvironment env, ParameterTool params) {
        if (params.has("input")) {
            return env.readCsvFile(params.get("input"))
                    .fieldDelimiter(";")
                    .pojoType(StringTriple.class);
        } else {
            System.out.println(
                    "Executing EmptyFieldsCountAccumulator example with default input data set.");
            System.out.println("Use --input to specify file input.");
            return env.fromCollection(getExampleInputTuples());
        }
    }

    private static Collection<StringTriple> getExampleInputTuples() {
        Collection<StringTriple> inputTuples = new ArrayList<StringTriple>();
        inputTuples.add(new StringTriple("John", "Doe", "Foo Str."));
        inputTuples.add(new StringTriple("Joe", "Johnson", ""));
        inputTuples.add(new StringTriple(null, "Kate Morn", "Bar Blvd."));
        inputTuples.add(new StringTriple("Tim", "Rinny", ""));
        inputTuples.add(new StringTriple("Alicia", "Jackson", "  "));
        return inputTuples;
    }

    /**
     * This function filters all incoming tuples that have one or more empty fields. In doing so, it
     * also counts the number of empty fields per attribute with an accumulator (registered under
     * {@link EmptyFieldsCountAccumulator#EMPTY_FIELD_ACCUMULATOR}).
     */
    public static final class EmptyFieldFilter extends RichFilterFunction<StringTriple> {

        // create a new accumulator in each filter function instance
        // accumulators can be merged later on
        private final VectorAccumulator emptyFieldCounter = new VectorAccumulator();

        @Override
        public void open(final Configuration parameters) throws Exception {
            super.open(parameters);

            // register the accumulator instance
            getRuntimeContext().addAccumulator(EMPTY_FIELD_ACCUMULATOR, this.emptyFieldCounter);
        }

        @Override
        public boolean filter(final StringTriple t) {
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
    }

    /**
     * This accumulator maintains a vector of counts. Calling {@link #add(Integer)} increments the
     * <i>n</i>-th vector component. The size of the vector is automatically managed.
     */
    public static class VectorAccumulator implements Accumulator<Integer, ArrayList<Integer>> {

        /** Stores the accumulated vector components. */
        private final ArrayList<Integer> resultVector;

        public VectorAccumulator() {
            this(new ArrayList<Integer>());
        }

        public VectorAccumulator(ArrayList<Integer> resultVector) {
            this.resultVector = resultVector;
        }

        /** Increases the result vector component at the specified position by 1. */
        @Override
        public void add(Integer position) {
            updateResultVector(position, 1);
        }

        /**
         * Increases the result vector component at the specified position by the specified delta.
         */
        private void updateResultVector(int position, int delta) {
            // inflate the vector to contain the given position
            while (this.resultVector.size() <= position) {
                this.resultVector.add(0);
            }

            // increment the component value
            final int component = this.resultVector.get(position);
            this.resultVector.set(position, component + delta);
        }

        @Override
        public ArrayList<Integer> getLocalValue() {
            return this.resultVector;
        }

        @Override
        public void resetLocal() {
            // clear the result vector if the accumulator instance shall be reused
            this.resultVector.clear();
        }

        @Override
        public void merge(final Accumulator<Integer, ArrayList<Integer>> other) {
            // merge two vector accumulators by adding their up their vector components
            final List<Integer> otherVector = other.getLocalValue();
            for (int index = 0; index < otherVector.size(); index++) {
                updateResultVector(index, otherVector.get(index));
            }
        }

        @Override
        public Accumulator<Integer, ArrayList<Integer>> clone() {
            return new VectorAccumulator(new ArrayList<Integer>(resultVector));
        }

        @Override
        public String toString() {
            return StringUtils.join(resultVector, ',');
        }
    }

    /**
     * It is recommended to use POJOs (Plain old Java objects) instead of TupleX for data types with
     * many fields. Also, POJOs can be used to give large Tuple-types a name. <a
     * href="https://ci.apache.org/projects/flink/flink-docs-master/apis/best_practices.html#naming-large-tuplex-types">Source
     * (docs)</a>
     */
    public static class StringTriple extends Tuple3<String, String, String> {

        public StringTriple() {}

        public StringTriple(String f0, String f1, String f2) {
            super(f0, f1, f2);
        }
    }
}
