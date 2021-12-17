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

package org.apache.flink.graph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.graph.drivers.AdamicAdar;
import org.apache.flink.graph.drivers.ClusteringCoefficient;
import org.apache.flink.graph.drivers.ConnectedComponents;
import org.apache.flink.graph.drivers.Driver;
import org.apache.flink.graph.drivers.EdgeList;
import org.apache.flink.graph.drivers.GraphMetrics;
import org.apache.flink.graph.drivers.HITS;
import org.apache.flink.graph.drivers.JaccardIndex;
import org.apache.flink.graph.drivers.PageRank;
import org.apache.flink.graph.drivers.TriangleListing;
import org.apache.flink.graph.drivers.input.CirculantGraph;
import org.apache.flink.graph.drivers.input.CompleteGraph;
import org.apache.flink.graph.drivers.input.CycleGraph;
import org.apache.flink.graph.drivers.input.EchoGraph;
import org.apache.flink.graph.drivers.input.EmptyGraph;
import org.apache.flink.graph.drivers.input.GridGraph;
import org.apache.flink.graph.drivers.input.HypercubeGraph;
import org.apache.flink.graph.drivers.input.Input;
import org.apache.flink.graph.drivers.input.PathGraph;
import org.apache.flink.graph.drivers.input.RMatGraph;
import org.apache.flink.graph.drivers.input.SingletonEdgeGraph;
import org.apache.flink.graph.drivers.input.StarGraph;
import org.apache.flink.graph.drivers.output.Hash;
import org.apache.flink.graph.drivers.output.Output;
import org.apache.flink.graph.drivers.output.Print;
import org.apache.flink.graph.drivers.parameter.BooleanParameter;
import org.apache.flink.graph.drivers.parameter.Parameterized;
import org.apache.flink.graph.drivers.parameter.ParameterizedBase;
import org.apache.flink.graph.drivers.parameter.StringParameter;
import org.apache.flink.graph.drivers.transform.Transform;
import org.apache.flink.graph.drivers.transform.Transformable;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.InstantiationUtil;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonEncoding;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import org.apache.commons.lang3.text.StrBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This default main class executes Flink drivers.
 *
 * <p>An execution has one input, one algorithm, and one output. Anything more complex can be
 * expressed as a user program written in a JVM language.
 *
 * <p>Inputs and algorithms are decoupled by, respectively, producing and consuming a graph.
 * Currently only {@code Graph} is supported but later updates may add support for new graph types
 * such as {@code BipartiteGraph}.
 *
 * <p>Algorithms must explicitly support each type of output via implementation of interfaces. This
 * is scalable as the number of outputs is small and finite.
 */
public class Runner extends ParameterizedBase {

    private static final String INPUT = "input";

    private static final String ALGORITHM = "algorithm";

    private static final String OUTPUT = "output";

    private static ParameterizedFactory<Input> inputFactory =
            new ParameterizedFactory<Input>()
                    .addClass(CirculantGraph.class)
                    .addClass(CompleteGraph.class)
                    .addClass(org.apache.flink.graph.drivers.input.CSV.class)
                    .addClass(CycleGraph.class)
                    .addClass(EchoGraph.class)
                    .addClass(EmptyGraph.class)
                    .addClass(GridGraph.class)
                    .addClass(HypercubeGraph.class)
                    .addClass(PathGraph.class)
                    .addClass(RMatGraph.class)
                    .addClass(SingletonEdgeGraph.class)
                    .addClass(StarGraph.class);

    private static ParameterizedFactory<Driver> driverFactory =
            new ParameterizedFactory<Driver>()
                    .addClass(AdamicAdar.class)
                    .addClass(ClusteringCoefficient.class)
                    .addClass(ConnectedComponents.class)
                    .addClass(EdgeList.class)
                    .addClass(GraphMetrics.class)
                    .addClass(HITS.class)
                    .addClass(JaccardIndex.class)
                    .addClass(PageRank.class)
                    .addClass(TriangleListing.class);

    private static ParameterizedFactory<Output> outputFactory =
            new ParameterizedFactory<Output>()
                    .addClass(org.apache.flink.graph.drivers.output.CSV.class)
                    .addClass(Hash.class)
                    .addClass(Print.class);

    // parameters

    private final ParameterTool parameters;

    private final BooleanParameter disableObjectReuse =
            new BooleanParameter(this, "__disable_object_reuse");

    private final StringParameter jobDetailsPath =
            new StringParameter(this, "__job_details_path").setDefaultValue(null);

    private StringParameter jobName = new StringParameter(this, "__job_name").setDefaultValue(null);

    // state

    private ExecutionEnvironment env;

    private DataSet result;

    private String executionName;

    private Driver algorithm;

    private Output output;

    /**
     * Create an algorithm runner from the given arguments.
     *
     * @param args command-line arguments
     */
    public Runner(String[] args) {
        parameters = ParameterTool.fromArgs(args);
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    /**
     * Get the ExecutionEnvironment. The ExecutionEnvironment is only available after calling {@link
     * Runner#run()}.
     *
     * @return the ExecutionEnvironment
     */
    public ExecutionEnvironment getExecutionEnvironment() {
        return env;
    }

    /**
     * Get the result DataSet. The result is only available after calling {@link Runner#run()}.
     *
     * @return the result DataSet
     */
    public DataSet getResult() {
        return result;
    }

    /**
     * List available algorithms. This is displayed to the user when no valid algorithm is given in
     * the program parameterization.
     *
     * @return usage string listing available algorithms
     */
    private static String getAlgorithmsListing() {
        StrBuilder strBuilder = new StrBuilder();

        strBuilder
                .appendNewLine()
                .appendln(
                        "Select an algorithm to view usage: flink run examples/flink-gelly-examples_<version>.jar --algorithm <algorithm>")
                .appendNewLine()
                .appendln("Available algorithms:");

        for (Driver algorithm : driverFactory) {
            strBuilder
                    .append("  ")
                    .appendFixedWidthPadRight(algorithm.getName(), 30, ' ')
                    .append(algorithm.getShortDescription())
                    .appendNewLine();
        }

        return strBuilder.toString();
    }

    /**
     * Display the usage for the given algorithm. This includes options for all compatible inputs,
     * the selected algorithm, and outputs implemented by the selected algorithm.
     *
     * @param algorithmName unique identifier of the selected algorithm
     * @return usage string for the given algorithm
     */
    private static String getAlgorithmUsage(String algorithmName) {
        StrBuilder strBuilder = new StrBuilder();

        Driver algorithm = driverFactory.get(algorithmName);

        strBuilder
                .appendNewLine()
                .appendNewLine()
                .appendln(algorithm.getLongDescription())
                .appendNewLine()
                .append("usage: flink run examples/flink-gelly-examples_<version>.jar --algorithm ")
                .append(algorithmName)
                .append(
                        " [algorithm options] --input <input> [input options] --output <output> [output options]")
                .appendNewLine()
                .appendNewLine()
                .appendln("Available inputs:");

        for (Input input : inputFactory) {
            strBuilder
                    .append("  --input ")
                    .append(input.getName())
                    .append(" ")
                    .appendln(input.getUsage());
        }

        String algorithmParameterization = algorithm.getUsage();

        if (algorithmParameterization.length() > 0) {
            strBuilder
                    .appendNewLine()
                    .appendln("Algorithm configuration:")
                    .append("  ")
                    .appendln(algorithm.getUsage());
        }

        strBuilder.appendNewLine().appendln("Available outputs:");

        for (Output output : outputFactory) {
            strBuilder
                    .append("  --output ")
                    .append(output.getName())
                    .append(" ")
                    .appendln(output.getUsage());
        }

        return strBuilder.appendNewLine().toString();
    }

    /**
     * Configure a runtime component. Catch {@link RuntimeException} and re-throw with a Flink
     * internal exception which is processed by CliFrontend for display to the user.
     *
     * @param parameterized the component to be configured
     */
    private void parameterize(Parameterized parameterized) {
        try {
            parameterized.configure(parameters);
        } catch (RuntimeException ex) {
            throw new ProgramParametrizationException(ex.getMessage());
        }
    }

    /**
     * Setup the Flink job with the graph input, algorithm, and output.
     *
     * <p>To then execute the job call {@link #execute}.
     *
     * @return this
     * @throws Exception on error
     */
    public Runner run() throws Exception {
        // Set up the execution environment
        env = ExecutionEnvironment.getExecutionEnvironment();
        ExecutionConfig config = env.getConfig();

        // should not have any non-Flink data types
        config.disableForceAvro();
        config.disableForceKryo();

        config.setGlobalJobParameters(parameters);
        parameterize(this);

        // configure local parameters and throw proper exception on error
        try {
            this.configure(parameters);
        } catch (RuntimeException ex) {
            throw new ProgramParametrizationException(ex.getMessage());
        }

        // integration tests run with with object reuse both disabled and enabled
        if (disableObjectReuse.getValue()) {
            config.disableObjectReuse();
        } else {
            config.enableObjectReuse();
        }

        // ----------------------------------------------------------------------------------------
        // Usage and configuration
        // ----------------------------------------------------------------------------------------

        // algorithm and usage
        if (!parameters.has(ALGORITHM)) {
            throw new ProgramParametrizationException(getAlgorithmsListing());
        }

        String algorithmName = parameters.get(ALGORITHM);
        algorithm = driverFactory.get(algorithmName);

        if (algorithm == null) {
            throw new ProgramParametrizationException("Unknown algorithm name: " + algorithmName);
        }

        // input and usage
        if (!parameters.has(INPUT)) {
            if (!parameters.has(OUTPUT)) {
                // if neither input nor output is given then print algorithm usage
                throw new ProgramParametrizationException(getAlgorithmUsage(algorithmName));
            }
            throw new ProgramParametrizationException("No input given");
        }

        parameterize(algorithm);

        String inputName = parameters.get(INPUT);
        Input input = inputFactory.get(inputName);

        if (input == null) {
            throw new ProgramParametrizationException("Unknown input type: " + inputName);
        }

        parameterize(input);

        // output and usage
        if (!parameters.has(OUTPUT)) {
            throw new ProgramParametrizationException("No output given");
        }

        String outputName = parameters.get(OUTPUT);
        output = outputFactory.get(outputName);

        if (output == null) {
            throw new ProgramParametrizationException("Unknown output type: " + outputName);
        }

        parameterize(output);

        // ----------------------------------------------------------------------------------------
        // Create list of input and algorithm transforms
        // ----------------------------------------------------------------------------------------

        List<Transform> transforms = new ArrayList<>();

        if (input instanceof Transformable) {
            transforms.addAll(((Transformable) input).getTransformers());
        }

        if (algorithm instanceof Transformable) {
            transforms.addAll(((Transformable) algorithm).getTransformers());
        }

        for (Transform transform : transforms) {
            parameterize(transform);
        }

        // unused parameters
        if (parameters.getUnrequestedParameters().size() > 0) {
            throw new ProgramParametrizationException(
                    "Unrequested parameters: " + parameters.getUnrequestedParameters());
        }

        // ----------------------------------------------------------------------------------------
        // Execute
        // ----------------------------------------------------------------------------------------

        // Create input
        Graph graph = input.create(env);

        // Transform input
        for (Transform transform : transforms) {
            graph = (Graph) transform.transformInput(graph);
        }

        // Run algorithm
        result = algorithm.plan(graph);

        // Output
        executionName = jobName.getValue() != null ? jobName.getValue() + ": " : "";

        executionName += input.getIdentity() + " ⇨ " + algorithmName + " ⇨ " + output.getName();

        if (transforms.size() > 0) {
            // append identifiers to job name
            StringBuffer buffer = new StringBuffer(executionName).append(" [");

            for (Transform transform : transforms) {
                buffer.append(transform.getIdentity());
            }

            executionName = buffer.append("]").toString();
        }

        if (output == null) {
            throw new ProgramParametrizationException("Unknown output type: " + outputName);
        }

        try {
            output.configure(parameters);
        } catch (RuntimeException ex) {
            throw new ProgramParametrizationException(ex.getMessage());
        }

        if (result != null) {
            // Transform output if algorithm returned result DataSet
            if (transforms.size() > 0) {
                Collections.reverse(transforms);
                for (Transform transform : transforms) {
                    result = (DataSet) transform.transformResult(result);
                }
            }
        }

        return this;
    }

    /**
     * Execute the Flink job.
     *
     * @throws Exception on error
     */
    private void execute() throws Exception {
        if (result == null) {
            env.execute(executionName);
        } else {
            output.write(executionName.toString(), System.out, result);
        }

        System.out.println();
        algorithm.printAnalytics(System.out);

        if (jobDetailsPath.getValue() != null) {
            writeJobDetails(env, jobDetailsPath.getValue());
        }
    }

    /**
     * Write the following job details as a JSON encoded file: runtime environment job ID, runtime,
     * parameters, and accumulators.
     *
     * @param env the execution environment
     * @param jobDetailsPath filesystem path to write job details
     * @throws IOException on error writing to jobDetailsPath
     */
    private static void writeJobDetails(ExecutionEnvironment env, String jobDetailsPath)
            throws IOException {
        JobExecutionResult result = env.getLastJobExecutionResult();

        File jsonFile = new File(jobDetailsPath);

        try (JsonGenerator json = new JsonFactory().createGenerator(jsonFile, JsonEncoding.UTF8)) {
            json.writeStartObject();

            json.writeObjectFieldStart("Apache Flink");
            json.writeStringField("version", EnvironmentInformation.getVersion());
            json.writeStringField(
                    "commit ID", EnvironmentInformation.getRevisionInformation().commitId);
            json.writeStringField(
                    "commit date", EnvironmentInformation.getRevisionInformation().commitDate);
            json.writeEndObject();

            json.writeStringField("job_id", result.getJobID().toString());
            json.writeNumberField("runtime_ms", result.getNetRuntime());

            json.writeObjectFieldStart("parameters");
            for (Map.Entry<String, String> entry :
                    env.getConfig().getGlobalJobParameters().toMap().entrySet()) {
                json.writeStringField(entry.getKey(), entry.getValue());
            }
            json.writeEndObject();

            json.writeObjectFieldStart("accumulators");
            for (Map.Entry<String, Object> entry : result.getAllAccumulatorResults().entrySet()) {
                json.writeStringField(entry.getKey(), entry.getValue().toString());
            }
            json.writeEndObject();

            json.writeEndObject();
        }
    }

    public static void main(String[] args) throws Exception {
        new Runner(args).run().execute();
    }

    /**
     * Stores a list of classes for which an instance can be requested by name and implements an
     * iterator over class instances.
     *
     * @param <T> base type for stored classes
     */
    private static class ParameterizedFactory<T extends Parameterized> implements Iterable<T> {
        private List<Class<? extends T>> classes = new ArrayList<>();

        /**
         * Add a class to the factory.
         *
         * @param cls subclass of T
         * @return this
         */
        public ParameterizedFactory<T> addClass(Class<? extends T> cls) {
            this.classes.add(cls);
            return this;
        }

        /**
         * Obtain a class instance by name.
         *
         * @param name String matching getName()
         * @return class instance or null if no matching class
         */
        public T get(String name) {
            for (T instance : this) {
                if (name.equalsIgnoreCase(instance.getName())) {
                    return instance;
                }
            }

            return null;
        }

        @Override
        public Iterator<T> iterator() {
            return new Iterator<T>() {
                private int index;

                @Override
                public boolean hasNext() {
                    return index < classes.size();
                }

                @Override
                public T next() {
                    return InstantiationUtil.instantiate(classes.get(index++));
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }
}
