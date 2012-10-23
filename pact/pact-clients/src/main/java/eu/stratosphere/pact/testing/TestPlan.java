/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.testing;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import junit.framework.Assert;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGraphIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.executiongraph.GraphConversionException;
import eu.stratosphere.nephele.executiongraph.InternalJobStatus;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.InstanceTypeDescriptionFactory;
import eu.stratosphere.nephele.instance.local.LocalInstanceManager;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.jobmanager.scheduler.local.LocalScheduler;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.SequentialInputFormat;
import eu.stratosphere.pact.common.io.SequentialOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.PactConfigConstants;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.costs.FixedSizeClusterCostEstimator;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;
import eu.stratosphere.pact.compiler.plan.PactConnection;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy.ForwardSS;

/**
 * The primary resource to test one or more implemented PACT stubs. It is
 * created in a unit tests and performs the following operations.
 * <ul>
 * <li>Adds {@link GenericDataSource<?>}s and {@link GenericDataSink}s if not explicitly specified,
 * <li>locally runs the PACT stubs,
 * <li>checks the results against the pairs as specified in {@link #getExpectedOutput()}, and
 * <li>provides comfortable access to the results with {@link #getActualOutput()}. <br>
 * </ul>
 * <br>
 * The typical usage is inside a unit test. And might look like one of the
 * following examples. <br>
 * <br>
 * <b>Test complete plan<br>
 * <code><pre>
 *    // build plan
 *    GenericDataSource<?>&lt;Key, Value&gt; source = ...;
 *    MapContract&lt;Key, Value, Key, Value&gt; map = new MapContract&lt;Key, Value, Key, Value&gt;(IdentityMap.class, "Map");
 *    map.setInput(source);    
 *    GenericDataSink&lt;Key, Value&gt; output = ...;
 *    output.setInput(map);
 *    // configure test
 *    TestPlan testPlan = new TestPlan(output);
 *    testPlan.getExpectedOutput(output).fromFile(...);
 *    testPlan.run();
 * </pre></code> <b>Test plan with ad-hoc source and sink<br>
 * <code><pre>
 *    // build plan
 *    MapContract&lt;Key, Value, Key, Value&gt; map = new MapContract&lt;Key, Value, Key, Value&gt;(IdentityMap.class, "Map");
 *    // configure test
 *    TestPlan testPlan = new TestPlan(map);
 *    testPlan.getInput().add(pair1).add(pair2).add(pair3);
 *    testPlan.getExpectedOutput(output).add(pair1).add(pair2).add(pair3);
 *    testPlan.run();
 * </pre></code> <b>Access ad-hoc source and sink of Testplan<br>
 * <code><pre>
 *    // build plan
 *    MapContract&lt;Key, Value, Key, Value&gt; map = new MapContract&lt;Key, Value, Key, Value&gt;(IdentityMap.class, "Map");
 *    // configure test
 *    TestPlan testPlan = new TestPlan(map);
 *    testPlan.getInput().add(randomInput1).add(randomInput2).add(randomInput3);
 *    testPlan.run();
 *    // custom assertions
 *    Assert.assertEquals(testPlan.getInput(), testPlan.getOutput());
 * </pre></code> <br>
 * 
 * @author Arvid Heise
 */

public class TestPlan implements Closeable {

	private static final InstanceTypeDescription MOCK_INSTANCE_DESCRIPTION =
		InstanceTypeDescriptionFactory.construct(
			MockInstanceManager.DEFAULT_INSTANCE_TYPE, MockInstance.DESCRIPTION, 1);

	private static GenericDataSink ALL_SINKS = null;

	private final Map<GenericDataSink, TestRecords> actualOutputs = new IdentityHashMap<GenericDataSink, TestRecords>();

	private final Contract[] contracts;

	private int degreeOfParallelism = 1;

	private final Map<GenericDataSink, TestRecords> expectedOutputs =
		new IdentityHashMap<GenericDataSink, TestRecords>();

	private final Map<GenericDataSource<?>, TestRecords> inputs =
		new IdentityHashMap<GenericDataSource<?>, TestRecords>();

	private final List<FileDataSink> sinks = new ArrayList<FileDataSink>();

	private final List<FileDataSource> sources = new ArrayList<FileDataSource>();

	private final Map<GenericDataSink, Int2ObjectMap<List<ValueSimilarity<?>>>> fuzzySimilarity =
		new HashMap<GenericDataSink, Int2ObjectMap<List<ValueSimilarity<?>>>>();

	private final Map<GenericDataSink, FuzzyValueMatcher> fuzzyMatchers =
		new HashMap<GenericDataSink, FuzzyValueMatcher>();

	private LocalScheduler localScheduler;

	List<ExecutionExceptionHandler> errorHandlers = new ArrayList<TestPlan.ExecutionExceptionHandler>();

	/**
	 * Initializes TestPlan with the given {@link Contract}s. Like the original {@link Plan}, the contracts may be
	 * {@link GenericDataSink}s. However, it
	 * is also possible to add arbitrary Contracts, to which FileDataSinkContracts
	 * are automatically added.
	 * 
	 * @param contracts
	 *        a list of Contracts with at least one element.
	 */
	public TestPlan(final Collection<? extends Contract> contracts) {
		this(contracts.toArray(new Contract[contracts.size()]));
	}

	/**
	 * Initializes TestPlan with the given {@link Contract}s. Like the original {@link Plan}, the contracts may be
	 * {@link GenericDataSink}s. However, it
	 * is also possible to add arbitrary Contracts, to which FileDataSinkContracts
	 * are automatically added.
	 * 
	 * @param contracts
	 *        a list of Contracts with at least one element.
	 */
	public TestPlan(final Contract... contracts) {
		if (contracts.length == 0)
			throw new IllegalArgumentException();

		this.fuzzyMatchers.put(ALL_SINKS, new NaiveFuzzyValueMatcher());

		final Configuration config = new Configuration();
		config.setString(PactConfigConstants.DEFAULT_INSTANCE_TYPE_KEY, "standard,1,1,200,1,1");
		GlobalConfiguration.includeConfiguration(config);

		this.contracts = new InputOutputAdder().process(contracts);

		this.findSinksAndSources();
		this.configureSinksAndSources();
	}

	/**
	 * Sets a fuzzy similarity measure for the values of the given data sink.
	 * 
	 * @param <V>
	 *        the value type
	 * @param sink
	 *        the data sink
	 * @param similarity
	 *        the similarity measure to use
	 */
	public void addFuzzyValueSimilarity(GenericDataSink sink, int valueIndex, ValueSimilarity<?> similarity) {
		this.getFuzzySimilarities(sink, valueIndex).add(similarity);
	}

	/**
	 * Sets a fuzzy similarity measure for the values of the given data sink.
	 * 
	 * @param <V>
	 *        the value type
	 * @param sink
	 *        the data sink
	 * @param similarity
	 *        the similarity measure to use
	 */
	public void addFuzzyValueSimilarity(GenericDataSink sink, ValueSimilarity<?> similarity) {
		this.addFuzzyValueSimilarity(sink, TestRecords.ALL_VALUES, similarity);
	}

	/**
	 * Sets a fuzzy similarity measure for the values of all data sinks.
	 * 
	 * @param similarity
	 *        the similarity measure to use
	 */
	public void addFuzzyValueSimilarity(ValueSimilarity<?> similarity) {
		this.addFuzzyValueSimilarity(ALL_SINKS, similarity);
	}

	@Override
	public void close() throws IOException {
		ClosableManager closableManager = new ClosableManager();

		for (TestRecords pairs : this.inputs.values())
			closableManager.add(pairs);
		for (TestRecords pairs : this.actualOutputs.values())
			closableManager.add(pairs);
		for (TestRecords pairs : this.expectedOutputs.values())
			closableManager.add(pairs);

		closableManager.close();
	}

	/**
	 * Returns the first output {@link TestPairs} of the TestPlan. If multiple
	 * contracts are tested in the TestPlan, it is recommended to use the {@link #getActualOutput(GenericDataSink)}
	 * method to unambiguously get
	 * the values.<br>
	 * The values are only meaningful after a {@link #run()}.
	 * 
	 * @return the first output of the TestPlan
	 */
	public TestRecords getActualOutput() {
		return this.getActualOutput(0);
	}

	/**
	 * Returns the output {@link TestPairs} of the TestPlan associated with the
	 * given sink. This is the recommended method to get output pairs for more
	 * complex TestPlans.<br>
	 * The values are only meaningful after a {@link #run()}.
	 * 
	 * @param <K>
	 *        the type of the key
	 * @param <V>
	 *        the type of the value
	 * @param sink
	 *        the sink of which the associated output TestPairs should be
	 *        returned
	 * @return the output {@link TestPairs} of the TestPlan associated with the
	 *         given sink
	 */
	public TestRecords getActualOutput(final FileDataSink sink) {
		TestRecords values = this.actualOutputs.get(sink);
		if (values == null)
			this.actualOutputs.put(sink, values = new TestRecords());
		return values;
	}

	/**
	 * Returns the output {@link TestPairs} associated with the <i>i</i>th
	 * output of the TestPlan. If multiple contracts are tested in the TestPlan,
	 * it is recommended to use the {@link #getActualOutput(GenericDataSink)} method to unambiguously get the
	 * values.<br>
	 * The values are only meaningful after a {@link #run()}.
	 * 
	 * @param number
	 *        the number of the output.
	 * @return the <i>i</i>th output of the TestPlan
	 */
	public TestRecords getActualOutput(final int number) {
		return this.getActualOutput(this.getDataSinks().get(number));
	}

	/**
	 * Allowed delta for PactDouble values, default value is 0.
	 * 
	 * @return the allowed delta
	 */
	public double getAllowedPactDoubleDelta() {
		for (ValueSimilarity<?> sim : this.getFuzzySimilarities())
			if (sim instanceof DoubleValueSimilarity)
				return ((DoubleValueSimilarity) sim).getDelta();
		return 0;
	}

	/**
	 * Returns the degreeOfParallelism.
	 * 
	 * @return the degreeOfParallelism
	 */
	public int getDegreeOfParallelism() {
		return this.degreeOfParallelism;
	}

	/**
	 * Returns the first expected output {@link TestPairs} of the TestPlan. If
	 * multiple contracts are tested in the TestPlan, it is recommended to use
	 * the {@link #getExpectedOutput(GenericDataSink)} method to unambiguously
	 * set the values.
	 * 
	 * @return the first expected output of the TestPlan
	 */
	public TestRecords getExpectedOutput(Class<? extends Value> firstFieldType, Class<?>... otherFieldTypes) {
		return this.getExpectedOutput(0, firstFieldType, otherFieldTypes);
	}

	/**
	 * Returns the first expected output {@link TestPairs} of the TestPlan. If
	 * multiple contracts are tested in the TestPlan, it is recommended to use
	 * the {@link #getExpectedOutput(GenericDataSink)} method to unambiguously
	 * set the values.
	 * 
	 * @return the first expected output of the TestPlan
	 */
	public TestRecords getExpectedOutput(Class<? extends Value>[] schema) {
		return this.getExpectedOutput(0, schema);
	}

	/**
	 * Returns the expected output {@link TestPairs} of the TestPlan associated
	 * with the given sink. This is the recommended method to set expected
	 * output pairs for more complex TestPlans.
	 * 
	 * @param <K>
	 *        the type of the key
	 * @param <V>
	 *        the type of the value
	 * @param sink
	 *        the sink of which the associated expected output TestPairs
	 *        should be returned
	 * @return the expected output {@link TestPairs} of the TestPlan associated
	 *         with the given sink
	 */
	public TestRecords getExpectedOutput(final FileDataSink sink, Class<? extends Value> firstFieldType,
			Class<?>... otherFieldTypes) {
		return this.getExpectedOutput(sink, SchemaUtils.combineSchema(firstFieldType, otherFieldTypes));
	}

	/**
	 * Returns the expected output {@link TestPairs} of the TestPlan associated
	 * with the given sink. This is the recommended method to set expected
	 * output pairs for more complex TestPlans.
	 * 
	 * @param <K>
	 *        the type of the key
	 * @param <V>
	 *        the type of the value
	 * @param sink
	 *        the sink of which the associated expected output TestPairs
	 *        should be returned
	 * @return the expected output {@link TestPairs} of the TestPlan associated
	 *         with the given sink
	 */
	public TestRecords getExpectedOutput(final FileDataSink sink, Class<? extends Value>[] schema) {
		TestRecords values = this.expectedOutputs.get(sink);
		if (values == null) {
			this.expectedOutputs.put(sink, values = new TestRecords(schema));
			TestRecords actualOutput = this.getActualOutput(sink);
			actualOutput.setSchema(values.getSchema());
		}
		return values;
	}

	/**
	 * Returns the expected output {@link TestPairs} associated with the
	 * <i>i</i>th expected output of the TestPlan. If multiple contracts are
	 * tested in the TestPlan, it is recommended to use the {@link #getExpectedOutput(GenericDataSink)} method to
	 * unambiguously set
	 * the values.
	 * 
	 * @param number
	 *        the number of the expected output.
	 * @return the <i>i</i>th expected output of the TestPlan
	 */
	public TestRecords getExpectedOutput(final int number, Class<? extends Value> firstFieldType,
			Class<?>... otherFieldTypes) {
		return this.getExpectedOutput(this.getDataSinks().get(number), firstFieldType, otherFieldTypes);
	}

	/**
	 * Returns the expected output {@link TestPairs} associated with the
	 * <i>i</i>th expected output of the TestPlan. If multiple contracts are
	 * tested in the TestPlan, it is recommended to use the {@link #getExpectedOutput(GenericDataSink)} method to
	 * unambiguously set
	 * the values.
	 * 
	 * @param number
	 *        the number of the expected output.
	 * @return the <i>i</i>th expected output of the TestPlan
	 */
	public TestRecords getExpectedOutput(final int number, Class<? extends Value>[] schema) {
		return this.getExpectedOutput(this.getDataSinks().get(number), schema);
	}

	/**
	 * Returns the default fuzzy global matcher of all data sinks.
	 * 
	 * @return the global matcher
	 */
	public FuzzyValueMatcher getFuzzyMatcher() {
		return this.fuzzyMatchers.get(ALL_SINKS);
	}

	/**
	 * Returns the global matcher of the given data sink. If no measure has been explicitly set for this sink,
	 * the matcher for all sinks is returned if set.
	 * 
	 * @param sink
	 *        the data sink
	 * @param <V>
	 *        the value type
	 * @return the global matcher
	 */
	public FuzzyValueMatcher getFuzzyMatcher(GenericDataSink sink) {
		FuzzyValueMatcher matcher = this.fuzzyMatchers.get(sink);
		if (matcher == null)
			matcher = this.fuzzyMatchers.get(ALL_SINKS);
		return matcher;
	}

	/**
	 * Returns the default fuzzy similarity measure of all data sinks.
	 * 
	 * @return the similarity measure
	 */
	public List<ValueSimilarity<?>> getFuzzySimilarities() {
		return this.getFuzzySimilarities(ALL_SINKS, TestRecords.ALL_VALUES);
	}

	/**
	 * Returns the fuzzy similarity measure of the given data sink. If no measure has been explicitly set for this sink,
	 * the measure for all sinks is returned if set.
	 * 
	 * @param sink
	 *        the data sink
	 * @param <V>
	 *        the value type
	 * @return the similarity measure
	 */
	public List<ValueSimilarity<?>> getFuzzySimilarities(GenericDataSink sink, int valueIndex) {
		Int2ObjectMap<List<ValueSimilarity<?>>> indexMap = this.getFuzzySimilarityIndexMap(sink);
		List<ValueSimilarity<?>> list = indexMap.get(valueIndex);
		if (list == null)
			indexMap.put(valueIndex, list = new ArrayList<ValueSimilarity<?>>());
		return list;
	}

	/**
	 * Returns the first input {@link TestPairs} of the TestPlan. If multiple
	 * contracts are tested in the TestPlan, it is recommended to use the {@link #getInput(GenericDataSource<?>)} method
	 * to unambiguously set the
	 * values.
	 * 
	 * @return the first input of the TestPlan
	 */
	public TestRecords getInput() {
		return this.getInput(0);
	}

	/**
	 * Returns the input {@link TestPairs} of the TestPlan associated with the
	 * given source. This is the recommended method to set input pairs for more
	 * complex TestPlans.
	 * 
	 * @param <K>
	 *        the type of the key
	 * @param <V>
	 *        the type of the value
	 * @param source
	 *        the source of which the associated input TestPairs should be
	 *        returned
	 * @return the input {@link TestPairs} of the TestPlan associated with the
	 *         given source
	 */
	public TestRecords getInput(final GenericDataSource<?> source) {
		TestRecords values = this.inputs.get(source);
		if (values == null)
			this.inputs.put(source, values = new TestRecords());
		return values;
	}

	/**
	 * Returns the input {@link TestPairs} associated with the <i>i</i>th input
	 * of the TestPlan. If multiple contracts are tested in the TestPlan, it is
	 * recommended to use the {@link #getInput(GenericDataSource<?>)} method to
	 * unambiguously set the values.
	 * 
	 * @param number
	 *        the number of the input.
	 * @return the <i>i</i>th input of the TestPlan
	 */
	public TestRecords getInput(final int number) {
		return this.getInput(this.getDataSources().get(number));
	}

	/**
	 * Traverses the test plan and returns the first contracts that process the
	 * data of the given contract.
	 * 
	 * @param contract
	 *        the contract of which one preceding contracts should be
	 *        returned
	 * @return returns the first contract that process the data of the given
	 *         contract
	 */
	public Contract getOutputOfContract(Contract contract) {
		return this.getOutputsOfContract(contract)[0];
	}

	/**
	 * Traverses the test plan and returns all contracts that process the data
	 * of the given contract.
	 * 
	 * @param contract
	 *        the contract of which preceding contracts should be returned
	 * @return returns all contracts that process the data of the given contract
	 */
	public Contract[] getOutputsOfContract(final Contract contract) {
		final ArrayList<Contract> outputs = new ArrayList<Contract>();

		for (final Contract sink : this.sinks)
			sink.accept(new Visitor<Contract>() {
				LinkedList<Contract> outputStack = new LinkedList<Contract>();

				@Override
				public void postVisit(final Contract visitable) {
				}

				@Override
				public boolean preVisit(final Contract visitable) {
					if (visitable == contract)
						outputs.add(this.outputStack.peek());
					this.outputStack.push(visitable);
					return true;
				}
			});

		return outputs.toArray(new Contract[outputs.size()]);
	}

	/**
	 * Returns all {@link GenericDataSink}s of this test plan.
	 * 
	 * @return the sinks
	 */
	public List<FileDataSink> getSinks() {
		return this.sinks;
	}

	/**
	 * Returns the sources.
	 * 
	 * @return the sources
	 */
	public List<FileDataSource> getSources() {
		return this.sources;
	}

	/**
	 * Removes the fuzzy global matcher of the given data sink.
	 * 
	 * @param sink
	 *        the data sink
	 */
	public void removeFuzzyValueMatcher(GenericDataSink sink) {
		this.fuzzyMatchers.remove(sink);
	}

	/**
	 * Removes the fuzzy similarity measure of the given data sink.
	 * 
	 * @param sink
	 *        the data sink
	 */
	public void removeFuzzyValueSimilarity(GenericDataSink sink) {
		this.fuzzySimilarity.remove(sink);
	}

	/**
	 * Compiles the plan to an {@link ExecutionGraph} and executes it. If
	 * expected values have been specified, the actual outputs values are
	 * compared to the expected values.
	 */
	public void run() {
		this.errorHandlers.clear();
		try {
			ExecutionGraph eg = this.getExecutionGraph();
			this.addErrorHandler(eg);
			this.localScheduler = new LocalScheduler(MockDeployManager.INSTANCE, MockInstanceManager.INSTANCE);
			MockTaskManager.INSTANCE.addJobGraph(eg);
			eg.registerJobStatusListener(MockJobManager.INSTANCE);
			this.localScheduler.schedulJob(eg);
			this.execute(eg);
		} catch (final Exception e) {
			Assert.fail("plan scheduling: " + StringUtils.stringifyException(e));
		}

		for (ExecutionExceptionHandler errorHandler : this.errorHandlers)
			// these fields are set by the ExecutionExceptionHandler in case of error
			if (errorHandler.executionError != null)
				Assert.fail(String.format("Error @ %s: %s", errorHandler.executionVertex.getName(),
					errorHandler.executionError));

		try {
			this.validateResults();
		} finally {
			this.errorHandlers.clear();
			try {
				this.close();
			} catch (IOException e) {
			}
		}
	}

	/**
	 * Compiles the plan to an {@link ExecutionGraph} and executes it. If
	 * expected values have been specified, the actual outputs values are
	 * compared to the expected values.
	 */
	public void runWithMiniCluster() {
		try {
			Configuration config = new Configuration();

			// local ip as job manager (localhost or 127.0.0.1 does not work)
			config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
			JobManager jobManager = new JobManager(this.getNepheleConf(), "local");

			PactCompiler pc = new PactCompiler();
			Plan plan = this.buildPlanWithReadableSinks();
			OptimizedPlan op = pc.compile(plan);

			JobGraphGenerator jgg = new JobGraphGenerator();
			JobGraph jobGraph = jgg.compileJobGraph(op);

			jobManager.submitJob(jobGraph);

			Configuration configuration = jobGraph.getJobConfiguration();
			configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");

			// submit
			JobClient jobClient = new JobClient(jobGraph, configuration);
			jobClient.submitJobAndWait();
		} catch (CompilerException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (JobExecutionException e1) {
			e1.printStackTrace();
		}

		try {
			this.validateResults();
		} finally {
			try {
				this.close();
			} catch (IOException e) {
			}
		}
	}

	/**
	 * Set the allowed delta for PactDouble values to match expected and actual values that differ due to inaccuracies
	 * in the floating point calculation.
	 * 
	 * @param delta
	 *        the delta that the actual value is allowed to differ from the expected value.
	 */
	public void setAllowedPactDoubleDelta(double delta) {
		ListIterator<ValueSimilarity<?>> simIter =
			this.getFuzzySimilarities(ALL_SINKS, TestRecords.ALL_VALUES).listIterator();
		// replace existing
		while (simIter.hasNext()) {
			ValueSimilarity<?> sim = simIter.next();
			if (sim instanceof DoubleValueSimilarity) {
				simIter.set(new DoubleValueSimilarity(delta));
				return;
			}
		}
		// or add new
		this.addFuzzyValueSimilarity(new DoubleValueSimilarity(delta));
	}

	/**
	 * Sets the degreeOfParallelism to the specified value.
	 * 
	 * @param degreeOfParallelism
	 *        the degreeOfParallelism to set
	 */
	public void setDegreeOfParallelism(final int degreeOfParallelism) {
		this.degreeOfParallelism = degreeOfParallelism;
	}

	/**
	 * Sets a fuzzy global matcher for the values of all data sinks.
	 * 
	 * @param matcher
	 *        the global matcher to use
	 */
	public void setFuzzyValueMatcher(FuzzyValueMatcher matcher) {
		this.fuzzyMatchers.put(ALL_SINKS, matcher);
	}

	/**
	 * Sets a fuzzy global matcher for the values of the given data sink.
	 * 
	 * @param <V>
	 *        the value type
	 * @param sink
	 *        the data sink
	 * @param matcher
	 *        the global matcher to use
	 */
	public void setFuzzyValueMatcher(GenericDataSink sink, FuzzyValueMatcher matcher) {
		this.fuzzyMatchers.put(sink, matcher);
	}

	protected void addErrorHandler(ExecutionGraph eg) {
		ExecutionGraphIterator executionGraphIterator = new ExecutionGraphIterator(eg, true);
		while (executionGraphIterator.hasNext()) {
			ExecutionVertex executionVertex = executionGraphIterator.next();
			ExecutionExceptionHandler errorHandler = new ExecutionExceptionHandler(executionVertex);
			executionVertex.registerExecutionListener(errorHandler);
			this.errorHandlers.add(errorHandler);
		}
	}

	protected Int2ObjectMap<List<ValueSimilarity<?>>> getFuzzySimilarityIndexMap(GenericDataSink sink) {
		Int2ObjectMap<List<ValueSimilarity<?>>> indexMap = this.fuzzySimilarity.get(sink);
		if (indexMap == null)
			this.fuzzySimilarity.put(sink, indexMap = new Int2ObjectArrayMap<List<ValueSimilarity<?>>>());
		return indexMap;
	}

	/**
	 * Actually builds the plan but guarantees that the output can be read
	 * without additional knowledge. Currently the {@link SequentialOutputFormat} is used for a guaranteed
	 * deserializable
	 * output.<br>
	 * If a data source is not {@link SequentialOutputFormat}, it is replaced by
	 * a {@link SplittingOutputFormat}, with two outputs: the original one and
	 * one {@link SequentialOutputFormat}.
	 */
	private Plan buildPlanWithReadableSinks() {
		final Collection<FileDataSink> existingSinks = this.getDataSinks();
		final Collection<GenericDataSink> wrappedSinks = new ArrayList<GenericDataSink>();
		for (final FileDataSink fileSink : existingSinks)
			// need a format which is deserializable without configuration
			if (!fileSink.getFormatClass().equals(SequentialOutputFormat.class)) {
				TestRecords expectedValues = this.expectedOutputs.get(fileSink);

				final FileDataSink safeSink = createDefaultSink(fileSink.getName());

				safeSink.setInputs(fileSink.getInputs());

				wrappedSinks.add(fileSink);
				wrappedSinks.add(safeSink);

				// only add to expected outputs if we need to check for values
				if (expectedValues != null)
					this.expectedOutputs.put(safeSink, expectedValues);
				this.actualOutputs.put(safeSink, this.getActualOutput(fileSink));
				this.getActualOutput(fileSink).fromFile(SequentialInputFormat.class, safeSink.getFilePath());

			} else {
				wrappedSinks.add(fileSink);
				this.getActualOutput(fileSink).fromFile(SequentialInputFormat.class, fileSink.getFilePath());
			}

		return new Plan(wrappedSinks);
	}

	private OptimizedPlan compile(final Plan plan) {
		final OptimizedPlan optimizedPlan =
			new PactCompiler(null, new CostEstimator(), new InetSocketAddress(0)).compile(
				plan, MOCK_INSTANCE_DESCRIPTION);
		return optimizedPlan;
	}

	/**
	 * 
	 */
	private void configureSinksAndSources() {
		for (FileDataSink sink : this.sinks)
			sink.getParameters().setLong(FileOutputFormat.OUTPUT_STREAM_OPEN_TIMEOUT_KEY, 0);
		for (FileDataSource source : this.sources)
			source.getParameters().setLong(FileInputFormat.INPUT_STREAM_OPEN_TIMEOUT_KEY, 0);
	}

	/**
	 * Locally executes the {@link ExecutionGraph}.
	 */
	private void execute(final ExecutionGraph eg) {
		while (!eg.isExecutionFinished() && eg.getJobStatus() != InternalJobStatus.FAILED)
			try {
				Thread.sleep(10);
			} catch (final InterruptedException e) {
			}
	}

	/**
	 * Traverses the plan for all sinks and sources.
	 */
	private void findSinksAndSources() {
		for (final Contract contract : this.contracts)
			contract.accept(new Visitor<Contract>() {
				@Override
				public void postVisit(final Contract visitable) {
				}

				@Override
				public boolean preVisit(final Contract visitable) {
					if (visitable instanceof FileDataSink && !TestPlan.this.sinks.contains(visitable))
						TestPlan.this.sinks.add((FileDataSink) visitable);
					if (visitable instanceof FileDataSource && !TestPlan.this.sources.contains(visitable))
						TestPlan.this.sources.add((FileDataSource) visitable);
					return true;
				}
			});

		for (FileDataSource source : this.sources)
			this.getInput(source).fromFile(source.getFormatClass(), source.getFilePath(), source.getParameters());
	}

	private List<FileDataSink> getDataSinks() {
		return this.sinks;
	}

	private List<? extends GenericDataSource<?>> getDataSources() {
		return this.sources;
	}

	private ExecutionGraph getExecutionGraph() throws IOException, GraphConversionException {
		final Plan plan = this.buildPlanWithReadableSinks();
		this.syncDegreeOfParallelism(plan);
		this.initAdhocInputs();

		final OptimizedPlan optimizedPlan = this.compile(plan);
		this.replaceShippingStrategy(optimizedPlan);
		final JobGraph jobGraph = new JobGraphGenerator().compileJobGraph(optimizedPlan);
		for (AbstractJobVertex vertex : jobGraph.getAllJobVertices())
			vertex.setNumberOfExecutionRetries(0);
		LibraryCacheManager.register(jobGraph.getJobID(), new String[0]);
		return new ExecutionGraph(jobGraph, MockInstanceManager.INSTANCE);
	}

	/**
	 * @return
	 */
	private String getNepheleConf() throws IOException {
		File nepheleConfigDir = File.createTempFile("nephele", null);
		nepheleConfigDir.delete();
		nepheleConfigDir.mkdir();

		// config
		final String nepheleConfigDirJob = nepheleConfigDir.getAbsolutePath();
		final int jobManagerRpcPort = ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT;
		final int rpcPort = 6501, dataPort = 7501;

		FileWriter writer = new FileWriter(new File(nepheleConfigDir, "nephele-user.xml"));
		String lines[] = {
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
			"<configuration>",
			"    <property>",
			"        <key>jobmanager.instancemanager.local.classname</key>",
			"        <value>" + LocalInstanceManager.class.getName() + "</value>",
			"    </property>",
			"    <property>",
			"        <key>jobmanager.scheduler.local.classname</key>",
			"        <value>" + LocalScheduler.class.getName() + "</value>",
			"    </property>",
			"    <property>",
			"        <key>" + ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY + "</key>",
			"        <value>localhost</value>",
			// "        <value>" + getLocalIpAddress() + "</value>",
			"    </property>",
			"    <property>",
			"        <key>" + ConfigConstants.JOB_MANAGER_IPC_PORT_KEY + "</key>",
			"        <value>" + jobManagerRpcPort + "</value>",
			"    </property>",
			"    <property>",
			"        <key>" + ConfigConstants.TASK_MANAGER_IPC_PORT_KEY + "</key>",
			"        <value>" + rpcPort + "</value>",
			"    </property>",
			"    <property>",
			"        <key>" + ConfigConstants.TASK_MANAGER_DATA_PORT_KEY + "</key>",
			"        <value>" + dataPort + "</value>",
			"    </property>",
			"    <property>",
			"        <key>taskmanager.setup.usediscovery</key>",
			"        <value>false</value>",
			"    </property>",
			"    <property>",
			"        <key>jobmanager.visualization.enable</key>",
			"        <value>false</value>",
			"    </property>",
			"</configuration>" };
		for (String line : lines)
			writer.write(line);
		writer.close();

		return nepheleConfigDirJob;
	}

	private void initAdhocInputs() throws IOException {
		for (final FileDataSource source : this.sources) {
			final TestRecords input = this.getInput(source);
			if (input.isAdhoc())
				input.saveToFile(source.getFilePath());
		}
	}

	private Int2ObjectMap<List<ValueSimilarity<?>>> mergeSimilarityIndices(
			Int2ObjectMap<List<ValueSimilarity<?>>> similarityMap) {
		similarityMap = new Int2ObjectOpenHashMap<List<ValueSimilarity<?>>>(similarityMap);
		for (Entry<List<ValueSimilarity<?>>> entry : this.getFuzzySimilarityIndexMap(ALL_SINKS).int2ObjectEntrySet())
			if (!entry.getValue().isEmpty()) {
				List<ValueSimilarity<?>> existingList = similarityMap.get(entry.getKey());
				if (existingList == null)
					similarityMap.put(entry.getKey(), entry.getValue());
				else
					existingList.addAll(entry.getValue());
			}
		return similarityMap;
	}

	private void replaceShippingStrategy(final OptimizedPlan optimizedPlan) {
		// final Field declaredField =
		// PactConnection.class.getDeclaredField("shipStrategy");
		// declaredField.setAccessible(true);
		for (final OptimizerNode node : optimizedPlan.getAllNodes()) {
			for (final PactConnection pactConnection : node.getIncomingConnections())
				pactConnection.setShipStrategy(new ForwardSS());
			for (final PactConnection pactConnection : node.getOutConns())
				// declaredField.set(pactConnection, ShipStrategy.FORWARD);
				pactConnection.setShipStrategy(new ForwardSS());
		}
	}

	/**
	 * Sets the degree of parallelism for every node in the plan.
	 */
	private void syncDegreeOfParallelism(final Plan plan) {
		plan.accept(new Visitor<Contract>() {

			@Override
			public void postVisit(final Contract visitable) {
			}

			@Override
			public boolean preVisit(final Contract visitable) {
				int degree = TestPlan.this.getDegreeOfParallelism();
				if (visitable instanceof GenericDataSource<?>)
					degree = 1;
				else if (degree > 1 && visitable instanceof FileDataSink)
					try {
						Path path = new Path(((FileDataSink) visitable).getFilePath());

						final FileSystem fs = path.getFileSystem();

						final FileStatus f = fs.getFileStatus(path);

						if (!f.isDir()) {
							fs.delete(path, false);
							fs.mkdirs(path);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				if (visitable.getDegreeOfParallelism() == -1)
					visitable.setDegreeOfParallelism(degree);
				return true;
			}
		});
	}

	@SuppressWarnings("unchecked")
	private void validateResults() {
		for (final FileDataSink sinkContract : this.getDataSinks()) {
			TestRecords expectedValues = this.expectedOutputs.get(sinkContract);
			// need a format which is deserializable without configuration
			if (sinkContract.getFormatClass() == SequentialOutputFormat.class && expectedValues != null
				&& expectedValues.isInitialized()) {
				final TestRecords actualValues = new TestRecords();
				actualValues.fromFile(SequentialInputFormat.class, sinkContract.getFilePath());

				FuzzyValueMatcher fuzzyMatcher = this.getFuzzyMatcher(sinkContract);
				Int2ObjectMap<List<ValueSimilarity<?>>> similarityMap = this.getFuzzySimilarityIndexMap(sinkContract);
				if (similarityMap == null)
					similarityMap = Int2ObjectMaps.EMPTY_MAP;
				if (!this.getFuzzySimilarityIndexMap(ALL_SINKS).isEmpty())
					similarityMap = this.mergeSimilarityIndices(similarityMap);
				try {
					actualValues.assertEquals(expectedValues, fuzzyMatcher, similarityMap);
				} catch (AssertionError e) {
					AssertionError assertionError = new AssertionError(sinkContract.getName() + ": " + e.getMessage());
					assertionError.initCause(e.getCause());
					throw assertionError;
				}
			}
		}
	}

	/**
	 * Creates a default sink with the given name. This sink may be used with ad-hoc values added to the corresponding
	 * {@link TestPairs}.
	 * 
	 * @param name
	 *        the name of the sink
	 * @return the created sink
	 */
	public static FileDataSink createDefaultSink(final String name) {
		return new FileDataSink(SequentialOutputFormat.class, getTestPlanFile("output"), name);
	}

	/**
	 * Creates a default source with the given name. This sink may be used with ad-hoc values added to the corresponding
	 * {@link TestPairs}.
	 * 
	 * @param name
	 *        the name of the source
	 * @return the created source
	 */
	public static FileDataSource createDefaultSource(final String name) {
		return new FileDataSource(SequentialInputFormat.class, getTestPlanFile("input"), name);
	}

	static String getTestPlanFile(final String prefix) {
		return createTemporaryFile("testPlan", prefix);
	}

	private static String createTemporaryFile(String suffix, String prefix) {
		try {
			final File tempFile = File.createTempFile(suffix, prefix);
			tempFile.deleteOnExit();
			return tempFile.toURI().toString();
		} catch (final IOException e) {
			throw new IllegalStateException("Cannot create temporary file for prefix " + prefix, e);
		}
	}

	private static final class CostEstimator extends FixedSizeClusterCostEstimator {
		private CostEstimator() {
			super();
		}
		//
		// @Override
		// public void getBroadcastCost(OptimizerNode target, OptimizerNode
		// source, Costs costs) {
		// costs.setNetworkCost(Long.MAX_VALUE);
		// }
	}

	private static final class ExecutionExceptionHandler implements ExecutionListener {
		private String executionError;

		public ExecutionVertex executionVertex;

		public ExecutionExceptionHandler(ExecutionVertex executionVertex) {
			this.executionVertex = executionVertex;
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.nephele.execution.ExecutionListener#executionStateChanged(eu.stratosphere.nephele.jobgraph
		 * .JobID, eu.stratosphere.nephele.executiongraph.ExecutionVertexID,
		 * eu.stratosphere.nephele.execution.ExecutionState, java.lang.String)
		 */
		@Override
		public void executionStateChanged(JobID jobID, ExecutionVertexID vertexID, ExecutionState newExecutionState,
				String optionalMessage) {
			if (newExecutionState == ExecutionState.FAILED && this.executionError == null)
				this.executionError = optionalMessage == null ? "FAILED" : optionalMessage;
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.nephele.execution.ExecutionListener#getPriority()
		 */
		@Override
		public int getPriority() {
			return 13;
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.nephele.execution.ExecutionListener#userThreadFinished(eu.stratosphere.nephele.jobgraph.JobID
		 * , eu.stratosphere.nephele.executiongraph.ExecutionVertexID, java.lang.Thread)
		 */
		@Override
		public void userThreadFinished(JobID jobID, ExecutionVertexID vertexID, Thread userThread) {
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.nephele.execution.ExecutionListener#userThreadStarted(eu.stratosphere.nephele.jobgraph.JobID,
		 * eu.stratosphere.nephele.executiongraph.ExecutionVertexID, java.lang.Thread)
		 */
		@Override
		public void userThreadStarted(JobID jobID, ExecutionVertexID vertexID, Thread userThread) {
		}

	}

}