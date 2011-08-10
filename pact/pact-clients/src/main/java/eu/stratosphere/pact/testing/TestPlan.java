/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.GraphConversionException;
import eu.stratosphere.nephele.executiongraph.InternalJobStatus;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.InstanceTypeDescriptionFactory;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.DeploymentManager;
import eu.stratosphere.nephele.jobmanager.scheduler.local.LocalScheduler;
import eu.stratosphere.nephele.jobmanager.splitassigner.InputSplitManager;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult;
import eu.stratosphere.nephele.taskmanager.TaskSubmissionResult;
import eu.stratosphere.nephele.template.InputSplit;
import eu.stratosphere.nephele.template.InputSplitProvider;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.util.PactConfigConstants;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.costs.FixedSizeClusterCostEstimator;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;
import eu.stratosphere.pact.compiler.plan.PactConnection;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.testing.ioformats.SequentialInputFormat;
import eu.stratosphere.pact.testing.ioformats.SequentialOutputFormat;

/**
 * The primary resource to test one or more implemented PACT stubs. It is
 * created in a unit tests and performs the following operations.
 * <ul>
 * <li>Adds {@link FileDataSourceContract}s and {@link FileDataSinkContract}s if not explicitly specified,
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
 *    FileDataSourceContract&lt;Key, Value&gt; source = ...;
 *    MapContract&lt;Key, Value, Key, Value&gt; map = new MapContract&lt;Key, Value, Key, Value&gt;(IdentityMap.class, "Map");
 *    map.setInput(source);    
 *    FileDataSinkContract&lt;Key, Value&gt; output = ...;
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

public class TestPlan implements Closeable, DeploymentManager {

	private final class MockInputSplitProvider implements InputSplitProvider {
		private ExecutionVertex vertex;

		public MockInputSplitProvider(ExecutionVertex vertex) {
			this.vertex = vertex;
		}

		@Override
		public InputSplit getNextInputSplit() {
			return inputSplitManager.getNextInputSplit(vertex);
		}
	}

	private static final class CostEstimator extends
			FixedSizeClusterCostEstimator {
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

	private final class ExecutionExceptionHandler implements ExecutionListener {
		private final ExecutionVertex executionVertex;

		private String executionError;

		private ExecutionExceptionHandler(final ExecutionVertex executionVertex) {
			this.executionVertex = executionVertex;
		}

		@Override
		public void executionStateChanged(final Environment ee,
				final ExecutionState newExecutionState,
				final String optionalMessage) {
			if (newExecutionState == ExecutionState.FAILED) {
				this.executionError = optionalMessage == null ? "FAILED" : optionalMessage;
				ee.cancelExecution();
			}
		}

		@Override
		public void userThreadFinished(final Environment ee,
				final Thread userThread) {
		}

		@Override
		public void userThreadStarted(final Environment ee,
				final Thread userThread) {
		}
	}

	private static final InstanceTypeDescription MOCK_INSTANCE_DESCRIPTION = InstanceTypeDescriptionFactory.construct(
		MockInstanceManager.DEFAULT_INSTANCE_TYPE, MockInstance.DESCRIPTION, 1);

	private static FileDataSinkContract<?, ?> ALL_SINKS = null;

	private final Map<FileDataSinkContract<?, ?>, TestPairs<?, ?>> actualOutputs = new IdentityHashMap<FileDataSinkContract<?, ?>, TestPairs<?, ?>>();

	private final Contract[] contracts;

	private int degreeOfParallelism = 1;

	private final Map<FileDataSinkContract<?, ?>, TestPairs<?, ?>> expectedOutputs = new IdentityHashMap<FileDataSinkContract<?, ?>, TestPairs<?, ?>>();

	private final Map<FileDataSourceContract<?, ?>, TestPairs<?, ?>> inputs = new IdentityHashMap<FileDataSourceContract<?, ?>, TestPairs<?, ?>>();

	private final List<FileDataSinkContract<?, ?>> sinks = new ArrayList<FileDataSinkContract<?, ?>>();

	private final List<FileDataSourceContract<?, ?>> sources = new ArrayList<FileDataSourceContract<?, ?>>();

	private final Map<FileDataSinkContract<?, ?>, FuzzyTestValueSimilarity<?>> fuzzySimilarity = new HashMap<FileDataSinkContract<?, ?>, FuzzyTestValueSimilarity<?>>();

	private final Map<FileDataSinkContract<?, ?>, FuzzyTestValueMatcher<?>> fuzzyMatchers = new HashMap<FileDataSinkContract<?, ?>, FuzzyTestValueMatcher<?>>();

	private InputSplitManager inputSplitManager = new InputSplitManager();

	/**
	 * Initializes TestPlan with the given {@link Contract}s. Like the original {@link Plan}, the contracts may be
	 * {@link FileDataSinkContract}s. However, it
	 * is also possible to add arbitrary Contracts, to which FileDataSinkContracts
	 * are automatically added.
	 * 
	 * @param contracts
	 *        a list of Contracts with at least one element.
	 */
	public TestPlan(final Contract... contracts) {
		if (contracts.length == 0)
			throw new IllegalArgumentException();

		this.fuzzyMatchers.put(ALL_SINKS, new EqualityValueMatcher<Value>());

		final Configuration config = new Configuration();
		config.setString(PactConfigConstants.DEFAULT_INSTANCE_TYPE_KEY,
			"standard,1,1,200,1,1");
		GlobalConfiguration.includeConfiguration(config);

		this.contracts = new InputOutputAdder().process(contracts);

		this.findSinksAndSources();
	}

	/**
	 * Initializes TestPlan with the given {@link Contract}s. Like the original {@link Plan}, the contracts may be
	 * {@link FileDataSinkContract}s. However, it
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
	 * Returns all {@link FileDataSinkContract}s of this test plan.
	 * 
	 * @return the sinks
	 */
	public List<FileDataSinkContract<?, ?>> getSinks() {
		return this.sinks;
	}

	/**
	 * Set the allowed delta for PactDouble values to match expected and actual values that differ due to inaccuracies
	 * in the floating point calculation.
	 * 
	 * @param delta
	 *        the delta that the actual value is allowed to differ from the expected value.
	 */
	public void setAllowedPactDoubleDelta(double delta) {
		setFuzzyValueMatcher(new NaiveFuzzyValueMatcher<PactDouble>());
		setFuzzyValueSimilarity(new DoubleValueSimilarity(delta));
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
	public <V extends Value> void setFuzzyValueSimilarity(FileDataSinkContract<?, ? extends V> sink,
			FuzzyTestValueSimilarity<V> similarity) {
		this.fuzzySimilarity.put(sink, similarity);
	}

	/**
	 * Sets a fuzzy similarity measure for the values of all data sinks.
	 * 
	 * @param similarity
	 *        the similarity measure to use
	 */
	public void setFuzzyValueSimilarity(FuzzyTestValueSimilarity<?> similarity) {
		this.fuzzySimilarity.put(ALL_SINKS, similarity);
	}

	/**
	 * Removes the fuzzy similarity measure of the given data sink.
	 * 
	 * @param sink
	 *        the data sink
	 */
	public void removeFuzzyValueSimilarity(FileDataSinkContract<?, ?> sink) {
		this.fuzzySimilarity.remove(sink);
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
	@SuppressWarnings("unchecked")
	public <V extends Value> FuzzyTestValueSimilarity<V> getFuzzySimilarity(FileDataSinkContract<?, ? extends V> sink) {
		FuzzyTestValueSimilarity<?> matcher = this.fuzzySimilarity.get(sink);
		if (matcher == null)
			matcher = this.fuzzySimilarity.get(ALL_SINKS);
		return (FuzzyTestValueSimilarity<V>) matcher;
	}

	/**
	 * Returns the default fuzzy similarity measure of all data sinks.
	 * 
	 * @return the similarity measure
	 */
	public FuzzyTestValueSimilarity<?> getFuzzySimilarity() {
		return this.fuzzySimilarity.get(ALL_SINKS);
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
	public <V extends Value> void setFuzzyValueMatcher(FileDataSinkContract<?, ? extends V> sink,
			FuzzyTestValueMatcher<V> matcher) {
		this.fuzzyMatchers.put(sink, matcher);
	}

	/**
	 * Sets a fuzzy global matcher for the values of all data sinks.
	 * 
	 * @param matcher
	 *        the global matcher to use
	 */
	public void setFuzzyValueMatcher(FuzzyTestValueMatcher<?> matcher) {
		this.fuzzyMatchers.put(ALL_SINKS, matcher);
	}

	/**
	 * Removes the fuzzy global matcher of the given data sink.
	 * 
	 * @param sink
	 *        the data sink
	 */
	public void removeFuzzyValueMatcher(FileDataSinkContract<?, ?> sink) {
		this.fuzzyMatchers.remove(sink);
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
	@SuppressWarnings("unchecked")
	public <V extends Value> FuzzyTestValueMatcher<V> getFuzzyMatcher(FileDataSinkContract<?, ? extends V> sink) {
		FuzzyTestValueMatcher<?> matcher = this.fuzzyMatchers.get(sink);
		if (matcher == null)
			matcher = this.fuzzyMatchers.get(ALL_SINKS);
		return (FuzzyTestValueMatcher<V>) matcher;
	}

	/**
	 * Returns the default fuzzy global matcher of all data sinks.
	 * 
	 * @return the global matcher
	 */
	public FuzzyTestValueMatcher<?> getFuzzyMatcher() {
		return this.fuzzyMatchers.get(ALL_SINKS);
	}

	/**
	 * Allowed delta for PactDouble values, default value is 0.
	 * 
	 * @return the allowed delta
	 */
	public double getAllowedPactDoubleDelta() {
		FuzzyTestValueSimilarity<?> matcher = this.fuzzySimilarity.get(ALL_SINKS);
		if (matcher instanceof DoubleValueSimilarity)
			return ((DoubleValueSimilarity) matcher).getDelta();
		return 0;
	}

	/**
	 * Locally executes the {@link ExecutionGraph}.
	 */
	private void execute(final ExecutionGraph eg) {
		while (!eg.isExecutionFinished()
			&& eg.getJobStatus() != InternalJobStatus.FAILED) {

			try {
				Thread.sleep(10);
			} catch (final InterruptedException e) {
			}
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
					if (visitable instanceof FileDataSinkContract<?, ?>
						&& !TestPlan.this.sinks.contains(visitable))
						TestPlan.this.sinks
							.add((FileDataSinkContract<?, ?>) visitable);
					if (visitable instanceof FileDataSourceContract<?, ?>
						&& !TestPlan.this.sources.contains(visitable))
						TestPlan.this.sources
							.add((FileDataSourceContract<?, ?>) visitable);
					return true;
				}
			});

		for (FileDataSourceContract<?, ?> source : this.sources) {
			getInput(source).fromFile(source.getFormatClass(), source.getFilePath());
		}
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
		final Collection<FileDataSinkContract<?, ?>> existingSinks = this
			.getDataSinks();
		final Collection<FileDataSinkContract<?, ?>> wrappedSinks = new ArrayList<FileDataSinkContract<?, ?>>();
		for (final FileDataSinkContract<?, ?> FileDataSinkContract : existingSinks)
			// need a format which is deserializable without configuration
			if (!FileDataSinkContract.getFormatClass().equals(SequentialOutputFormat.class)) {

				final FileDataSinkContract<Key, Value> safeSink = createDefaultSink(FileDataSinkContract.getName());

				safeSink.setInput(FileDataSinkContract.getInput());

				wrappedSinks.add(FileDataSinkContract);
				wrappedSinks.add(safeSink);

				this.expectedOutputs.put(safeSink, this.getExpectedOutput(FileDataSinkContract));
				this.actualOutputs.put(safeSink, this.getActualOutput(FileDataSinkContract));
				this.getActualOutput(FileDataSinkContract)
					.fromFile(SequentialInputFormat.class, safeSink.getFilePath());

			} else {
				wrappedSinks.add(FileDataSinkContract);
				this.getActualOutput(FileDataSinkContract).fromFile(
					SequentialInputFormat.class,
					FileDataSinkContract.getFilePath());
			}

		return new Plan(wrappedSinks);
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
				if (visitable instanceof FileDataSourceContract<?, ?>) {
					degree = 1;
				} else if (degree > 1 && visitable instanceof FileDataSinkContract<?, ?>) {
					try {
						Path path = new Path(
							((FileDataSinkContract<?, ?>) visitable)
								.getFilePath());

						final FileSystem fs = path.getFileSystem();

						final FileStatus f = fs.getFileStatus(path);

						if (!f.isDir()) {
							fs.delete(path, false);
							fs.mkdirs(path);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				visitable.setDegreeOfParallelism(degree);
				return true;
			}
		});
	}

	/**
	 * Returns the first output {@link TestPairs} of the TestPlan. If multiple
	 * contracts are tested in the TestPlan, it is recommended to use the {@link #getActualOutput(FileDataSinkContract)}
	 * method to unambiguously get
	 * the values.<br>
	 * The values are only meaningful after a {@link #run()}.
	 * 
	 * @return the first output of the TestPlan
	 */
	public TestPairs<Key, Value> getActualOutput() {
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
	@SuppressWarnings("unchecked")
	public <K extends Key, V extends Value> TestPairs<K, V> getActualOutput(
			final FileDataSinkContract<K, V> sink) {
		TestPairs<K, V> values = (TestPairs<K, V>) this.actualOutputs.get(sink);
		if (values == null)
			this.actualOutputs.put(sink, values = new TestPairs<K, V>());
		return values;
	}

	/**
	 * Returns the output {@link TestPairs} associated with the <i>i</i>th
	 * output of the TestPlan. If multiple contracts are tested in the TestPlan,
	 * it is recommended to use the {@link #getActualOutput(FileDataSinkContract)} method to unambiguously get the
	 * values.<br>
	 * The values are only meaningful after a {@link #run()}.
	 * 
	 * @param number
	 *        the number of the output.
	 * @return the <i>i</i>th output of the TestPlan
	 */
	@SuppressWarnings("unchecked")
	public TestPairs<Key, Value> getActualOutput(final int number) {
		return (TestPairs<Key, Value>) this.getActualOutput(this.getDataSinks()
			.get(number));
	}

	private List<FileDataSinkContract<?, ?>> getDataSinks() {
		return this.sinks;
	}

	private List<? extends FileDataSourceContract<?, ?>> getDataSources() {
		return this.sources;
	}

	/**
	 * Returns the degreeOfParallelism.
	 * 
	 * @return the degreeOfParallelism
	 */
	public int getDegreeOfParallelism() {
		return this.degreeOfParallelism;
	}

	private ExecutionGraph getExecutionGraph() throws IOException,
			GraphConversionException {
		final Plan plan = this.buildPlanWithReadableSinks();
		this.syncDegreeOfParallelism(plan);
		this.initAdhocInputs();

		final OptimizedPlan optimizedPlan = this.compile(plan);
		this.replaceShippingStrategy(optimizedPlan);
		final JobGraph jobGraph = new JobGraphGenerator()
			.compileJobGraph(optimizedPlan);
		LibraryCacheManager.register(jobGraph.getJobID(), new String[0]);
		final ExecutionGraph eg = new ExecutionGraph(jobGraph, MockInstanceManager.INSTANCE);
		return eg;
	}

	private OptimizedPlan compile(final Plan plan) {
		final OptimizedPlan optimizedPlan = new PactCompiler(null, new CostEstimator(), new InetSocketAddress(0))
			.compile(plan, MOCK_INSTANCE_DESCRIPTION);
		return optimizedPlan;
	}

	private void replaceShippingStrategy(final OptimizedPlan optimizedPlan) {
		// final Field declaredField =
		// PactConnection.class.getDeclaredField("shipStrategy");
		// declaredField.setAccessible(true);
		for (final OptimizerNode node : optimizedPlan.getAllNodes()) {
			for (final PactConnection pactConnection : node
				.getIncomingConnections())
				// declaredField.set(pactConnection, ShipStrategy.FORWARD);
				pactConnection.setShipStrategy(ShipStrategy.FORWARD);
			for (final PactConnection pactConnection : node
				.getOutgoingConnections())
				// declaredField.set(pactConnection, ShipStrategy.FORWARD);
				pactConnection.setShipStrategy(ShipStrategy.FORWARD);
		}
	}

	private void initAdhocInputs() throws IOException {
		for (final FileDataSourceContract<?, ?> FileDataSourceContract : this.sources) {
			final TestPairs<?, ?> input = this.getInput(FileDataSourceContract);
			if (input.isAdhoc())
				input.saveToFile(FileDataSourceContract.getFilePath());
		}
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
	 * Returns the first expected output {@link TestPairs} of the TestPlan. If
	 * multiple contracts are tested in the TestPlan, it is recommended to use
	 * the {@link #getExpectedOutput(FileDataSinkContract)} method to unambiguously
	 * set the values.
	 * 
	 * @return the first expected output of the TestPlan
	 */
	public TestPairs<Key, Value> getExpectedOutput() {
		return this.getExpectedOutput(0);
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
	@SuppressWarnings("unchecked")
	public <K extends Key, V extends Value> TestPairs<K, V> getExpectedOutput(
			final FileDataSinkContract<K, V> sink) {
		TestPairs<K, V> values = (TestPairs<K, V>) this.expectedOutputs
			.get(sink);
		if (values == null)
			this.expectedOutputs.put(sink, values = new TestPairs<K, V>());
		return values;
	}

	/**
	 * Returns the expected output {@link TestPairs} associated with the
	 * <i>i</i>th expected output of the TestPlan. If multiple contracts are
	 * tested in the TestPlan, it is recommended to use the {@link #getExpectedOutput(FileDataSinkContract)} method to
	 * unambiguously set
	 * the values.
	 * 
	 * @param number
	 *        the number of the expected output.
	 * @return the <i>i</i>th expected output of the TestPlan
	 */
	@SuppressWarnings("unchecked")
	public TestPairs<Key, Value> getExpectedOutput(final int number) {
		return (TestPairs<Key, Value>) this
			.getExpectedOutput(new ArrayList<FileDataSinkContract<?, ?>>(this
				.getDataSinks()).get(number));
	}

	/**
	 * Returns the first input {@link TestPairs} of the TestPlan. If multiple
	 * contracts are tested in the TestPlan, it is recommended to use the {@link #getInput(FileDataSourceContract)}
	 * method
	 * to unambiguously set the
	 * values.
	 * 
	 * @return the first input of the TestPlan
	 */
	public TestPairs<Key, Value> getInput() {
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
	@SuppressWarnings("unchecked")
	public <K extends Key, V extends Value> TestPairs<K, V> getInput(
			final FileDataSourceContract<K, V> source) {
		TestPairs<K, V> values = (TestPairs<K, V>) this.inputs.get(source);
		if (values == null)
			this.inputs.put(source, values = new TestPairs<K, V>());
		return values;
	}

	/**
	 * Returns the input {@link TestPairs} associated with the <i>i</i>th input
	 * of the TestPlan. If multiple contracts are tested in the TestPlan, it is
	 * recommended to use the {@link #getInput(FileDataSourceContract)} method to
	 * unambiguously set the values.
	 * 
	 * @param number
	 *        the number of the input.
	 * @return the <i>i</i>th input of the TestPlan
	 */
	@SuppressWarnings("unchecked")
	public TestPairs<Key, Value> getInput(final int number) {
		return (TestPairs<Key, Value>) this.getInput(this.getDataSources().get(
			number));
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
			final LocalScheduler localScheduler = new LocalScheduler(this, MockInstanceManager.INSTANCE);
			this.inputSplitManager.registerJob(eg);
			localScheduler.schedulJob(eg);
			this.execute(eg);
		} catch (final Exception e) {
			Assert.fail("plan scheduling: " + StringUtils.stringifyException(e));
		}

		for (ExecutionExceptionHandler errorHandler : errorHandlers)
			// these fields are set by the ExecutionExceptionHandler in case of error
			if (errorHandler.executionError != null)
				Assert.fail(String.format("Error @ %s: %s", errorHandler.executionVertex.getName(),
					errorHandler.executionError));

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
	 * Sets the degreeOfParallelism to the specified value.
	 * 
	 * @param degreeOfParallelism
	 *        the degreeOfParallelism to set
	 */
	public void setDegreeOfParallelism(final int degreeOfParallelism) {
		this.degreeOfParallelism = degreeOfParallelism;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void validateResults() {
		for (final FileDataSinkContract sinkContract : this.getDataSinks())
			// need a format which is deserializable without configuration
			if (sinkContract.getFormatClass() == SequentialOutputFormat.class
				&& this.getExpectedOutput(sinkContract).isInitialized()) {
				final TestPairs<Key, Value> actualValues = new TestPairs<Key, Value>();
				actualValues.fromFile(SequentialInputFormat.class,
					sinkContract.getFilePath());

				final TestPairs<Key, Value> expectedValues = (TestPairs<Key, Value>) this
					.getExpectedOutput(sinkContract);

				FuzzyTestValueMatcher<Value> fuzzyMatcher = this.getFuzzyMatcher(sinkContract);
				FuzzyTestValueSimilarity<Value> fuzzySimilarity = this.getFuzzySimilarity(sinkContract);
				try {
					actualValues.assertEquals(expectedValues, fuzzyMatcher, fuzzySimilarity);
				} catch (AssertionError e) {
					AssertionError assertionError = new AssertionError(sinkContract.getName() + ": "
						+ e.getMessage());
					assertionError.initCause(e.getCause());
					throw assertionError;
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
	public static FileDataSinkContract<Key, Value> createDefaultSink(final String name) {
		return new FileDataSinkContract<Key, Value>(SequentialOutputFormat.class,
			getTestPlanFile("output"), name);
	}

	/**
	 * Creates a default source with the given name. This sink may be used with ad-hoc values added to the corresponding
	 * {@link TestPairs}.
	 * 
	 * @param name
	 *        the name of the source
	 * @return the created source
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static FileDataSourceContract<Key, Value> createDefaultSource(final String name) {
		return new FileDataSourceContract(SequentialInputFormat.class,
			getTestPlanFile("input"), name);
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
			throw new IllegalStateException(
				"Cannot create temporary file for prefix " + prefix, e);
		}
	}

	@Override
	public void close() throws IOException {
		ClosableManager closableManager = new ClosableManager();

		for (TestPairs<?, ?> pairs : this.inputs.values())
			closableManager.add(pairs);
		for (TestPairs<?, ?> pairs : this.actualOutputs.values())
			closableManager.add(pairs);
		for (TestPairs<?, ?> pairs : this.expectedOutputs.values())
			closableManager.add(pairs);

		closableManager.close();
	}

	List<ExecutionExceptionHandler> errorHandlers = new ArrayList<TestPlan.ExecutionExceptionHandler>();

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deploy(final JobID jobID, final AbstractInstance instance,
			final List<ExecutionVertex> verticesToBeDeployed) {
		final Iterator<ExecutionVertex> it = verticesToBeDeployed.iterator();
		while (it.hasNext()) {

			final ExecutionVertex executionVertex = it.next();

			ExecutionExceptionHandler executionListener = new ExecutionExceptionHandler(executionVertex);
			final Environment environment = executionVertex.getEnvironment();
			environment.registerExecutionListener(executionListener);
			environment.setInputSplitProvider(new MockInputSplitProvider(executionVertex));
			errorHandlers.add(executionListener);

			final TaskSubmissionResult submissionResult = executionVertex
				.startTask();

			if (submissionResult.getReturnCode() == AbstractTaskResult.ReturnCode.ERROR)
				Assert.fail(submissionResult.getDescription() + " @ " + executionVertex);
		}
	}

}