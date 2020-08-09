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

package org.apache.flink.streaming.api.runners.python.beam;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.python.PythonConfig;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.ProcessPythonEnvironment;
import org.apache.flink.python.env.PythonEnvironment;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.util.Preconditions;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.DefaultJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * An base class for {@link PythonFunctionRunner} based on beam.
 */
public abstract class BeamPythonFunctionRunner implements PythonFunctionRunner {
	protected static final Logger LOG = LoggerFactory.getLogger(BeamPythonFunctionRunner.class);

	private transient boolean bundleStarted;

	private static final String MAIN_INPUT_ID = "input";

	private final String taskName;

	/**
	 * The Python execution environment manager.
	 */
	private final PythonEnvironmentManager environmentManager;

	/**
	 * The options used to configure the Python worker process.
	 */
	private final Map<String, String> jobOptions;

	/**
	 * The Python function execution result tuple.
	 */
	protected final Tuple2<byte[], Integer> resultTuple;

	/**
	 * The bundle factory which has all job-scoped information and can be used to create a {@link StageBundleFactory}.
	 */
	private transient JobBundleFactory jobBundleFactory;

	/**
	 * The bundle factory which has all of the resources it needs to provide new {@link RemoteBundle}.
	 */
	private transient StageBundleFactory stageBundleFactory;

	/**
	 * Handler for state requests.
	 */
	private final StateRequestHandler stateRequestHandler;

	/**
	 * Handler for bundle progress messages, both during bundle execution and on its completion.
	 */
	private transient BundleProgressHandler progressHandler;

	/**
	 * A bundle handler for handling input elements by forwarding them to a remote environment for processing.
	 * It holds a collection of {@link FnDataReceiver}s which actually perform the data forwarding work.
	 *
	 * <p>When a RemoteBundle is closed, it will block until bundle processing is finished on remote resources,
	 * and throw an exception if bundle processing has failed.
	 */
	private transient RemoteBundle remoteBundle;

	/**
	 * The Python function execution result receiver.
	 */
	protected transient LinkedBlockingQueue<byte[]> resultBuffer;

	/**
	 * The receiver which forwards the input elements to a remote environment for processing.
	 */
	protected transient FnDataReceiver<WindowedValue<byte[]>> mainInputReceiver;

	/**
	 * The flinkMetricContainer will be set to null if metric is configured to be turned off.
	 */
	@Nullable
	private FlinkMetricContainer flinkMetricContainer;

	public BeamPythonFunctionRunner(
		String taskName,
		PythonEnvironmentManager environmentManager,
		StateRequestHandler stateRequestHandler,
		Map<String, String> jobOptions,
		@Nullable FlinkMetricContainer flinkMetricContainer) {
		this.taskName = Preconditions.checkNotNull(taskName);
		this.environmentManager = Preconditions.checkNotNull(environmentManager);
		this.stateRequestHandler = Preconditions.checkNotNull(stateRequestHandler);
		this.jobOptions = Preconditions.checkNotNull(jobOptions);
		this.flinkMetricContainer = flinkMetricContainer;
		this.resultTuple = new Tuple2<>();
	}

	@Override
	public void open(PythonConfig config) throws Exception {
		this.bundleStarted = false;
		this.resultBuffer = new LinkedBlockingQueue<>();

		// The creation of stageBundleFactory depends on the initialized environment manager.
		environmentManager.open();

		PortablePipelineOptions portableOptions =
			PipelineOptionsFactory.as(PortablePipelineOptions.class);
		// one operator has one Python SDK harness
		portableOptions.setSdkWorkerParallelism(1);

		Struct pipelineOptions = PipelineOptionsTranslation.toProto(portableOptions);

		jobBundleFactory = createJobBundleFactory(pipelineOptions);
		stageBundleFactory = createStageBundleFactory();
		progressHandler = getProgressHandler(flinkMetricContainer);
	}

	@Override
	public void close() throws Exception {
		try {
			if (jobBundleFactory != null) {
				jobBundleFactory.close();
			}
		} finally {
			jobBundleFactory = null;
		}

		environmentManager.close();
	}

	@Override
	public void process(byte[] data) throws Exception {
		checkInvokeStartBundle();
		mainInputReceiver.accept(WindowedValue.valueInGlobalWindow(data));
	}

	@Override
	public Tuple2<byte[], Integer> pollResult() throws Exception {
		byte[] result = resultBuffer.poll();
		if (result == null) {
			return null;
		} else {
			this.resultTuple.f0 = result;
			this.resultTuple.f1 = result.length;
			return this.resultTuple;
		}
	}

	@Override
	public void flush() throws Exception {
		if (bundleStarted) {
			finishBundle();
			bundleStarted = false;
		}
	}

	public JobBundleFactory createJobBundleFactory(Struct pipelineOptions) throws Exception {
		return DefaultJobBundleFactory.create(
			JobInfo.create(taskName, taskName, environmentManager.createRetrievalToken(), pipelineOptions));
	}

	/**
	 * Creates a specification which specifies the portability Python execution environment.
	 * It's used by Beam's portability framework to creates the actual Python execution environment.
	 */
	protected RunnerApi.Environment createPythonExecutionEnvironment() throws Exception {
		PythonEnvironment environment = environmentManager.createEnvironment();
		if (environment instanceof ProcessPythonEnvironment) {
			ProcessPythonEnvironment processEnvironment = (ProcessPythonEnvironment) environment;
			Map<String, String> env = processEnvironment.getEnv();
			env.putAll(jobOptions);
			return Environments.createProcessEnvironment(
				"",
				"",
				processEnvironment.getCommand(),
				env);
		}
		throw new RuntimeException("Currently only ProcessPythonEnvironment is supported.");
	}

	protected void startBundle() {
		try {
			remoteBundle = stageBundleFactory.getBundle(createOutputReceiverFactory(), stateRequestHandler, progressHandler);
			mainInputReceiver =
				Preconditions.checkNotNull(
					remoteBundle.getInputReceivers().get(MAIN_INPUT_ID),
					"Failed to retrieve main input receiver.");
		} catch (Throwable t) {
			throw new RuntimeException("Failed to start remote bundle", t);
		}
	}

	/**
	 * Creates a {@link ExecutableStage} which contains the Python user-defined functions to be executed
	 * and all the other information needed to execute them, such as the execution environment, the input
	 * and output coder, etc.
	 */
	public abstract ExecutableStage createExecutableStage() throws Exception;

	private void finishBundle() {
		try {
			remoteBundle.close();
		} catch (Throwable t) {
			throw new RuntimeException("Failed to close remote bundle", t);
		} finally {
			remoteBundle = null;
		}
	}

	private OutputReceiverFactory createOutputReceiverFactory() {
		return new OutputReceiverFactory() {

			// the input value type is always byte array
			@SuppressWarnings("unchecked")
			@Override
			public FnDataReceiver<WindowedValue<byte[]>> create(String pCollectionId) {
				return input -> resultBuffer.add(input.getValue());
			}
		};
	}

	/**
	 * Ignore bundle progress if flinkMetricContainer is null. The flinkMetricContainer will be set
	 * to null if metric is configured to be turned off.
	 */
	private BundleProgressHandler getProgressHandler(FlinkMetricContainer flinkMetricContainer) {
		if (flinkMetricContainer == null) {
			return BundleProgressHandler.ignored();
		} else {
			return new BundleProgressHandler() {
				@Override
				public void onProgress(BeamFnApi.ProcessBundleProgressResponse progress) {
					flinkMetricContainer.updateMetrics(taskName, progress.getMonitoringInfosList());
				}

				@Override
				public void onCompleted(BeamFnApi.ProcessBundleResponse response) {
					flinkMetricContainer.updateMetrics(taskName, response.getMonitoringInfosList());
				}
			};
		}
	}

	/**
	 * To make the error messages more user friendly, throws an exception with the boot logs.
	 */
	private StageBundleFactory createStageBundleFactory() throws Exception {
		try {
			return jobBundleFactory.forStage(createExecutableStage());
		} catch (Throwable e) {
			throw new RuntimeException(environmentManager.getBootLog(), e);
		}
	}

	/**
	 * Checks whether to invoke startBundle.
	 */
	private void checkInvokeStartBundle() throws Exception {
		if (!bundleStarted) {
			startBundle();
			bundleStarted = true;
		}
	}

}
