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

package org.apache.flink.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.util.Preconditions;

import org.apache.beam.model.pipeline.v1.RunnerApi;
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

import java.io.File;
import java.util.List;

/**
 * An base class for {@link PythonFunctionRunner}.
 *
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the execution results.
 */
@Internal
public abstract class AbstractPythonFunctionRunner<IN, OUT> implements PythonFunctionRunner<IN> {

	/** The logger used by the runner class and its subclasses. */
	protected static final Logger LOG = LoggerFactory.getLogger(AbstractPythonFunctionRunner.class);

	private static final String MAIN_INPUT_ID = "input";

	private final String taskName;

	/**
	 * The Python function execution result receiver.
	 */
	private final FnDataReceiver<OUT> resultReceiver;

	/**
	 * The Python execution environment manager.
	 */
	private final PythonEnvironmentManager environmentManager;

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
	 * The receiver which forwards the input elements to a remote environment for processing.
	 */
	private transient FnDataReceiver<WindowedValue<?>> mainInputReceiver;

	/**
	 * The TypeSerializer for input elements.
	 */
	private transient TypeSerializer<IN> inputTypeSerializer;

	/**
	 * The TypeSerializer for execution results.
	 */
	private transient TypeSerializer<OUT> outputTypeSerializer;

	/**
	 * Reusable InputStream used to holding the execution results to be deserialized.
	 */
	private transient ByteArrayInputStreamWithPos bais;

	/**
	 * InputStream Wrapper.
	 */
	private transient DataInputViewStreamWrapper baisWrapper;

	/**
	 * Reusable OutputStream used to holding the serialized input elements.
	 */
	private transient ByteArrayOutputStreamWithPos baos;

	/**
	 * OutputStream Wrapper.
	 */
	private transient DataOutputViewStreamWrapper baosWrapper;

	/**
	 * Python libraries and shell script extracted from resource of flink-python jar.
	 * They are used to support running python udf worker in process mode.
	 */
	private transient List<File> pythonInternalLibs;

	public AbstractPythonFunctionRunner(
		String taskName,
		FnDataReceiver<OUT> resultReceiver,
		PythonEnvironmentManager environmentManager,
		StateRequestHandler stateRequestHandler) {
		this.taskName = Preconditions.checkNotNull(taskName);
		this.resultReceiver = Preconditions.checkNotNull(resultReceiver);
		this.environmentManager = Preconditions.checkNotNull(environmentManager);
		this.stateRequestHandler = Preconditions.checkNotNull(stateRequestHandler);
	}

	@Override
	public void open() throws Exception {
		bais = new ByteArrayInputStreamWithPos();
		baisWrapper = new DataInputViewStreamWrapper(bais);
		baos = new ByteArrayOutputStreamWithPos();
		baosWrapper = new DataOutputViewStreamWrapper(baos);
		inputTypeSerializer = getInputTypeSerializer();
		outputTypeSerializer = getOutputTypeSerializer();

		// The creation of stageBundleFactory depends on the initialized environment manager.
		environmentManager.open();

		PortablePipelineOptions portableOptions =
			PipelineOptionsFactory.as(PortablePipelineOptions.class);
		// one operator has one Python SDK harness
		portableOptions.setSdkWorkerParallelism(1);
		Struct pipelineOptions = PipelineOptionsTranslation.toProto(portableOptions);

		jobBundleFactory = createJobBundleFactory(pipelineOptions);
		stageBundleFactory = jobBundleFactory.forStage(createExecutableStage());
		progressHandler = BundleProgressHandler.ignored();
	}

	@Override
	public void close() throws Exception {

		try {
			if (pythonInternalLibs != null) {
				pythonInternalLibs.forEach(File::delete);
			}
		} finally {
			pythonInternalLibs = null;
		}

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
	public void startBundle() {
		OutputReceiverFactory receiverFactory =
			new OutputReceiverFactory() {

				// the input value type is always byte array
				@SuppressWarnings("unchecked")
				@Override
				public FnDataReceiver<WindowedValue<byte[]>> create(String pCollectionId) {
					return input -> {
						bais.setBuffer(input.getValue(), 0, input.getValue().length);
						resultReceiver.accept(outputTypeSerializer.deserialize(baisWrapper));
					};
				}
			};

		try {
			remoteBundle = stageBundleFactory.getBundle(receiverFactory, stateRequestHandler, progressHandler);
			mainInputReceiver =
				Preconditions.checkNotNull(
					remoteBundle.getInputReceivers().get(MAIN_INPUT_ID),
					"Failed to retrieve main input receiver.");
		} catch (Throwable t) {
			throw new RuntimeException("Failed to start remote bundle", t);
		}
	}

	@Override
	public void finishBundle() {
		try {
			remoteBundle.close();
		} catch (Throwable t) {
			throw new RuntimeException("Failed to close remote bundle", t);
		} finally {
			remoteBundle = null;
		}
	}

	@Override
	public void processElement(IN element) {
		try {
			baos.reset();
			inputTypeSerializer.serialize(element, baosWrapper);
			// TODO: support to use ValueOnlyWindowedValueCoder for better performance.
			// Currently, FullWindowedValueCoder has to be used in Beam's portability framework.
			mainInputReceiver.accept(WindowedValue.valueInGlobalWindow(baos.toByteArray()));
		} catch (Throwable t) {
			throw new RuntimeException("Failed to process element when sending data to Python SDK harness.", t);
		}
	}

	@VisibleForTesting
	public JobBundleFactory createJobBundleFactory(Struct pipelineOptions) throws Exception {
		return DefaultJobBundleFactory.create(
			JobInfo.create(taskName, taskName, environmentManager.createRetrievalToken(), pipelineOptions));
	}

	/**
	 * Creates a specification which specifies the portability Python execution environment.
	 * It's used by Beam's portability framework to creates the actual Python execution environment.
	 */
	protected RunnerApi.Environment createPythonExecutionEnvironment() throws Exception {
		return environmentManager.createEnvironment();
	}

	/**
	 * Creates a {@link ExecutableStage} which contains the Python user-defined functions to be executed
	 * and all the other information needed to execute them, such as the execution environment, the input
	 * and output coder, etc.
	 */
	public abstract ExecutableStage createExecutableStage() throws Exception;

	/**
	 * Returns the TypeSerializer for input elements.
	 */
	public abstract TypeSerializer<IN> getInputTypeSerializer();

	/**
	 * Returns the TypeSerializer for execution results.
	 */
	public abstract TypeSerializer<OUT> getOutputTypeSerializer();

}
