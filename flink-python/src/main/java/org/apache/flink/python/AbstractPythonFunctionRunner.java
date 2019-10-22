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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.Struct;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * An base class for {@link PythonFunctionRunner}.
 *
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the execution results.
 */
@Internal
public abstract class AbstractPythonFunctionRunner<IN, OUT> implements PythonFunctionRunner<IN> {

	private static final String MAIN_INPUT_ID = "input";

	private final String taskName;

	/**
	 * The Python function execution result receiver.
	 */
	private final FnDataReceiver<OUT> resultReceiver;

	/**
	 * The Python execution environment.
	 */
	private final PythonEnv pythonEnv;

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
	 * Temporary directories to store the retrieval token.
	 */
	private final String[] tempDirs;

	/**
	 * The file of the retrieval token representing the entirety of the staged artifacts.
	 * Used during requesting the manifest of a job in Beam portability framework.
	 */
	private transient File retrievalToken;

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
	 * The coder for input elements.
	 */
	private transient Coder<IN> inputCoder;

	/**
	 * The coder for execution results.
	 */
	private transient Coder<OUT> outputCoder;

	/**
	 * Reusable InputStream used to holding the execution results to be deserialized.
	 */
	private transient ByteArrayInputStreamWithPos bais;

	/**
	 * Reusable OutputStream used to holding the serialized input elements.
	 */
	private transient ByteArrayOutputStreamWithPos baos;

	public AbstractPythonFunctionRunner(
		String taskName,
		FnDataReceiver<OUT> resultReceiver,
		PythonEnv pythonEnv,
		StateRequestHandler stateRequestHandler,
		String[] tempDirs) {
		this.taskName = Preconditions.checkNotNull(taskName);
		this.resultReceiver = Preconditions.checkNotNull(resultReceiver);
		this.pythonEnv = Preconditions.checkNotNull(pythonEnv);
		this.stateRequestHandler = Preconditions.checkNotNull(stateRequestHandler);
		this.tempDirs = Preconditions.checkNotNull(tempDirs);
	}

	@Override
	public void open() throws Exception {
		bais = new ByteArrayInputStreamWithPos();
		baos = new ByteArrayOutputStreamWithPos();
		inputCoder = getInputCoder();
		outputCoder = getOutputCoder();

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
			if (retrievalToken != null) {
				retrievalToken.delete();
			}
		} finally {
			retrievalToken = null;
		}

		try {
			if (jobBundleFactory != null) {
				jobBundleFactory.close();
			}
		} finally {
			jobBundleFactory = null;
		}
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
						resultReceiver.accept(outputCoder.decode(bais));
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
			inputCoder.encode(element, baos);
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
			JobInfo.create(taskName, taskName, createEmptyRetrievalToken(), pipelineOptions));
	}

	private String createEmptyRetrievalToken() throws Exception {
		// try to find a unique file name for the retrieval token
		final Random rnd = new Random();
		for (int attempt = 0; attempt < 10; attempt++) {
			String directory = tempDirs[rnd.nextInt(tempDirs.length)];
			retrievalToken = new File(directory, randomString(rnd) + ".json");
			if (retrievalToken.createNewFile()) {
				final DataOutputStream dos = new DataOutputStream(new FileOutputStream(retrievalToken));
				dos.writeBytes("{\"manifest\": {}}");
				dos.flush();
				dos.close();
				return retrievalToken.getAbsolutePath();
			}
		}

		throw new IOException(
			"Could not find a unique file name in '" + Arrays.toString(tempDirs) + "' for retrieval token.");
	}

	private static String randomString(Random random) {
		final byte[] bytes = new byte[20];
		random.nextBytes(bytes);
		return StringUtils.byteToHexString(bytes);
	}

	/**
	 * Creates a specification which specifies the portability Python execution environment.
	 * It's used by Beam's portability framework to creates the actual Python execution environment.
	 */
	protected RunnerApi.Environment createPythonExecutionEnvironment() {
		if (pythonEnv.getExecType() == PythonEnv.ExecType.PROCESS) {
			String flinkHomePath = System.getenv(ConfigConstants.ENV_FLINK_HOME_DIR);
			String pythonWorkerCommand =
				flinkHomePath + File.separator + "bin" + File.separator + "pyflink-udf-runner.sh";

			return Environments.createProcessEnvironment(
				"",
				"",
				pythonWorkerCommand,
				null);
		} else {
			throw new UnsupportedOperationException(String.format(
				"Execution type '%s' is not supported.", pythonEnv.getExecType()));
		}
	}

	/**
	 * Creates a {@link ExecutableStage} which contains the Python user-defined functions to be executed
	 * and all the other information needed to execute them, such as the execution environment, the input
	 * and output coder, etc.
	 */
	public abstract ExecutableStage createExecutableStage();

	/**
	 * Returns the coder for input elements.
	 */
	public abstract Coder<IN> getInputCoder();

	/**
	 * Returns the coder for execution results.
	 */
	public abstract Coder<OUT> getOutputCoder();
}
