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
package eu.stratosphere.sopremo.server;

import java.io.Closeable;
import java.io.IOException;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;
import junit.framework.AssertionFailedError;

import org.apache.hadoop.fs.FileSystem;
import org.junit.Ignore;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheProfileRequest;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheProfileResponse;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheUpdate;
import eu.stratosphere.nephele.rpc.RPCService;
import eu.stratosphere.pact.test.util.Constants;
import eu.stratosphere.pact.test.util.filesystem.FilesystemProvider;
import eu.stratosphere.pact.test.util.minicluster.ClusterProvider;
import eu.stratosphere.pact.test.util.minicluster.ClusterProviderPool;
import eu.stratosphere.sopremo.execution.ExecutionRequest;
import eu.stratosphere.sopremo.execution.ExecutionResponse;
import eu.stratosphere.sopremo.execution.ExecutionResponse.ExecutionState;
import eu.stratosphere.sopremo.execution.SopremoConstants;
import eu.stratosphere.sopremo.execution.SopremoExecutionProtocol;
import eu.stratosphere.sopremo.execution.SopremoID;
import eu.stratosphere.sopremo.io.JsonGenerator;
import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Arvid Heise
 */
@Ignore
public class SopremoTestServer implements Closeable, SopremoExecutionProtocol {
	
	private RPCService rpcService;
	
	private SopremoServer server;
	
	private SopremoExecutionProtocol executor;

	private ClusterProvider cluster;

	private static final int MINIMUM_HEAP_SIZE_MB = 192;

	private final String configName = Constants.DEFAULT_TEST_CONFIG;

	private Set<String> filesToCleanup = new HashSet<String>();

	private String tempDir, protocol;

	/**
	 * Initializes SopremoTestServer.
	 */
	public SopremoTestServer(boolean rpc) {

		verifyJvmOptions();

		this.server = new SopremoServer();
		this.server.setJobManagerAddress(
			new InetSocketAddress("localhost", ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT));
		try {
			this.cluster = ClusterProviderPool.getInstance(this.configName);
		} catch (Exception e) {
			fail(e, "Cannot start mini cluster");
		}

		if (rpc) {
			try {
				this.server.setServerAddress(
					new InetSocketAddress("localhost", SopremoConstants.DEFAULT_SOPREMO_SERVER_IPC_PORT));
				this.server.start();
				this.rpcService = new RPCService();
				this.executor = this.rpcService.getProxy(this.server.getServerAddress(), SopremoExecutionProtocol.class);
			} catch (IOException e) {
				fail(e, "Cannot start rpc sopremo server");
			}
		} else {
			this.executor = this.server;
		}

		this.tempDir = this.cluster.getFilesystemProvider().getTempDirPath() + "/";
		this.protocol = this.cluster.getFilesystemProvider().getURIPrefix();
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.execution.LibraryTransferProtocol#getLibraryCacheProfile(eu.stratosphere.nephele.execution
	 * .librarycache.LibraryCacheProfileRequest)
	 */
	@Override
	public LibraryCacheProfileResponse getLibraryCacheProfile(LibraryCacheProfileRequest request) throws IOException {
		LibraryCacheProfileResponse response = new LibraryCacheProfileResponse(request);
		String[] requiredLibraries = request.getRequiredLibraries();

		// since the test server is executed locally, all libraries are available
		for (int i = 0; i < requiredLibraries.length; i++)
			response.setCached(i, true);

		return response;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.execution.LibraryTransferProtocol#updateLibraryCache(eu.stratosphere.nephele.execution
	 * .librarycache.LibraryCacheUpdate)
	 */
	@Override
	public void updateLibraryCache(LibraryCacheUpdate update) throws IOException {
	}

	public void checkContentsOf(String fileName, IJsonNode... expected) throws IOException {
		List<IJsonNode> remainingValues = new ArrayList<IJsonNode>(Arrays.asList(expected));

		final JsonParser parser = new JsonParser(getFilesystemProvider().getInputStream(this.tempDir + fileName));
		int index = 0;

		for (; index < expected.length && !parser.checkEnd(); index++) {
			final IJsonNode actual = parser.readValueAsTree();
			Assert.assertTrue("Unexpected value " + actual, remainingValues.remove(actual));
		}
		if (!remainingValues.isEmpty())
			Assert.fail("More elements expected " + remainingValues);
	}

	/*
	 * (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		for (String fileToClean : this.filesToCleanup)
			try {
				if (!getFilesystemProvider().delete(fileToClean, false))
					getFilesystemProvider().delete(fileToClean, true);
			} catch (IOException e) {
			}

		try {
			this.cluster.stopCluster();
			ClusterProviderPool.removeInstance(this.configName);
		} catch (Exception e) {
		}
		this.server.close();
		if (this.executor != this.server)
			this.rpcService.shutDown();
		FileSystem.closeAll();
	}

	public void correctPathsOfPlan(SopremoPlan plan) {
		for (Operator<?> op : plan.getContainedOperators())
			if (op instanceof Source) {
				final String fileName = ((Source) op).getInputPath();
				this.filesToCleanup.add(getTempName(fileName));
				((Source) op).setInputPath(this.protocol + getTempName(fileName));
			} else if (op instanceof Sink) {
				final String fileName = ((Sink) op).getOutputPath();
				this.filesToCleanup.add(getTempName(fileName));
				((Sink) op).setOutputPath(this.protocol + getTempName(fileName));
			}
	}

	public boolean createDir(String dirName) throws IOException {
		this.filesToCleanup.add(getTempName(dirName));
		return getFilesystemProvider().createDir(getTempName(dirName));
	}

	public void createFile(String fileName, IJsonNode... nodes) throws IOException {
		this.filesToCleanup.add(getTempName(fileName));
		this.cluster.getFilesystemProvider().createFile(getTempName(fileName), getJsonString(nodes));
	}

	public boolean delete(String path, boolean recursive) throws IOException {
		return getFilesystemProvider().delete(getTempName(path), recursive);
	}

	@Override
	public ExecutionResponse execute(ExecutionRequest request) throws IOException, InterruptedException {
		correctPathsOfPlan(request.getQuery());

		return this.executor.execute(request);
	}

	public FilesystemProvider getFilesystemProvider() {
		return this.cluster.getFilesystemProvider();
	}

	public InetSocketAddress getServerAddress() {
		return this.server.getServerAddress();
	}

	@Override
	public ExecutionResponse getState(SopremoID jobId) throws IOException, InterruptedException {
		return this.executor.getState(jobId);
	}

	private void fail(Exception e, final String message) throws AssertionFailedError {
		final AssertionFailedError assertionFailedError = new AssertionFailedError(message);
		assertionFailedError.initCause(e);
		throw assertionFailedError;
	}

	private String getJsonString(IJsonNode... nodes) throws IOException {
		final StringWriter jsonWriter = new StringWriter();
		JsonGenerator generator = new JsonGenerator(jsonWriter);
		generator.writeStartArray();
		for (IJsonNode node : nodes)
			generator.writeTree(node);
		generator.writeEndArray();
		generator.close();
		final String jsonString = jsonWriter.toString();
		return jsonString;
	}

	private String getTempName(String name) {
		return this.tempDir + name;
	}

	private void verifyJvmOptions() {
		long heap = Runtime.getRuntime().maxMemory() >> 20;
		Assert.assertTrue("Insufficient java heap space " + heap + "mb - set JVM option: -Xmx" + MINIMUM_HEAP_SIZE_MB
			+ "m", heap > MINIMUM_HEAP_SIZE_MB - 50);
		Assert.assertTrue("IPv4 stack required - set JVM option: -Djava.net.preferIPv4Stack=true", "true".equals(System
			.getProperty("java.net.preferIPv4Stack")));
	}

	public static ExecutionResponse waitForStateToFinish(SopremoExecutionProtocol server, ExecutionResponse response,
			ExecutionState status) throws IOException, InterruptedException {
		for (int waits = 0; response.getState() == status && waits < 1000; waits++) {
			Thread.sleep(100);
			response = server.getState(response.getJobId());
		}
		return response;
	}

}
