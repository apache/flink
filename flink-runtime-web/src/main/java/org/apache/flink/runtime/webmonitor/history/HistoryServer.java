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
package org.apache.flink.runtime.webmonitor.history;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.typesafe.config.Config;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.router.Handler;
import io.netty.handler.codec.http.router.Router;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.TaskStateStats;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.runtime.webmonitor.HttpRequestHandler;
import org.apache.flink.runtime.webmonitor.PipelineErrorHandler;
import org.apache.flink.runtime.webmonitor.utils.JsonUtils;
import org.apache.flink.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.UUID;

public class HistoryServer extends UntypedActor {
	/** Logger for web frontend startup / shutdown messages */
	private static final Logger LOG = LoggerFactory.getLogger(HistoryServer.class);
	public static final JsonFactory jacksonFactory = new JsonFactory()
		.enable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
		.disable(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT);

	private final int webPort;
	private final long refreshInterval;
	private Router router;
	private ServerBootstrap bootstrap;
	private Channel serverChannel;

	private final SSLContext serverSSLContext;

	private final File webRootDir;

	public static void main(String[] args) throws Exception {
		ParameterTool pt = ParameterTool.fromArgs(args);
		String configDir = pt.getRequired("configDir");

		LOG.info("Loading configuration from {}", configDir);
		Configuration flinkConfig = GlobalConfiguration.loadConfiguration(configDir);

		boolean enableSSL = flinkConfig.getBoolean(HistoryServerOptions.HISTORY_SERVER_WEB_SSL_ENABLED) &&
			SSLUtils.getSSLEnabled(flinkConfig);

		SSLContext serverSSLContext = null;
		if (enableSSL) {
			LOG.info("Enabling ssl for the history server.");
			try {
				serverSSLContext = SSLUtils.createSSLServerContext(flinkConfig);
			} catch (Exception e) {
				throw new IOException("Failed to initialize SSLContext for the history server.", e);
			}
		}

		String listeningAddress = flinkConfig.getString(HistoryServerOptions.HISTORY_SERVER_RPC_HOST);
		int listeningPort = flinkConfig.getInteger(HistoryServerOptions.HISTORY_SERVER_RPC_PORT);

		if (listeningAddress == null || listeningAddress.isEmpty() || listeningPort == -1) {
			throw new IllegalArgumentException("Invalid HistoryServer RPC host/port configuration. " +
				HistoryServerOptions.HISTORY_SERVER_RPC_HOST.key() + ": " + listeningAddress +
				' ' + HistoryServerOptions.HISTORY_SERVER_RPC_PORT.key() + ": " + listeningPort);
		}

		Config akkaConfig = AkkaUtils.getAkkaConfig(flinkConfig, Option.apply(new Tuple2<String, Object>(listeningAddress, listeningPort)));
		ActorSystem actorSystem = AkkaUtils.createActorSystem(akkaConfig);

		LOG.info("Starting history server actor at {}", NetUtils.hostAndPortToUrlString(listeningAddress, listeningPort));
		actorSystem.actorOf(Props.create(HistoryServer.class, flinkConfig, serverSSLContext), "HistoryServer");
		LOG.info("Started history server actor at {}", NetUtils.hostAndPortToUrlString(listeningAddress, listeningPort));

		actorSystem.awaitTermination();
	}

	// =================================================================================================================
	// Actor life-cycle
	// =================================================================================================================
	public HistoryServer(Configuration config, SSLContext sslContext) {
		this.serverSSLContext = sslContext;

		webPort = config.getInteger(HistoryServerOptions.HISTORY_SERVER_WEB_PORT);
		String directory = config.getString(HistoryServerOptions.HISTORY_SERVER_DIR);
		if (directory == null) {
			directory = System.getProperty("java.io.tmpdir") + "flink-web-history-" + UUID.randomUUID();
		}
		webRootDir = new File(directory);
		refreshInterval = config.getLong(HistoryServerOptions.HISTORY_SERVER_WEB_REFRESH_INTERVAL);
	}

	@Override
	public void preStart() throws IOException, InterruptedException {
		LOG.info("Starting history server.");

		Files.createDirectories(webRootDir.toPath());
		LOG.info("Using directory {} for the history server files", webRootDir);

		router = new Router();
		router.GET("/:*", new HistoryServerStaticFileServerHandler(webRootDir));

		ChannelHandler initializer = new ChannelInitializer<SocketChannel>() {

			@Override
			protected void initChannel(SocketChannel ch) {
				Handler handler = new Handler(router);

				// SSL should be the first handler in the pipeline
				if (serverSSLContext != null) {
					SSLEngine sslEngine = serverSSLContext.createSSLEngine();
					sslEngine.setUseClientMode(false);
					ch.pipeline().addLast("ssl", new SslHandler(sslEngine));
				}

				ch.pipeline()
					.addLast(new HttpServerCodec())
					.addLast(new ChunkedWriteHandler())
					.addLast(new HttpRequestHandler(webRootDir))
					.addLast(handler.name(), handler)
					.addLast(new PipelineErrorHandler(LOG));
			}
		};

		EventLoopGroup bossGroup = new NioEventLoopGroup(1);
		EventLoopGroup workerGroup = new NioEventLoopGroup();

		this.bootstrap = new ServerBootstrap();
		this.bootstrap
			.group(bossGroup, workerGroup)
			.channel(NioServerSocketChannel.class)
			.childHandler(initializer);

		Channel ch = this.bootstrap.bind(webPort).sync().channel();
		this.serverChannel = ch;

		InetSocketAddress bindAddress = (InetSocketAddress) ch.localAddress();
		String address = bindAddress.getAddress().getHostAddress();
		int port = bindAddress.getPort();

		createConfigFile();
		updateOverview();

		LOG.info("Started history server on {}:{}.", address, port);
	}

	@Override
	public void onReceive(Object message) {
		AccessExecutionGraph graph = (AccessExecutionGraph) message;
		try {
			prepareJobFiles(webRootDir, graph);
			updateJobList(graph);
			updateOverview();
		} catch (Exception e) {
			LOG.error("Failed to process ExecutionGraph.", e);
		}
	}

	@Override
	public void postStop() {
		LOG.info("Stopping history server.");
		if (this.serverChannel != null) {
			this.serverChannel.close().awaitUninterruptibly();
		}
		if (bootstrap != null) {
			if (bootstrap.group() != null) {
				bootstrap.group().shutdownGracefully();
			}
		}

		try {
			LOG.info("Removing history server root cache directory {}", webRootDir);
			FileUtils.deleteDirectory(webRootDir);
			LOG.info("Removed history server root cached directory {}", webRootDir);
		} catch (Throwable t) {
			LOG.warn("Error while deleting history server root directory {}", webRootDir, t);
		}
		LOG.info("Stopped history server.");
	}

	// =================================================================================================================
	// File generation
	// =================================================================================================================
	private static File createFolder(File baseDir, String... folders) throws IOException {
		StringBuilder sb = new StringBuilder();
		if (folders.length > 0) {
			sb.append(folders[0]);
			for (int x = 1; x < folders.length; x++) {
				sb.append(File.separator).append(folders[x]);
			}
		}
		File folder = new File(baseDir, sb.toString());
		if (!folder.exists()) {
			Files.createDirectories(folder.toPath());
		}
		return folder;
	}

	private static JsonGenerator createFile(File folder, String name) throws IOException {
		File file = new File(folder, name + ".json");
		if (!file.exists()) {
			Files.createFile(file.toPath());
		}
		FileWriter fr = new FileWriter(file);
		return jacksonFactory.createGenerator(fr);

	}

	// -----------------------------------------------------------------------------------------------------------------
	// File generation - Config
	// -----------------------------------------------------------------------------------------------------------------
	private void createConfigFile() throws IOException {
		try (JsonGenerator gen = createFile(webRootDir, "config")) {
			JsonUtils.writeConfigAsJson(gen, refreshInterval);
		} catch (IOException ioe) {
			LOG.error("Failed to write config file.");
			throw ioe;
		}
	}

	// -----------------------------------------------------------------------------------------------------------------
	// File generation - Jobs
	// -----------------------------------------------------------------------------------------------------------------
	private void updateJobList(AccessExecutionGraph graph) throws IOException {
		boolean first = false;
		File f = new File(webRootDir, "temp");
		if (!f.exists()) {
			Files.createFile(f.toPath());
			first = true;
		}
		FileWriter fw = new FileWriter(f, true);
		try (JsonGenerator gen = jacksonFactory.createGenerator(fw)) {
			if (!first) {
				gen.writeRaw(",");
			}
			JsonUtils.writeJobDetailOverviewAsJson(graph, gen);
		}
	}

	private void updateOverview() throws IOException {
		try (JsonGenerator gen = createFile(webRootDir, "joboverview")) {
			gen.writeStartObject();
			gen.writeArrayFieldStart("running");
			gen.writeEndArray();
			gen.writeArrayFieldStart("finished");

			File source = new File(webRootDir, "temp");
			if (source.exists()) {
				try (BufferedReader fr = new BufferedReader(new FileReader(source))) {
					String line;
					while ((line = fr.readLine()) != null) {
						gen.writeRaw(line);
					}
				}
			}
			gen.writeEndArray();
			gen.writeEndObject();
		} catch (IOException ioe) {
			LOG.error("Failed to update job overview.");
			throw ioe;
		}
	}

	private static void prepareJobFiles(File rootDir, AccessExecutionGraph graph) throws IOException {
		File jobsDirectory = createFolder(rootDir, "jobs");
		File jobDirectory = createFolder(jobsDirectory, graph.getJobID().toString());
		try {
			writeJobDetails(jobsDirectory, jobDirectory, graph);
		} catch (Exception e) {
			LOG.error("Error", e);
		}

		File checkpointsDirectory = createFolder(jobDirectory, "checkpoints");
		try {
			writeCheckpoints(jobDirectory, graph);
			writeCheckpointConfig(checkpointsDirectory, graph);			
		} catch (Exception e) {
			LOG.error("Error", e);
		}

		File checkpointsDetailsDirectory = createFolder(checkpointsDirectory, "details");
		if (graph.getCheckpointStatsSnapshot() != null) {
			for (AbstractCheckpointStats stats : graph.getCheckpointStatsSnapshot().getHistory().getCheckpoints()) {
				try {
					writeCheckpointDetails(checkpointsDetailsDirectory, stats);
				} catch (Exception e) {
					LOG.error("Error", e);
				}

				File subtasksDirectory = createFolder(checkpointsDetailsDirectory, String.valueOf(stats.getCheckpointId()), "subtasks");
				for (TaskStateStats subtaskStats : stats.getAllTaskStateStats()) {
					try {
						writeSubtaskCheckpointStats(subtasksDirectory, stats, subtaskStats);
					} catch (Exception e) {
						LOG.error("Error", e);
					}
				}
			}
		}

		File tasksDirectory = createFolder(jobDirectory, "vertices");
		for (AccessExecutionJobVertex task : graph.getAllVertices().values()) {
			File taskDirectory = createFolder(tasksDirectory, task.getJobVertexId().toString());
			try {
				writeTaskDetails(tasksDirectory, taskDirectory, graph, task);
			} catch (Exception e) {
				LOG.error("Error", e);
			}

			File subtasksDirectory = createFolder(taskDirectory, "subtasks");
			writeTaskAccumulatorsBySubtask(subtasksDirectory, task);
			for (AccessExecutionVertex subtask : task.getTaskVertices()) {
				File subtaskDirectory = createFolder(subtasksDirectory, String.valueOf(subtask.getParallelSubtaskIndex()));
				try {
					writeSubtaskDetails(subtaskDirectory, subtask);
				} catch (Exception e) {
					LOG.error("Error", e);
				}

				AccessExecution attempt = subtask.getCurrentExecutionAttempt();
				File attemptsDirectory = createFolder(subtaskDirectory, "attempts");
				File attemptDirectory = createFolder(attemptsDirectory, String.valueOf(attempt.getAttemptNumber()));
				try {
					writeAttemptDetails(attemptsDirectory, graph, task, attempt);
					writeAttemptAccumulators(attemptDirectory, attempt);
				} catch (Exception e) {
					LOG.error("Error", e);
				}
				for (AccessExecution priorAttempt : subtask.getPriorExecutionAttempts()) {
					attemptDirectory = createFolder(attemptsDirectory, String.valueOf(priorAttempt.getAttemptNumber()));
					try {
						writeAttemptDetails(attemptDirectory, graph, task, priorAttempt);
						writeAttemptAccumulators(attemptDirectory, priorAttempt);
					} catch (Exception e) {
						LOG.error("Error", e);
					}
				}
			}
		}
	}

	private static void writeJobDetails(File jobsDirectory, File jobDirectory, AccessExecutionGraph graph) throws IOException {
		writeJobPlan(jobDirectory, graph);
		writeJobConfig(jobDirectory, graph);
		writeJobExceptions(jobDirectory, graph);
		writeJobAccumulators(jobDirectory, graph);
		writeJobMetrics(jobDirectory, graph);
		writeJobDetails(jobsDirectory, graph);
	}

	private static void writeTaskDetails(File tasksDirectory, File taskDirectory, AccessExecutionGraph graph, AccessExecutionJobVertex task) throws IOException {
		writeTaskDetails(tasksDirectory, task);
		writeTaskSubtaskTimes(taskDirectory, task);
		writeTaskByTaskManager(taskDirectory, task);
		writeTaskAccumulators(taskDirectory, task);
		writeTaskBackpressure(taskDirectory, task);
		writeTaskMetrics(taskDirectory, task);
	}

	private static void writeJobPlan(File folder, AccessExecutionGraph graph) throws IOException {
		try (JsonGenerator gen = createFile(folder, "plan")) {
			gen.writeRaw(graph.getJsonPlan());
		}
	}

	private static void writeJobConfig(File folder, AccessExecutionGraph graph) throws IOException {
		try (JsonGenerator gen = createFile(folder, "config")) {
			JsonUtils.writeJobConfigAsJson(graph, gen);
		}
	}

	private static void writeJobExceptions(File folder, AccessExecutionGraph graph) throws IOException {
		try (JsonGenerator gen = createFile(folder, "exceptions")) {
			JsonUtils.writeJobExceptionsAsJson(graph, gen);
		}
	}

	private static void writeJobAccumulators(File folder, AccessExecutionGraph graph) throws IOException {
		try (JsonGenerator gen = createFile(folder, "accumulators")) {
			JsonUtils.writeJobAccumulatorsAsJson(graph, gen);
		}
	}

	private static void writeJobMetrics(File folder, AccessExecutionGraph graph) throws IOException {
		try (JsonGenerator gen = createFile(folder, "metrics")) {
		}
	}

	private static void writeJobDetails(File folder, AccessExecutionGraph graph) throws IOException {
		try (JsonGenerator gen = createFile(folder, graph.getJobID().toString())) {
			JsonUtils.writeJobDetailsAsJson(graph, null, gen);
		}
	}

	private static void writeCheckpoints(File folder, AccessExecutionGraph graph) throws IOException {
		try (JsonGenerator gen = createFile(folder, "checkpoints")) {
			JsonUtils.writeCheckpointStatsAsJson(graph, gen);
		}
	}

	private static void writeCheckpointConfig(File folder, AccessExecutionGraph graph) throws IOException {
		try (JsonGenerator gen = createFile(folder, "config")) {
			JsonUtils.writeCheckpointConfigAsJson(graph, gen);
		}
	}

	private static void writeCheckpointDetails(File folder, AbstractCheckpointStats stats) throws IOException {
		try (JsonGenerator gen = createFile(folder, String.valueOf(stats.getCheckpointId()))) {
			JsonUtils.writeCheckpointDetailsAsJson(stats, gen);
		}
	}

	// -----------------------------------------------------------------------------------------------------------------
	// File generation - Task
	// -----------------------------------------------------------------------------------------------------------------
	private static void writeTaskDetails(File folder, AccessExecutionJobVertex task) throws IOException {
		try (JsonGenerator gen = createFile(folder, task.getJobVertexId().toString())) {
			JsonUtils.writeVertexDetailsAsJson(task, null, null, gen);
		}
	}

	private static void writeTaskSubtaskTimes(File folder, AccessExecutionJobVertex task) throws IOException {
		try (JsonGenerator gen = createFile(folder, "subtasktimes")) {
			JsonUtils.writeSubtaskDetailsAsJson(task, gen);
		}
	}

	private static void writeTaskByTaskManager(File folder, AccessExecutionJobVertex task) throws IOException {
		try (JsonGenerator gen = createFile(folder, "taskmanagers")) {
			JsonUtils.writeVertexDetailsByTaskManagerAsJson(task, null, null, gen);
		}
	}

	private static void writeTaskAccumulators(File folder, AccessExecutionJobVertex task) throws IOException {
		try (JsonGenerator gen = createFile(folder, "accumulators")) {
			JsonUtils.writeVertexAccumulatorsAsJson(task, gen);
		}
	}

	private static void writeTaskBackpressure(File folder, AccessExecutionJobVertex task) throws IOException {
		try (JsonGenerator gen = createFile(folder, "backpressure")) {
			gen.writeRaw("{}");
		}
	}

	private static void writeTaskMetrics(File folder, AccessExecutionJobVertex task) throws IOException {
		try (JsonGenerator gen = createFile(folder, "metrics")) {
			gen.writeRaw("{}");
		}
	}

	// -----------------------------------------------------------------------------------------------------------------
	// File generation - Subtask
	// -----------------------------------------------------------------------------------------------------------------
	private static void writeSubtaskDetails(File folder, AccessExecutionVertex subtask) throws IOException {
		try (JsonGenerator gen = createFile(folder, String.valueOf(subtask.getParallelSubtaskIndex()))) {
			JsonUtils.writeAttemptDetails(subtask.getCurrentExecutionAttempt(), null, null, null, gen);
		}
	}

	private static void writeTaskAccumulatorsBySubtask(File folder, AccessExecutionJobVertex task) throws IOException {
		try (JsonGenerator gen = createFile(folder, "accumulators")) {
			JsonUtils.writeSubtasksAccumulatorsAsJson(task, gen);
		}
	}

	private static void writeSubtaskCheckpointStats(File folder, AbstractCheckpointStats checkpoint, TaskStateStats stats) throws IOException {
		try (JsonGenerator gen = createFile(folder, stats.getJobVertexId().toString())) {
			JsonUtils.writeSubtaskCheckpointDetailsAsJson(checkpoint, stats, gen);
		}
	}

	// -----------------------------------------------------------------------------------------------------------------
	// File generation - Execution Attempt
	// -----------------------------------------------------------------------------------------------------------------
	private static void writeAttemptDetails(File folder, AccessExecutionGraph graph, AccessExecutionJobVertex task, AccessExecution attempt) throws IOException {
		try (JsonGenerator gen = createFile(folder, String.valueOf(attempt.getAttemptNumber()))) {
			JsonUtils.writeAttemptDetails(attempt, graph.getJobID().toString() , task.getJobVertexId().toString(), null, gen);
		}
	}

	private static void writeAttemptAccumulators(File folder, AccessExecution attempt) throws IOException {
		try (JsonGenerator gen = createFile(folder, "accumulators")) {
			JsonUtils.writeAttemptAccumulators(attempt, gen);
		}
	}
}
