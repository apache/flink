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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.messages.JobManagerMessages.CancelJobWithSavepoint;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Request handler for {@link CancelJobWithSavepoint} messages.
 */
public class JobCancellationWithSavepointHandlers {

	private static final String CANCEL_WITH_SAVEPOINT_REST_PATH = "/jobs/:jobid/cancel-with-savepoint";
	private static final String CANCEL_WITH_SAVEPOINT_DIRECTORY_REST_PATH = "/jobs/:jobid/cancel-with-savepoint/target-directory/:targetDirectory";

	/** URL for in-progress cancellations. */
	private static final String CANCELLATION_IN_PROGRESS_REST_PATH = "/jobs/:jobid/cancel-with-savepoint/in-progress/:requestId";

	/** Encodings for String. */
	private static final Charset ENCODING = ConfigConstants.DEFAULT_CHARSET;

	/** Shared lock between Trigger and In-Progress handlers. */
	private final Object lock = new Object();

	/** In-Progress requests. */
	private final Map<JobID, Long> inProgress = new HashMap<>();

	/** Succeeded/failed request. Either String or Throwable. */
	private final Map<Long, Object> completed = new HashMap<>();

	/** Atomic request counter. */
	private long requestCounter;

	/** Handler for trigger requests. */
	private final TriggerHandler triggerHandler;

	/** Handler for in-progress requests. */
	private final InProgressHandler inProgressHandler;

	/** Default savepoint directory. */
	private final String defaultSavepointDirectory;

	public JobCancellationWithSavepointHandlers(
			ExecutionGraphCache currentGraphs,
			Executor executor) {
		this(currentGraphs, executor, null);
	}

	public JobCancellationWithSavepointHandlers(
			ExecutionGraphCache currentGraphs,
			Executor executor,
			@Nullable String defaultSavepointDirectory) {

		this.triggerHandler = new TriggerHandler(currentGraphs, executor);
		this.inProgressHandler = new InProgressHandler();
		this.defaultSavepointDirectory = defaultSavepointDirectory;
	}

	public TriggerHandler getTriggerHandler() {
		return triggerHandler;
	}

	public InProgressHandler getInProgressHandler() {
		return inProgressHandler;
	}

	// ------------------------------------------------------------------------
	// New requests
	// ------------------------------------------------------------------------

	/**
	 * Handler for triggering a {@link CancelJobWithSavepoint} message.
	 */
	class TriggerHandler implements RequestHandler {

		/** Current execution graphs. */
		private final ExecutionGraphCache currentGraphs;

		/** Execution context for futures. */
		private final Executor executor;

		public TriggerHandler(ExecutionGraphCache currentGraphs, Executor executor) {
			this.currentGraphs = checkNotNull(currentGraphs);
			this.executor = checkNotNull(executor);
		}

		@Override
		public String[] getPaths() {
			return new String[]{CANCEL_WITH_SAVEPOINT_REST_PATH, CANCEL_WITH_SAVEPOINT_DIRECTORY_REST_PATH};
		}

		@Override
		@SuppressWarnings("unchecked")
		public CompletableFuture<FullHttpResponse> handleRequest(
				Map<String, String> pathParams,
				Map<String, String> queryParams,
				JobManagerGateway jobManagerGateway) {

			if (jobManagerGateway != null) {
				JobID jobId = JobID.fromHexString(pathParams.get("jobid"));
				final CompletableFuture<AccessExecutionGraph> graphFuture;

				graphFuture = currentGraphs.getExecutionGraph(jobId, jobManagerGateway);

				return graphFuture.handleAsync(
					(AccessExecutionGraph graph, Throwable throwable) -> {
						if (throwable != null) {
							throw new CompletionException(new NotFoundException("Could not find ExecutionGraph with jobId " + jobId + '.'));
						} else {
							CheckpointCoordinatorConfiguration jobCheckpointingConfiguration = graph.getCheckpointCoordinatorConfiguration();
							if (jobCheckpointingConfiguration == null) {
								throw new CompletionException(new FlinkException("Cannot find checkpoint coordinator configuration for job."));
							}

							String targetDirectory = pathParams.get("targetDirectory");
							if (targetDirectory == null) {
								if (defaultSavepointDirectory == null) {
									throw new IllegalStateException("No savepoint directory configured. " +
										"You can either specify a directory when triggering this savepoint or " +
										"configure a cluster-wide default via key '" +
											CheckpointingOptions.SAVEPOINT_DIRECTORY.key() + "'.");
								} else {
									targetDirectory = defaultSavepointDirectory;
								}
							}

							try {
								return handleNewRequest(jobManagerGateway, jobId, targetDirectory, jobCheckpointingConfiguration.getCheckpointTimeout());
							} catch (IOException e) {
								throw new CompletionException(new FlinkException("Could not cancel job with savepoint.", e));
							}
						}
					},
					executor);
			} else {
				return FutureUtils.completedExceptionally(new Exception("No connection to the leading JobManager."));
			}
		}

		@SuppressWarnings("unchecked")
		private FullHttpResponse handleNewRequest(JobManagerGateway jobManagerGateway, final JobID jobId, String targetDirectory, long checkpointTimeout) throws IOException {
			// Check whether a request exists
			final long requestId;
			final boolean isNewRequest;
			synchronized (lock) {
				if (inProgress.containsKey(jobId)) {
					requestId = inProgress.get(jobId);
					isNewRequest = false;
				} else {
					requestId = ++requestCounter;
					inProgress.put(jobId, requestId);
					isNewRequest = true;
				}
			}

			if (isNewRequest) {
				boolean success = false;

				try {
					// Trigger cancellation
					CompletableFuture<String> cancelJobFuture = jobManagerGateway
						.cancelJobWithSavepoint(jobId, targetDirectory, Time.milliseconds(checkpointTimeout));

					cancelJobFuture.whenCompleteAsync(
						(String path, Throwable throwable) -> {
							try {
								if (throwable != null) {
									completed.put(requestId, throwable);
								} else {
									completed.put(requestId, path);
								}
							} finally {
								synchronized (lock) {
									inProgress.remove(jobId);
								}
							}
						}, executor);

					success = true;
				} finally {
					synchronized (lock) {
						if (!success) {
							inProgress.remove(jobId);
						}
					}
				}
			}

			// In-progress location
			String location = CANCELLATION_IN_PROGRESS_REST_PATH
					.replace(":jobid", jobId.toString())
					.replace(":requestId", Long.toString(requestId));

			// Accepted response
			StringWriter writer = new StringWriter();
			JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);
			gen.writeStartObject();
			gen.writeStringField("status", "accepted");
			gen.writeNumberField("request-id", requestId);
			gen.writeStringField("location", location);
			gen.writeEndObject();
			gen.close();

			String json = writer.toString();
			byte[] bytes = json.getBytes(ENCODING);

			DefaultFullHttpResponse response = new DefaultFullHttpResponse(
					HttpVersion.HTTP_1_1,
					HttpResponseStatus.ACCEPTED,
					Unpooled.wrappedBuffer(bytes));

			response.headers().set(HttpHeaders.Names.LOCATION, location);

			response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json; charset=" + ENCODING.name());
			response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());

			FullHttpResponse accepted = response;

			return accepted;
		}
	}

	// ------------------------------------------------------------------------
	// In-progress requests
	// ------------------------------------------------------------------------

	/**
	 * Handler for in-progress cancel with savepoint operations.
	 */
	class InProgressHandler implements RequestHandler {

		/** The number of recent checkpoints whose IDs are remembered. */
		private static final int NUM_GHOST_REQUEST_IDS = 16;

		/** Remember some recently completed. */
		private final ArrayDeque<Tuple2<Long, Object>> recentlyCompleted = new ArrayDeque<>(NUM_GHOST_REQUEST_IDS);

		@Override
		public String[] getPaths() {
			return new String[]{CANCELLATION_IN_PROGRESS_REST_PATH};
		}

		@Override
		@SuppressWarnings("unchecked")
		public CompletableFuture<FullHttpResponse> handleRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) {
			JobID jobId = JobID.fromHexString(pathParams.get("jobid"));
			long requestId = Long.parseLong(pathParams.get("requestId"));

			return CompletableFuture.supplyAsync(
				() -> {
					try {
						synchronized (lock) {
							Object result = completed.remove(requestId);

							if (result != null) {
								// Add to recent history
								recentlyCompleted.add(new Tuple2<>(requestId, result));
								if (recentlyCompleted.size() > NUM_GHOST_REQUEST_IDS) {
									recentlyCompleted.remove();
								}

								if (result.getClass() == String.class) {
									String savepointPath = (String) result;
									return createSuccessResponse(requestId, savepointPath);
								} else {
									Throwable cause = (Throwable) result;
									return createFailureResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, requestId, cause.getMessage());
								}
							} else {
								// Check in-progress
								Long inProgressRequestId = inProgress.get(jobId);
								if (inProgressRequestId != null) {
									// Sanity check
									if (inProgressRequestId == requestId) {
										return createInProgressResponse(requestId);
									} else {
										String msg = "Request ID does not belong to JobID";
										return createFailureResponse(HttpResponseStatus.BAD_REQUEST, requestId, msg);
									}
								}

								// Check recent history
								for (Tuple2<Long, Object> recent : recentlyCompleted) {
									if (recent.f0 == requestId) {
										if (recent.f1.getClass() == String.class) {
											String savepointPath = (String) recent.f1;
											return createSuccessResponse(requestId, savepointPath);
										} else {
											Throwable cause = (Throwable) recent.f1;
											return createFailureResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, requestId, cause.getMessage());
										}
									}
								}

								return createFailureResponse(HttpResponseStatus.BAD_REQUEST, requestId, "Unknown job/request ID");
							}
						}
					} catch (Exception e) {
						throw new CompletionException(new FlinkException("Could not handle in progress request.", e));
					}
				});
		}

		private FullHttpResponse createSuccessResponse(long requestId, String savepointPath) throws IOException {
			StringWriter writer = new StringWriter();
			JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);
			gen.writeStartObject();

			gen.writeStringField("status", "success");
			gen.writeNumberField("request-id", requestId);
			gen.writeStringField("savepoint-path", savepointPath);

			gen.writeEndObject();
			gen.close();

			String json = writer.toString();
			byte[] bytes = json.getBytes(ENCODING);

			DefaultFullHttpResponse response = new DefaultFullHttpResponse(
					HttpVersion.HTTP_1_1,
					HttpResponseStatus.CREATED,
					Unpooled.wrappedBuffer(bytes));

			response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json; charset=" + ENCODING.name());
			response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());

			return response;
		}

		private FullHttpResponse createInProgressResponse(long requestId) throws IOException {
			StringWriter writer = new StringWriter();
			JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);
			gen.writeStartObject();

			gen.writeStringField("status", "in-progress");
			gen.writeNumberField("request-id", requestId);

			gen.writeEndObject();
			gen.close();

			String json = writer.toString();
			byte[] bytes = json.getBytes(ENCODING);

			DefaultFullHttpResponse response = new DefaultFullHttpResponse(
					HttpVersion.HTTP_1_1,
					HttpResponseStatus.ACCEPTED,
					Unpooled.wrappedBuffer(bytes));

			response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json; charset=" + ENCODING.name());
			response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());

			return response;
		}

		private FullHttpResponse createFailureResponse(HttpResponseStatus code, long requestId, String errMsg) throws IOException {
			StringWriter writer = new StringWriter();
			JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);
			gen.writeStartObject();

			gen.writeStringField("status", "failed");
			gen.writeNumberField("request-id", requestId);
			gen.writeStringField("cause", errMsg);

			gen.writeEndObject();
			gen.close();

			String json = writer.toString();
			byte[] bytes = json.getBytes(ENCODING);

			DefaultFullHttpResponse response = new DefaultFullHttpResponse(
					HttpVersion.HTTP_1_1,
					code,
					Unpooled.wrappedBuffer(bytes));

			response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json; charset=" + ENCODING.name());
			response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());

			return response;
		}
	}
}
