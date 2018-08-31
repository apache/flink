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

/*****************************************************************************
 * This code is based on the "HttpStaticFileServerHandler" from the
 * Netty project's HTTP server example.
 *
 * See http://netty.io and
 * https://github.com/netty/netty/blob/4.0/example/src/main/java/io/netty/example/http/file/HttpStaticFileServerHandler.java
 *****************************************************************************/

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.TransientBlobCache;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.rest.handler.RedirectHandler;
import org.apache.flink.runtime.rest.handler.WebHandler;
import org.apache.flink.runtime.rest.handler.router.RoutedRequest;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.DefaultFileRegion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpChunkedInput;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedFile;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.Future;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.GenericFutureListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Request handler that returns the TaskManager log/out files.
 *
 * <p>This code is based on the "HttpStaticFileServerHandler" from the Netty project's HTTP server
 * example.</p>
 */
@ChannelHandler.Sharable
public class TaskManagerLogHandler extends RedirectHandler<JobManagerGateway> implements WebHandler {
	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerLogHandler.class);

	private static final String TASKMANAGER_LOG_REST_PATH = "/taskmanagers/:taskmanagerid/log";
	private static final String TASKMANAGER_OUT_REST_PATH = "/taskmanagers/:taskmanagerid/stdout";

	/** Keep track of last transmitted log, to clean up old ones. */
	private final HashMap<String, TransientBlobKey> lastSubmittedLog = new HashMap<>();
	private final HashMap<String, TransientBlobKey> lastSubmittedStdout = new HashMap<>();

	/** Keep track of request status, prevents multiple log requests for a single TM running concurrently. */
	private final ConcurrentHashMap<String, Boolean> lastRequestPending = new ConcurrentHashMap<>();
	private final Configuration config;

	/** Future of the blob cache. */
	private CompletableFuture<TransientBlobCache> cache;

	/** Indicates which log file should be displayed. */
	private FileMode fileMode;

	private final Executor executor;

	/** Used to control whether this handler serves the .log or .out file. */
	public enum FileMode {
		LOG,
		STDOUT
	}

	public TaskManagerLogHandler(
		GatewayRetriever<JobManagerGateway> retriever,
		Executor executor,
		CompletableFuture<String> localJobManagerAddressPromise,
		Time timeout,
		FileMode fileMode,
		Configuration config) {
		super(localJobManagerAddressPromise, retriever, timeout, Collections.emptyMap());

		this.executor = checkNotNull(executor);
		this.config = config;
		this.fileMode = fileMode;
	}

	@Override
	public String[] getPaths() {
		switch (fileMode) {
			case LOG:
				return new String[]{TASKMANAGER_LOG_REST_PATH};
			case STDOUT:
			default:
				return new String[]{TASKMANAGER_OUT_REST_PATH};
		}
	}

	/**
	 * Response when running with leading JobManager.
	 */
	@Override
	protected void respondAsLeader(final ChannelHandlerContext ctx, final RoutedRequest routedRequest, final JobManagerGateway jobManagerGateway) {
		if (cache == null) {
			CompletableFuture<Integer> blobPortFuture = jobManagerGateway.requestBlobServerPort(timeout);
			cache = blobPortFuture.thenApplyAsync(
				(Integer port) -> {
					try {
						return new TransientBlobCache(config, new InetSocketAddress(jobManagerGateway.getHostname(), port));
					} catch (IOException e) {
						throw new CompletionException(new FlinkException("Could not create TransientBlobCache.", e));
					}
				},
				executor);
		}

		final String taskManagerId = routedRequest.getRouteResult().param(TaskManagersHandler.TASK_MANAGER_ID_KEY);
		final HttpRequest request = routedRequest.getRequest();

		//fetch TaskManager logs if no other process is currently doing it
		if (lastRequestPending.putIfAbsent(taskManagerId, true) == null) {
			try {
				final String unescapedString;

				try {
					unescapedString = URLDecoder.decode(taskManagerId, "UTF-8");
				} catch (UnsupportedEncodingException e) {
					throw new FlinkException("Could not decode task manager id: " + taskManagerId + '.', e);
				}

				final ResourceID resourceId = new ResourceID(unescapedString);
				final CompletableFuture<Optional<Instance>> taskManagerFuture = jobManagerGateway.requestTaskManagerInstance(resourceId, timeout);

				CompletableFuture<TransientBlobKey> blobKeyFuture = taskManagerFuture.thenCompose(
					(Optional<Instance> optTMInstance) -> {
						Instance taskManagerInstance = optTMInstance.orElseThrow(
							() -> new CompletionException(new FlinkException("Could not find instance with " + resourceId + '.')));
						switch (fileMode) {
							case LOG:
								return taskManagerInstance.getTaskManagerGateway().requestTaskManagerLog(timeout);
							case STDOUT:
							default:
								return taskManagerInstance.getTaskManagerGateway().requestTaskManagerStdout(timeout);
						}
					}
				);

				CompletableFuture<String> logPathFuture = blobKeyFuture
					.thenCombineAsync(
						cache,
						(blobKey, blobCache) -> {
							//delete previous log file, if it is different than the current one
							HashMap<String, TransientBlobKey> lastSubmittedFile = fileMode == FileMode.LOG ? lastSubmittedLog : lastSubmittedStdout;
							if (lastSubmittedFile.containsKey(taskManagerId)) {
								// the BlobKey will almost certainly be different but the old file
								// may not exist anymore so we cannot rely on it and need to
								// download the new file anyway, even if the hashes match
								if (!Objects.equals(blobKey, lastSubmittedFile.get(taskManagerId))) {
									if (!blobCache.deleteFromCache(lastSubmittedFile.get(taskManagerId))) {
										throw new CompletionException(new FlinkException("Could not delete file for " + taskManagerId + '.'));
									}
									lastSubmittedFile.put(taskManagerId, blobKey);
								}
							} else {
								lastSubmittedFile.put(taskManagerId, blobKey);
							}
							try {
								return blobCache.getFile(blobKey).getAbsolutePath();
							} catch (IOException e) {
								throw new CompletionException(new FlinkException("Could not retrieve blob for " + blobKey + '.', e));
							}
						},
						executor);

				logPathFuture.exceptionally(
					failure -> {
						display(ctx, request, "Fetching TaskManager log failed.");
						LOG.error("Fetching TaskManager log failed.", failure);
						lastRequestPending.remove(taskManagerId);

						return null;
					});

				logPathFuture.thenAccept(
					filePath -> {
						File file = new File(filePath);
						final RandomAccessFile raf;
						try {
							raf = new RandomAccessFile(file, "r");
						} catch (FileNotFoundException e) {
							display(ctx, request, "Displaying TaskManager log failed.");
							LOG.error("Displaying TaskManager log failed.", e);

							return;
						}
						long fileLength;
						try {
							fileLength = raf.length();
						} catch (IOException ioe) {
							display(ctx, request, "Displaying TaskManager log failed.");
							LOG.error("Displaying TaskManager log failed.", ioe);
							try {
								raf.close();
							} catch (IOException e) {
								LOG.error("Could not close random access file.", e);
							}

							return;
						}
						final FileChannel fc = raf.getChannel();

						HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
						response.headers().set(CONTENT_TYPE, "text/plain");

						if (HttpHeaders.isKeepAlive(request)) {
							response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
						}
						HttpHeaders.setContentLength(response, fileLength);

						// write the initial line and the header.
						ctx.write(response);

						// write the content.
						ChannelFuture lastContentFuture;
						final GenericFutureListener<Future<? super Void>> completionListener = future -> {
							lastRequestPending.remove(taskManagerId);
							fc.close();
							raf.close();
						};
						if (ctx.pipeline().get(SslHandler.class) == null) {
							ctx.write(
								new DefaultFileRegion(fc, 0, fileLength), ctx.newProgressivePromise())
									.addListener(completionListener);
							lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

						} else {
							try {
								lastContentFuture = ctx.writeAndFlush(
									new HttpChunkedInput(new ChunkedFile(raf, 0, fileLength, 8192)),
									ctx.newProgressivePromise())
									.addListener(completionListener);
							} catch (IOException e) {
								display(ctx, request, "Displaying TaskManager log failed.");
								LOG.warn("Could not write http data.", e);

								return;
							}
							// HttpChunkedInput will write the end marker (LastHttpContent) for us.
						}

						// close the connection, if no keep-alive is needed
						if (!HttpHeaders.isKeepAlive(request)) {
							lastContentFuture.addListener(ChannelFutureListener.CLOSE);
						}
					});
			} catch (Exception e) {
				display(ctx, request, "Error: " + e.getMessage());
				LOG.error("Fetching TaskManager log failed.", e);
				lastRequestPending.remove(taskManagerId);
			}
		} else {
			display(ctx, request, "loading...");
		}
	}

	private void display(ChannelHandlerContext ctx, HttpRequest request, String message) {
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
		response.headers().set(CONTENT_TYPE, "text/plain");

		if (HttpHeaders.isKeepAlive(request)) {
			response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
		}

		byte[] buf = message.getBytes(ConfigConstants.DEFAULT_CHARSET);

		ByteBuf b = Unpooled.copiedBuffer(buf);

		HttpHeaders.setContentLength(response, buf.length);

		// write the initial line and the header.
		ctx.write(response);

		ctx.write(b);

		ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

		// close the connection, if no keep-alive is needed
		if (!HttpHeaders.isKeepAlive(request)) {
			lastContentFuture.addListener(ChannelFutureListener.CLOSE);
		}
	}
}
