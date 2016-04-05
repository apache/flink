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

package org.apache.flink.runtime.webmonitor.handlers;

/*****************************************************************************
 * This code is based on the "HttpStaticFileServerHandler" from the
 * Netty project's HTTP server example.
 *
 * See http://netty.io and
 * https://github.com/netty/netty/blob/4.0/example/src/main/java/io/netty/example/http/file/HttpStaticFileServerHandler.java
 *****************************************************************************/

import akka.dispatch.Mapper;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.router.Routed;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobCache;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.TaskManagerMessages;
import org.apache.flink.runtime.webmonitor.JobManagerRetriever;
import org.apache.flink.runtime.webmonitor.RuntimeMonitorHandlerBase;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Request handler that returns the TaskManager log/out files.
 *
 * <p>This code is based on the "HttpStaticFileServerHandler" from the Netty project's HTTP server
 * example.</p>
 */
@ChannelHandler.Sharable
public class TaskManagerLogHandler extends RuntimeMonitorHandlerBase {
	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerLogHandler.class);

	/** Keep track of last transmitted log, to clean up old ones */
	private final HashMap<String, BlobKey> lastSubmittedLog = new HashMap<>();
	private final HashMap<String, BlobKey> lastSubmittedStdout = new HashMap<>();

	/** Keep track of request status, prevents multiple log requests for a single TM running concurrently */
	private final ConcurrentHashMap<String, Boolean> lastRequestPending = new ConcurrentHashMap<>();
	private final Configuration config;

	/** */
	private Future<BlobCache> cache;

	/** Indicates which log file should be displayed; true indicates .log, false indicates .out */
	private boolean serveLogFile;

	private final ExecutionContextExecutor executor;

	public enum FileMode {
		LOG,
		STDOUT
	}

	public TaskManagerLogHandler(
		JobManagerRetriever retriever,
		ExecutionContextExecutor executor,
		Future<String> localJobManagerAddressPromise,
		FiniteDuration timeout,
		FileMode fileMode,
		Configuration config) throws IOException {
		super(retriever, localJobManagerAddressPromise, timeout);

		this.executor = checkNotNull(executor);
		this.config = config;
		switch (fileMode) {
			case LOG:
				serveLogFile = true;
				break;
			case STDOUT:
				serveLogFile = false;
				break;
		}
	}

	/**
	 * Response when running with leading JobManager.
	 */
	@Override
	protected void respondAsLeader(final ChannelHandlerContext ctx, final Routed routed, final ActorGateway jobManager) {
		if (cache == null) {
			Future<Object> portFuture = jobManager.ask(JobManagerMessages.getRequestBlobManagerPort(), timeout);
			cache = portFuture.map(new Mapper<Object, BlobCache>() {
				@Override
				public BlobCache apply(Object result) {
					Option<String> hostOption = jobManager.actor().path().address().host();
					String host = hostOption.isDefined() ? hostOption.get() : "localhost";
					int port = (int) result;
					return new BlobCache(new InetSocketAddress(host, port), config);
				}
			}, executor);
		}

		final String taskManagerID = routed.pathParams().get(TaskManagersHandler.TASK_MANAGER_ID_KEY);
		final HttpRequest request = routed.request();

		//fetch TaskManager logs if no other process is currently doing it
		if (lastRequestPending.putIfAbsent(taskManagerID, true) == null) {
			try {
				InstanceID instanceID = new InstanceID(StringUtils.hexStringToByte(taskManagerID));
				Future<Object> taskManagerFuture = jobManager.ask(new JobManagerMessages.RequestTaskManagerInstance(instanceID), timeout);

				Future<Object> blobKeyFuture = taskManagerFuture.flatMap(new Mapper<Object, Future<Object>>() {
					@Override
					public Future<Object> apply(Object instance) {
						Instance taskManager = ((JobManagerMessages.TaskManagerInstance) instance).instance().get();
						return taskManager.getActorGateway().ask(serveLogFile ? TaskManagerMessages.getRequestTaskManagerLog() : TaskManagerMessages.getRequestTaskManagerStdout(), timeout);
					}
				}, executor);

				Future<Object> logPathFuture = cache.zip(blobKeyFuture).map(new Mapper<Tuple2<BlobCache, Object>, Object>() {
					@Override
					public Object checkedApply(Tuple2<BlobCache, Object> instance) throws Exception {
						BlobCache cache = instance._1();
						if (instance._2() instanceof Exception) {
							throw (Exception) instance._2();
						}
						BlobKey blobKey = (BlobKey) instance._2();

						//delete previous log file, if it is different than the current one
						HashMap<String, BlobKey> lastSubmittedFile = serveLogFile ? lastSubmittedLog : lastSubmittedStdout;
						if (lastSubmittedFile.containsKey(taskManagerID)) {
							if (!blobKey.equals(lastSubmittedFile.get(taskManagerID))) {
								cache.deleteGlobal(lastSubmittedFile.get(taskManagerID));
								lastSubmittedFile.put(taskManagerID, blobKey);
							}
						} else {
							lastSubmittedFile.put(taskManagerID, blobKey);
						}
						return cache.getURL(blobKey).getFile();
					}
				}, executor);

				logPathFuture.onFailure(new OnFailure() {
					@Override
					public void onFailure(Throwable failure) throws Throwable {
						display(ctx, request, "Fetching TaskManager log failed.");
						LOG.error("Fetching TaskManager log failed.", failure);
						lastRequestPending.remove(taskManagerID);
					}
				}, executor);

				logPathFuture.onSuccess(new OnSuccess<Object>() {
					@Override
					public void onSuccess(Object filePathOption) throws Throwable {
						String filePath = (String) filePathOption;

						File file = new File(filePath);
						final RandomAccessFile raf;
						try {
							raf = new RandomAccessFile(file, "r");
						} catch (FileNotFoundException e) {
							display(ctx, request, "Displaying TaskManager log failed.");
							LOG.error("Displaying TaskManager log failed.", e);
							return;
						}
						long fileLength = raf.length();
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
						ctx.write(new DefaultFileRegion(fc, 0, fileLength), ctx.newProgressivePromise())
							.addListener(new GenericFutureListener<io.netty.util.concurrent.Future<? super Void>>() {
								@Override
								public void operationComplete(io.netty.util.concurrent.Future<? super Void> future) throws Exception {
									lastRequestPending.remove(taskManagerID);
									fc.close();
									raf.close();
								}
							});
						ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

						// close the connection, if no keep-alive is needed
						if (!HttpHeaders.isKeepAlive(request)) {
							lastContentFuture.addListener(ChannelFutureListener.CLOSE);
						}
					}
				}, executor);
			} catch (Exception e) {
				display(ctx, request, "Error: " + e.getMessage());
				LOG.error("Fetching TaskManager log failed.", e);
				lastRequestPending.remove(taskManagerID);
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

		byte[] buf = message.getBytes();

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
