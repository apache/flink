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

package org.apache.flink.runtime.rest;

import org.apache.flink.runtime.rest.messages.job.JobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObject;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.Attribute;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.DiskFileUpload;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.HttpDataFactory;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.InterfaceHttpData;
import org.apache.flink.shaded.netty4.io.netty.util.AttributeKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Writes multipart/form-data to disk. Delegates all other requests to the next
 * {@link ChannelInboundHandler} in the {@link ChannelPipeline}.
 */
public class FileUploadHandler extends SimpleChannelInboundHandler<HttpObject> {

	private static final Logger LOG = LoggerFactory.getLogger(FileUploadHandler.class);

	public static final String HTTP_ATTRIBUTE_REQUEST = "request";
	public static final String HTTP_ATTRIBUTE_JARS = "jars";
	public static final String HTTP_ATTRIBUTE_ARTIFACTS = "artifacts";

	static final AttributeKey<Path> UPLOADED_FILE = AttributeKey.valueOf("UPLOADED_FILE");
	public static final AttributeKey<JobSubmitRequestBody> SUBMITTED_JOB = AttributeKey.valueOf("SUBMITTED_JOB");

	private static final HttpDataFactory DATA_FACTORY = new DefaultHttpDataFactory(true);

	private final Path uploadDir;

	private HttpPostRequestDecoder currentHttpPostRequestDecoder;

	private HttpRequest currentHttpRequest;

	private JobSubmitRequestBodyBuffer currentJobSubmitRequestBuffer;

	public FileUploadHandler(final Path uploadDir) {
		super(false);
		DiskFileUpload.baseDirectory = uploadDir.normalize().toAbsolutePath().toString();
		this.uploadDir = requireNonNull(uploadDir);
	}

	@Override
	protected void channelRead0(final ChannelHandlerContext ctx, final HttpObject msg) throws Exception {
		if (msg instanceof HttpRequest) {
			final HttpRequest httpRequest = (HttpRequest) msg;
			LOG.trace("Received request. URL:{} Method:{}", httpRequest.getUri(), httpRequest.getMethod());
			if (httpRequest.getMethod().equals(HttpMethod.POST)) {
				if (HttpPostRequestDecoder.isMultipart(httpRequest)) {
					currentHttpPostRequestDecoder = new HttpPostRequestDecoder(DATA_FACTORY, httpRequest);
					currentHttpRequest = httpRequest;
					if (httpRequest.getUri().equals(JobSubmitHeaders.getInstance().getTargetRestEndpointURL())) {
						currentJobSubmitRequestBuffer = new JobSubmitRequestBodyBuffer(uploadDir);
					}
				} else {
					ctx.fireChannelRead(msg);
				}
			} else {
				ctx.fireChannelRead(msg);
			}
		} else if (msg instanceof HttpContent && currentHttpPostRequestDecoder != null) {
			// make sure that we still have a upload dir in case that it got deleted in the meanwhile
			RestServerEndpoint.createUploadDir(uploadDir, LOG);

			final HttpContent httpContent = (HttpContent) msg;
			currentHttpPostRequestDecoder.offer(httpContent);

			while (httpContent != LastHttpContent.EMPTY_LAST_CONTENT && currentHttpPostRequestDecoder.hasNext()) {
				final InterfaceHttpData data = currentHttpPostRequestDecoder.next();
				if (currentHttpRequest.getUri().equals(JobSubmitHeaders.getInstance().getTargetRestEndpointURL())) {
					if (data.getHttpDataType() == InterfaceHttpData.HttpDataType.FileUpload) {
						final DiskFileUpload fileUpload = (DiskFileUpload) data;
						checkState(fileUpload.isCompleted());
						LOG.trace("Received job-submit file upload. attribute:{} fileName:{}.", fileUpload.getName(), fileUpload.getFilename());

						Path dest;
						if (data.getName().startsWith(HTTP_ATTRIBUTE_JARS)) {
							dest = currentJobSubmitRequestBuffer.getJarDir().resolve(fileUpload.getFilename());
							fileUpload.renameTo(dest.toFile());
							currentJobSubmitRequestBuffer.addJar(fileUpload.getFile().toPath());
						} else if (data.getName().startsWith(HTTP_ATTRIBUTE_ARTIFACTS)) {
							dest = currentJobSubmitRequestBuffer.getArtifactDir().resolve(fileUpload.getFilename());
							fileUpload.renameTo(dest.toFile());
							currentJobSubmitRequestBuffer.addUserArtifact(fileUpload.getFile().toPath());
						} else {
							LOG.warn("Received unexpected FileUpload that will be ignored. attribute:{} fileName:{}.", data.getName(), fileUpload.getFilename());
							fileUpload.delete();
						}
					} else if (data.getHttpDataType() == InterfaceHttpData.HttpDataType.Attribute) {
						final Attribute request = (Attribute) data;
						final byte[] requestJson = request.get();
						JobSubmitRequestBody jobSubmitRequestBody = RestMapperUtils.getStrictObjectMapper().readValue(requestJson, JobSubmitHeaders.getInstance().getRequestClass());
						currentJobSubmitRequestBuffer.setJobGraph(jobSubmitRequestBody.serializedJobGraph);
					}
				} else {
					if (data.getHttpDataType() == InterfaceHttpData.HttpDataType.FileUpload) {
						final DiskFileUpload fileUpload = (DiskFileUpload) data;
						checkState(fileUpload.isCompleted());

						final Path dest = uploadDir.resolve(Paths.get(UUID.randomUUID() +
							"_" + fileUpload.getFilename()));
						fileUpload.renameTo(dest.toFile());
						ctx.channel().attr(UPLOADED_FILE).set(dest);
					}
				}
			}

			if (httpContent instanceof LastHttpContent) {
				if (currentJobSubmitRequestBuffer != null) {
					ctx.channel().attr(SUBMITTED_JOB).set(currentJobSubmitRequestBuffer.get());
				}
				ctx.fireChannelRead(currentHttpRequest);
				ctx.fireChannelRead(httpContent);
				reset();
			}
		} else {
			ctx.fireChannelRead(msg);
		}
	}

	private void reset() {
		currentHttpPostRequestDecoder.destroy();
		currentHttpPostRequestDecoder = null;
		currentHttpRequest = null;
		currentJobSubmitRequestBuffer = null;
	}

	/**
	 * Mutable container for the received contents for a {@link JobSubmitRequestBody}.
	 */
	private static final class JobSubmitRequestBodyBuffer {

		private final UUID requestID = UUID.randomUUID();

		private final Path storageDir;
		private final Path jarDir;
		private final Path artifactDir;

		private byte[] jobGraph;
		private final List<Path> jarFiles = new ArrayList<>(4);
		private final List<Path> userArtifacts = new ArrayList<>(4);

		public JobSubmitRequestBodyBuffer(Path uploadDir) throws IOException {
			storageDir = Files.createDirectory(uploadDir.resolve(requestID.toString()));
			jarDir = Files.createDirectory(storageDir.resolve("jars"));
			artifactDir = Files.createDirectory(storageDir.resolve("artifacts"));
		}

		public void setJobGraph(byte[] jobGraph) {
			this.jobGraph = jobGraph;
		}

		public void addJar(Path jar) {
			jarFiles.add(jar);
		}

		public void addUserArtifact(Path artifact) {
			userArtifacts.add(artifact);
		}

		public JobSubmitRequestBody get() {
			return new JobSubmitRequestBody(jobGraph, jarFiles, userArtifacts, storageDir);
		}

		public Path getJarDir() {
			return jarDir;
		}

		public Path getArtifactDir() {
			return artifactDir;
		}
	}
}
