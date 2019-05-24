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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import static java.util.Objects.requireNonNull;

/**
 * Handle request for listing uploaded artifacts.
 */
public class ArtifactListHandler extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, ArtifactListInfo, EmptyMessageParameters> {

	private static final File[] EMPTY_FILES_ARRAY = new File[0];

	private final CompletableFuture<String> localAddressFuture;

	private final File artifactDir;

	private final Executor executor;

	public ArtifactListHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, ArtifactListInfo, EmptyMessageParameters> messageHeaders,
			CompletableFuture<String> localAddressFuture,
			File artifactDir,
			Executor executor) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders);

		this.localAddressFuture = localAddressFuture;
		this.artifactDir = requireNonNull(artifactDir);
		this.executor = requireNonNull(executor);
	}

	@Override
	protected CompletableFuture<ArtifactListInfo> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
		final String localAddress;
		Preconditions.checkState(localAddressFuture.isDone());

		try {
			localAddress = localAddressFuture.get();
		} catch (Exception e) {
			return FutureUtils.completedExceptionally(e);
		}

		return CompletableFuture.supplyAsync(() -> {
			try {
				final File[] list = getArtifactFiles();
				final List<ArtifactListInfo.ArtifactFileInfo> artifactFileList = new ArrayList<>(list.length);
				for (File f : list) {
					// separate the uuid and the name parts.
					String id = f.getName();

					int startIndex = id.indexOf("_");
					if (startIndex < 0) {
						continue;
					}
					String name = id.substring(startIndex + 1);
					if (!((name.length() >= 5 && name.endsWith(".jar")) ||
						(name.length() >= 4 && name.endsWith(".py")) ||
						(name.length() >= 5 && name.endsWith(".zip")) ||
						(name.length() >= 5 && name.endsWith(".egg")))) {
						continue;
					}

					List<ArtifactListInfo.ArtifactEntryInfo> artifactEntryList = new ArrayList<>();
					if (name.endsWith(".jar")) {
						String[] classes = new String[0];
						try (JarFile jar = new JarFile(f)) {
							Manifest manifest = jar.getManifest();
							String assemblerClass = null;

							if (manifest != null) {
								assemblerClass = manifest.getMainAttributes().getValue(PackagedProgram.MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS);
								if (assemblerClass == null) {
									assemblerClass = manifest.getMainAttributes().getValue(PackagedProgram.MANIFEST_ATTRIBUTE_MAIN_CLASS);
								}
							}
							if (assemblerClass != null) {
								classes = assemblerClass.split(",");
							}
						} catch (IOException ignored) {
							// we simply show no entries here
						}

						// show every entry class that can be loaded later on.
						for (String clazz : classes) {
							clazz = clazz.trim();

							PackagedProgram program = null;
							try {
								program = new PackagedProgram(f, clazz, new String[0]);
							} catch (Exception ignored) {
								// ignore jar files which throw an error upon creating a PackagedProgram
							}
							if (program != null) {
								ArtifactListInfo.ArtifactEntryInfo artifactEntryInfo = new ArtifactListInfo.ArtifactEntryInfo(clazz, program.getDescription());
								artifactEntryList.add(artifactEntryInfo);
							}
						}
					}

					artifactFileList.add(new ArtifactListInfo.ArtifactFileInfo(id, name, f.lastModified(), artifactEntryList));
				}

				return new ArtifactListInfo(localAddress, artifactFileList);
			} catch (Exception e) {
				throw new CompletionException(new FlinkException("Failed to fetch artifact list.", e));
			}
		}, executor);
	}

	private File[] getArtifactFiles() {
		final File[] list = artifactDir.listFiles(
			(dir, name) ->
				name.endsWith(".jar") || name.endsWith(".py") || name.endsWith(".zip") || name.endsWith(".egg"));
		if (list == null) {
			log.warn("Artifact upload dir {} does not exist, or had been deleted externally. " +
				"Previously uploaded artifacts are no longer available.", artifactDir);
			return EMPTY_FILES_ARRAY;
		} else {
			// last modified ascending order
			Arrays.sort(list, (f1, f2) -> Long.compare(f2.lastModified(), f1.lastModified()));
			return list;
		}
	}
}
