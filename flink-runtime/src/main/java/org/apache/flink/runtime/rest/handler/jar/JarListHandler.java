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

package org.apache.flink.runtime.rest.handler.jar;

import org.apache.flink.api.common.Program;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JarListInfo;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.util.JarWithProgramUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Handle request for listing uploaded jars.
 */
public class JarListHandler extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, JarListInfo, EmptyMessageParameters> {

	private final File jarDir;

	public JarListHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, JarListInfo, EmptyMessageParameters> messageHeaders,
			File jarDir) {
		super(localRestAddress, leaderRetriever, timeout, responseHeaders, messageHeaders);

		this.jarDir = jarDir;
	}

	@Override
	protected CompletableFuture<JarListInfo> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
		String localAddress;
		try {
			localAddress = localAddressFuture.get();
		} catch (Exception e) {
			return FutureUtils.completedExceptionally(e);
		}

		CompletableFuture<JarListInfo> jarListFuture = new CompletableFuture<>();
		return jarListFuture.thenApply(jarListInfo -> {
			try {
				List<JarListInfo.JarFileInfo> jarFileList = new ArrayList<>();
				File[] list = jarDir.listFiles(new FilenameFilter() {
					@Override
					public boolean accept(File dir, String name) {
						return name.endsWith(".jar");
					}
				});
				// last modified ascending order
				Arrays.sort(list, (f1, f2) -> Long.compare(f2.lastModified(), f1.lastModified()));

				for (File f : list) {
					// separate the uuid and the name parts.
					String id = f.getName();

					int startIndex = id.indexOf("_");
					if (startIndex < 0) {
						continue;
					}
					String name = id.substring(startIndex + 1);
					if (name.length() < 5 || !name.endsWith(".jar")) {
						continue;
					}

					List<JarListInfo.JarEntryInfo> jarEntryList = new ArrayList<>();
					String[] classes = new String[0];
					try {
						JarFile jar = new JarFile(f);
						Manifest manifest = jar.getManifest();
						String assemblerClass = null;

						if (manifest != null) {
							assemblerClass = manifest.getMainAttributes().getValue(JarWithProgramUtils.MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS);
							if (assemblerClass == null) {
								assemblerClass = manifest.getMainAttributes().getValue(JarWithProgramUtils.MANIFEST_ATTRIBUTE_MAIN_CLASS);
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

						Class<?> mainClass = null;
						Program program = null;
						try {
							mainClass = JarWithProgramUtils.getMainClass(f, clazz, getClass().getClassLoader());
							program = JarWithProgramUtils.createProgram(mainClass);
						} catch (Exception e) {
							// ignore jar files which throw an error upon creating a PackagedProgram
						}
						if (mainClass != null && program != null) {
							JarListInfo.JarEntryInfo jarEntryInfo = new JarListInfo.JarEntryInfo(clazz, JarWithProgramUtils.getDescription(mainClass, program));
							jarEntryList.add(jarEntryInfo);
						}
					}

					jarFileList.add(new JarListInfo.JarFileInfo(id, name, f.lastModified(), jarEntryList));
				}

				return new JarListInfo(localAddress, jarFileList);
			} catch (Exception e) {
				throw new CompletionException(new FlinkException("Failed to fetch jar list.", e));
			}
		});
	}
}
