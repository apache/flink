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

package org.apache.flink.runtime.webmonitor.handlers.legacy;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.rest.handler.legacy.AbstractJsonRequestHandler;
import org.apache.flink.runtime.rest.handler.legacy.JsonFactory;
import org.apache.flink.runtime.webmonitor.RuntimeMonitorHandler;
import org.apache.flink.runtime.webmonitor.WebRuntimeMonitor;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Handle request for listing uploaded jars.
 */
public class JarListHandler extends AbstractJsonRequestHandler {

	static final String JAR_LIST_REST_PATH = "/jars";

	private final File jarDir;

	public  JarListHandler(Executor executor, File jarDirectory) {
		super(executor);
		jarDir = jarDirectory;
	}

	@Override
	public String[] getPaths() {
		return new String[]{JAR_LIST_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) {
		return CompletableFuture.supplyAsync(
			() -> {
				try {
					StringWriter writer = new StringWriter();
					JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

					gen.writeStartObject();
					gen.writeStringField("address", queryParams.get(RuntimeMonitorHandler.WEB_MONITOR_ADDRESS_KEY));
					gen.writeArrayFieldStart("files");

					File[] list = jarDir.listFiles(new FilenameFilter() {
						@Override
						public boolean accept(File dir, String name) {
							return name.endsWith(".jar");
						}
					});

					if (list == null) {
						WebRuntimeMonitor.logExternalUploadDirDeletion(jarDir);
						try {
							WebRuntimeMonitor.checkAndCreateUploadDir(jarDir);
						} catch (IOException ioe) {
							// re-throwing an exception here breaks the UI
						}
						list = new File[0];
					}

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

						gen.writeStartObject();
						gen.writeStringField("id", id);
						gen.writeStringField("name", name);
						gen.writeNumberField("uploaded", f.lastModified());
						gen.writeArrayFieldStart("entry");

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
								gen.writeStartObject();
								gen.writeStringField("name", clazz);
								String desc = program.getDescription();
								gen.writeStringField("description", desc == null ? "No description provided" : desc);
								gen.writeEndObject();
							}
						}
						gen.writeEndArray();
						gen.writeEndObject();
					}
					gen.writeEndArray();
					gen.writeEndObject();
					gen.close();
					return writer.toString();
				}
				catch (Exception e) {
					throw new CompletionException(new FlinkException("Failed to fetch jar list.", e));
				}
			},
			executor);

	}
}
