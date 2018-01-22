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

import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.rest.handler.legacy.AbstractJsonRequestHandler;
import org.apache.flink.runtime.rest.handler.legacy.JsonFactory;
import org.apache.flink.runtime.webmonitor.WebRuntimeMonitor;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Handles requests for deletion of jars.
 */
public class JarDeleteHandler extends AbstractJsonRequestHandler {

	static final String JAR_DELETE_REST_PATH = "/jars/:jarid";

	private final File jarDir;

	public JarDeleteHandler(Executor executor, File jarDirectory) {
		super(executor);
		jarDir = jarDirectory;
	}

	@Override
	public String[] getPaths() {
		return new String[]{JAR_DELETE_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) {
		final String file = pathParams.get("jarid");
		return CompletableFuture.supplyAsync(
			() -> {
				try {
					File[] list = jarDir.listFiles(new FilenameFilter() {
						@Override
						public boolean accept(File dir, String name) {
							return name.equals(file);
						}
					});

					if (list == null) {
						WebRuntimeMonitor.logExternalUploadDirDeletion(jarDir);
						try {
							WebRuntimeMonitor.checkAndCreateUploadDir(jarDir);
						} catch (IOException ioe) {
							// entire directory doesn't exist anymore, continue as if deletion succeeded
						}
						list = new File[0];
					}

					boolean success = false;
					for (File f: list) {
						// although next to impossible for multiple files, we still delete them.
						success = success || f.delete();
					}
					StringWriter writer = new StringWriter();
					JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);
					gen.writeStartObject();
					if (!success) {
						// this seems to always fail on Windows.
						gen.writeStringField("error", "The requested jar couldn't be deleted. Please try again.");
					}
					gen.writeEndObject();
					gen.close();
					return writer.toString();
				}
				catch (Exception e) {
					throw new CompletionException(new FlinkException("Failed to delete jar id " + pathParams.get("jarid") + '.', e));
				}
			},
			executor);
	}
}
