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

import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.rest.handler.legacy.AbstractJsonRequestHandler;

import java.io.File;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Handles requests for uploading of jars.
 */
public class JarUploadHandler extends AbstractJsonRequestHandler {

	static final String JAR_UPLOAD_REST_PATH = "/jars/upload";

	private final File jarDir;

	public JarUploadHandler(Executor executor, File jarDir) {
		super(executor);
		this.jarDir = jarDir;
	}

	@Override
	public String[] getPaths() {
		return new String[]{JAR_UPLOAD_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleJsonRequest(
			Map<String, String> pathParams,
			Map<String, String> queryParams,
			JobManagerGateway jobManagerGateway) {

		String tempFilePath = queryParams.get("filepath");
		String filename = queryParams.get("filename");

		return CompletableFuture.supplyAsync(
			() -> {
				File tempFile;
				if (tempFilePath != null && (tempFile = new File(tempFilePath)).exists()) {
					if (!tempFile.getName().endsWith(".jar")) {
						//noinspection ResultOfMethodCallIgnored
						tempFile.delete();
						return "{\"error\": \"Only Jar files are allowed.\"}";
					}

					String filenameWithUUID = UUID.randomUUID() + "_" + filename;
					File newFile = new File(jarDir, filenameWithUUID);
					if (tempFile.renameTo(newFile)) {
						// all went well
						return "{\"status\": \"success\", \"filename\": \"" + filenameWithUUID + "\"}";
					}
					else {
						//noinspection ResultOfMethodCallIgnored
						tempFile.delete();
					}
				}

				return "{\"error\": \"Failed to upload the file.\"}";
			},
			executor);
	}
}
