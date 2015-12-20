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

import org.apache.flink.runtime.instance.ActorGateway;

import java.io.File;
import java.util.Map;

/**
 * Handles requests for uploading of jars.
 */
public class JarUploadHandler implements RequestHandler, RequestHandler.JsonResponse {

	private final File jarDir;

	public JarUploadHandler(File jarDir) {
		this.jarDir = jarDir;
	}

	@Override
	public String handleRequest(Map<String, String> pathParams, Map<String, String> queryParams, ActorGateway jobManager) throws Exception {
		String filename = queryParams.get("file");
		if(filename != null) {
			File f = new File(jarDir, filename);
			if (f.exists()) {
				if (f.getName().endsWith(".jar")) {
					return "{}";
				} else {
					f.delete();
					return "{\"error\": \"Only Jar files are allowed.\"}";
				}
			}
		}
		return "{\"error\": \"Failed to upload the file.\"}";
	}
}
