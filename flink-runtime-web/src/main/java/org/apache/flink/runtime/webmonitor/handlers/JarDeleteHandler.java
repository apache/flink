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

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.runtime.instance.ActorGateway;

import java.io.File;
import java.io.FilenameFilter;
import java.io.StringWriter;
import java.util.Map;

/**
 * Handles requests for deletion of jars.
 */
public class JarDeleteHandler implements RequestHandler {

	private final File jarDir;

	public JarDeleteHandler(File jarDirectory) {
		jarDir = jarDirectory;
	}

	@Override
	public String handleRequest(Map<String, String> pathParams, Map<String, String> queryParams, ActorGateway jobManager) throws Exception {
		final String file = pathParams.get("jarid");
		try {
			File[] list = jarDir.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.equals(file);
				}
			});
			boolean success = false;
			for (File f: list) {
				// although next to impossible for multiple files, we still delete them.
				success = success || f.delete();
			}
			StringWriter writer = new StringWriter();
			JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);
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
			throw new RuntimeException("Failed to delete jar id " + pathParams.get("jarid") + ": " + e.getMessage(), e);
		}
	}
}
