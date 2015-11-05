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
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.webmonitor.RuntimeMonitorHandler;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.UUID;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

public class JarListHandler implements RequestHandler, RequestHandler.JsonResponse {

	private final File jarDir;

	public  JarListHandler(File jarDirectory) {
		jarDir = jarDirectory;
	}

	@Override
	public String handleRequest(Map<String, String> pathParams, Map<String, String> queryParams, ActorGateway jobManager) throws Exception {
		try {
			StringWriter writer = new StringWriter();
			JsonGenerator gen = JsonFactory.jacksonFactory.createJsonGenerator(writer);
			gen.writeStartObject();
			gen.writeStringField("address", queryParams.get(RuntimeMonitorHandler.WEB_MONITOR_ADDRESS_KEY));
			gen.writeArrayFieldStart("files");

			File[] list = jarDir.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.endsWith(".jar");
				}
			});
			for (File f : list) {
				// separate the uuid and the name parts.
				String id = f.getName();
				int startIndex = id.indexOf("_");
				try {
					UUID.fromString(id.substring(0, startIndex));
					if (id.substring(startIndex + 1).equals(".jar")) {
						throw new Exception();
					}
				} catch (Exception e) {
					continue;
				}
				String name = id.substring(startIndex + 1);
				gen.writeStartObject();
				gen.writeStringField("id", id);
				gen.writeStringField("name", name);
				gen.writeNumberField("uploaded", f.lastModified());
				gen.writeArrayFieldStart("entry");
				String[] classes = new String[0];
				try {
					JarFile jar = new JarFile(f);
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
				} catch (IOException e) {
					//
				}
				// show every entry class that can be loaded later on.
				PackagedProgram program;
				for (String clazz : classes) {
					clazz = clazz.trim();
					try {
						program = new PackagedProgram(f, clazz, new String[0]);
						gen.writeStartObject();
						gen.writeStringField("name", clazz);
						String desc = program.getDescription();
						gen.writeStringField("description", desc == null ? "No description provided" : desc);
						gen.writeEndObject();
					} catch (ProgramInvocationException e) {
						//
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
			throw new RuntimeException("Failed to fetch jar list: " + e.getMessage(), e);
		}
	}
}
