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

package org.apache.flink.runtime.history;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.util.IOUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonEncoding;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Utility class for writing an archive file to a {@link FileSystem} and reading it back.
 */
public class FsJobArchivist {

	private static final Logger LOG = LoggerFactory.getLogger(FsJobArchivist.class);
	private static final JsonFactory jacksonFactory = new JsonFactory();
	private static final ObjectMapper mapper = new ObjectMapper();

	private static final String ARCHIVE = "archive";
	private static final String PATH = "path";
	private static final String JSON = "json";

	private FsJobArchivist() {
	}

	/**
	 * Writes the given {@link AccessExecutionGraph} to the {@link FileSystem} pointed to by
	 * {@link JobManagerOptions#ARCHIVE_DIR}.
	 *
	 * @param rootPath directory to which the archive should be written to
	 * @param graph  graph to archive
	 * @return path to where the archive was written, or null if no archive was created
	 * @throws IOException
	 * @deprecated only kept for legacy reasons
	 */
	@Deprecated
	public static Path archiveJob(Path rootPath, AccessExecutionGraph graph) throws IOException {
		try {
			FileSystem fs = rootPath.getFileSystem();
			Path path = new Path(rootPath, graph.getJobID().toString());
			OutputStream out = fs.create(path, FileSystem.WriteMode.NO_OVERWRITE);

			try (JsonGenerator gen = jacksonFactory.createGenerator(out, JsonEncoding.UTF8)) {
				gen.writeStartObject();
				gen.writeArrayFieldStart(ARCHIVE);
				for (JsonArchivist archiver : WebMonitorUtils.getJsonArchivists()) {
					for (ArchivedJson archive : archiver.archiveJsonWithPath(graph)) {
						gen.writeStartObject();
						gen.writeStringField(PATH, archive.getPath());
						gen.writeStringField(JSON, archive.getJson());
						gen.writeEndObject();
					}
				}
				gen.writeEndArray();
				gen.writeEndObject();
			} catch (Exception e) {
				fs.delete(path, false);
				throw e;
			}
			LOG.info("Job {} has been archived at {}.", graph.getJobID(), path);
			return path;
		} catch (IOException e) {
			LOG.error("Failed to archive job.", e);
			throw e;
		}
	}

	/**
	 * Writes the given {@link AccessExecutionGraph} to the {@link FileSystem} pointed to by
	 * {@link JobManagerOptions#ARCHIVE_DIR}.
	 *
	 * @param rootPath directory to which the archive should be written to
	 * @param jobId  job id
	 * @param jsonToArchive collection of json-path pairs to that should be archived
	 * @return path to where the archive was written, or null if no archive was created
	 * @throws IOException
	 */
	public static Path archiveJob(Path rootPath, JobID jobId, Collection<ArchivedJson> jsonToArchive) throws IOException {
		try {
			FileSystem fs = rootPath.getFileSystem();
			Path path = new Path(rootPath, jobId.toString());
			OutputStream out = fs.create(path, FileSystem.WriteMode.NO_OVERWRITE);

			try (JsonGenerator gen = jacksonFactory.createGenerator(out, JsonEncoding.UTF8)) {
				gen.writeStartObject();
				gen.writeArrayFieldStart(ARCHIVE);
				for (ArchivedJson archive : jsonToArchive) {
					gen.writeStartObject();
					gen.writeStringField(PATH, archive.getPath());
					gen.writeStringField(JSON, archive.getJson());
					gen.writeEndObject();
				}
				gen.writeEndArray();
				gen.writeEndObject();
			} catch (Exception e) {
				fs.delete(path, false);
				throw e;
			}
			LOG.info("Job {} has been archived at {}.", jobId, path);
			return path;
		} catch (IOException e) {
			LOG.error("Failed to archive job.", e);
			throw e;
		}
	}

	/**
	 * Reads the given archive file and returns a {@link Collection} of contained {@link ArchivedJson}.
	 *
	 * @param file archive to extract
	 * @return collection of archived jsons
	 * @throws IOException if the file can't be opened, read or doesn't contain valid json
	 */
	public static Collection<ArchivedJson> getArchivedJsons(Path file) throws IOException {
		try (FSDataInputStream input = file.getFileSystem().open(file);
			ByteArrayOutputStream output = new ByteArrayOutputStream()) {
			IOUtils.copyBytes(input, output);

			JsonNode archive = mapper.readTree(output.toByteArray());

			Collection<ArchivedJson> archives = new ArrayList<>();
			for (JsonNode archivePart : archive.get(ARCHIVE)) {
				String path = archivePart.get(PATH).asText();
				String json = archivePart.get(JSON).asText();
				archives.add(new ArchivedJson(path, json));
			}
			return archives;
		}
	}
}
