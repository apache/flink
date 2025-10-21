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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.jackson.JacksonMapperFactory;

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

/** Utility class for writing an archive file to a {@link FileSystem} and reading it back. */
public class FsJsonArchivist {

    private static final Logger LOG = LoggerFactory.getLogger(FsJsonArchivist.class);
    private static final JsonFactory jacksonFactory = new JsonFactory();
    private static final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

    private static final String ARCHIVE = "archive";
    private static final String PATH = "path";
    private static final String JSON = "json";

    private FsJsonArchivist() {}

    /**
     * Writes the given {@link ArchivedJson} to the {@link FileSystem}.
     *
     * @param filePath file path to which the archive should be written to
     * @param jsonToArchive collection of json-path pairs to that should be archived
     */
    public static void writeArchivedJsons(Path filePath, Collection<ArchivedJson> jsonToArchive)
            throws IOException {
        FileSystem fs = filePath.getFileSystem();
        OutputStream out = fs.create(filePath, FileSystem.WriteMode.NO_OVERWRITE);
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
            fs.delete(filePath, false);
            LOG.error("Failed to archive {}", filePath, e);
            throw e;
        }
        LOG.info("Successfully archived {}.", filePath);
    }

    /**
     * Reads the given archive file and returns a {@link Collection} of contained {@link
     * ArchivedJson}.
     *
     * @param file archive to extract
     * @return collection of archived jsons
     * @throws IOException if the file can't be opened, read or doesn't contain valid json
     */
    public static Collection<ArchivedJson> readArchivedJsons(Path file) throws IOException {
        try (FSDataInputStream input = file.getFileSystem().open(file);
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            IOUtils.copyBytes(input, output);

            try {
                JsonNode archive = mapper.readTree(output.toByteArray());

                Collection<ArchivedJson> archives = new ArrayList<>();
                for (JsonNode archivePart : archive.get(ARCHIVE)) {
                    String path = archivePart.get(PATH).asText();
                    String json = archivePart.get(JSON).asText();
                    archives.add(new ArchivedJson(path, json));
                }
                return archives;
            } catch (NullPointerException npe) {
                // occurs if the archive is empty or any of the expected fields are not present
                throw new IOException(
                        "Json archive ("
                                + file.getPath()
                                + ") did not conform to expected format.");
            }
        }
    }
}
