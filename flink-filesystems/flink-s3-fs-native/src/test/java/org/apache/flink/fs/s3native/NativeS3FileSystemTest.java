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

package org.apache.flink.fs.s3native;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the status of empty objects, directory markers and missing keys against a stub S3 endpoint,
 * and the exists() and create() checks that rest on it.
 */
class NativeS3FileSystemTest {

    private static final Instant LAST_MODIFIED = Instant.parse("2026-10-25T08:00:00Z");
    private static final Path MARKER = new Path("s3://test-bucket/app/startup-marker");

    @TempDir java.nio.file.Path tmp;

    private StubS3Endpoint s3;
    private NativeS3FileSystem fs;

    @BeforeEach
    void setUp() throws IOException {
        s3 = new StubS3Endpoint();
        S3ClientProvider clientProvider =
                S3ClientProvider.builder()
                        .endpoint(s3.endpoint)
                        .region("us-east-1")
                        .accessKey("test-key")
                        .secretKey("test-secret")
                        .pathStyleAccess(true)
                        .build();
        fs =
                new NativeS3FileSystem(
                        clientProvider,
                        URI.create("s3://test-bucket/"),
                        null,
                        0,
                        tmp.toString(),
                        5 * 1024 * 1024,
                        1,
                        null,
                        false,
                        64 * 1024,
                        Duration.ofSeconds(10),
                        false);
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            fs.closeAsync().get(10, TimeUnit.SECONDS);
        } finally {
            s3.close();
        }
    }

    @Test
    void emptyObjectWithoutChildrenIsAFile() throws IOException {
        s3.objectExists = true;
        s3.contentLength = 0L;

        FileStatus status = fs.getFileStatus(MARKER);

        assertThat(status.isDir()).isFalse();
        assertThat(status.getLen()).isZero();
        assertThat(status.getModificationTime()).isEqualTo(LAST_MODIFIED.toEpochMilli());
        assertThat(status.getPath()).isEqualTo(MARKER);
        assertThat(s3.listQueries).hasSize(1);
        assertThat(s3.listQueries.get(0)).contains("prefix=app/startup-marker/", "max-keys=1");

        assertThat(fs.exists(MARKER)).isTrue();
    }

    @Test
    void emptyObjectWithoutLengthAndModificationHeadersIsAFile() throws IOException {
        s3.objectExists = true;
        s3.contentLength = null;
        s3.lastModifiedHeader = false;
        long before = System.currentTimeMillis();

        FileStatus status = fs.getFileStatus(MARKER);

        assertThat(status.isDir()).isFalse();
        assertThat(status.getLen()).isZero();
        assertThat(status.getModificationTime()).isBetween(before, System.currentTimeMillis());
    }

    @Test
    void emptyObjectWithChildrenIsADirectory() throws IOException {
        s3.objectExists = true;
        s3.contentLength = 0L;
        s3.children.add("app/startup-marker/child");

        assertThat(fs.getFileStatus(MARKER).isDir()).isTrue();
    }

    @Test
    void directoryMarkerKeyIsADirectory() throws IOException {
        s3.objectExists = true;
        s3.contentLength = 0L;
        // Path(URI) keeps the trailing slash that Path(String) strips.
        Path marker = new Path(URI.create("s3://test-bucket/app/dir/"));

        assertThat(fs.getFileStatus(marker).isDir()).isTrue();
        assertThat(s3.listQueries).isEmpty();
    }

    @Test
    void directoryMarkerKeyWithContentIsADirectory() throws IOException {
        s3.objectExists = true;
        s3.contentLength = 42L;
        Path marker = new Path(URI.create("s3://test-bucket/app/dir/"));

        assertThat(fs.getFileStatus(marker).isDir()).isTrue();
        assertThat(s3.listQueries).isEmpty();
    }

    @Test
    void nonEmptyObjectIsAFileOfItsLength() throws IOException {
        s3.objectExists = true;
        s3.contentLength = 42L;

        FileStatus status = fs.getFileStatus(MARKER);

        assertThat(status.isDir()).isFalse();
        assertThat(status.getLen()).isEqualTo(42L);
        assertThat(s3.listQueries).isEmpty();
    }

    @Test
    void missingKeyWithChildrenIsADirectory() throws IOException {
        s3.children.add("app/startup-marker/child");

        assertThat(fs.getFileStatus(MARKER).isDir()).isTrue();
    }

    @Test
    void missingKeyWithCommonPrefixIsADirectory() throws IOException {
        s3.commonPrefixes.add("app/startup-marker/sub/");

        assertThat(fs.getFileStatus(MARKER).isDir()).isTrue();
    }

    @Test
    void missingKeyWithoutChildrenIsNotFound() throws IOException {
        assertThatThrownBy(() -> fs.getFileStatus(MARKER))
                .isInstanceOf(FileNotFoundException.class)
                .hasMessageContaining(MARKER.toString());
        assertThat(fs.exists(MARKER)).isFalse();
    }

    @Test
    void createWithNoOverwriteRefusesAnExistingEmptyObject() {
        s3.objectExists = true;
        s3.contentLength = 0L;

        assertThatThrownBy(() -> fs.create(MARKER, WriteMode.NO_OVERWRITE))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("File already exists");
    }

    /**
     * An HTTP endpoint standing in for S3 over path-style requests. HeadObject answers 404 until a
     * test marks the object as existing, then 200 with the Content-Length and Last-Modified headers
     * a test leaves on; ListObjectsV2 answers the child keys and common prefixes a test adds and
     * records each listing's decoded query string. The fields are written by the test thread and
     * read by the server thread, so they are volatile or concurrent lists.
     */
    private static final class StubS3Endpoint implements AutoCloseable {

        private static final String LAST_MODIFIED_HEADER =
                DateTimeFormatter.RFC_1123_DATE_TIME.format(LAST_MODIFIED.atOffset(ZoneOffset.UTC));
        private static final String LISTING_HEAD =
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                        + "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                        + "<Name>test-bucket</Name><MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated>";
        private static final String CONTENTS_TAIL =
                "</Key><LastModified>2026-10-25T08:00:00.000Z</LastModified>"
                        + "<ETag>&quot;etag&quot;</ETag><Size>1</Size>"
                        + "<StorageClass>STANDARD</StorageClass></Contents>";

        private final HttpServer server;
        final String endpoint;

        volatile boolean objectExists;
        @Nullable volatile Long contentLength;
        volatile boolean lastModifiedHeader = true;
        final List<String> children = new CopyOnWriteArrayList<>();
        final List<String> commonPrefixes = new CopyOnWriteArrayList<>();
        final List<String> listQueries = new CopyOnWriteArrayList<>();

        StubS3Endpoint() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", this::handle);
            server.start();
            endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
        }

        private void handle(HttpExchange exchange) throws IOException {
            try (exchange) {
                String query = exchange.getRequestURI().getRawQuery();
                Long length = contentLength;
                if ("HEAD".equals(exchange.getRequestMethod()) && !objectExists) {
                    exchange.sendResponseHeaders(404, -1);
                } else if ("HEAD".equals(exchange.getRequestMethod())) {
                    if (length != null) {
                        exchange.getResponseHeaders().set("Content-Length", length.toString());
                    }
                    if (lastModifiedHeader) {
                        exchange.getResponseHeaders().set("Last-Modified", LAST_MODIFIED_HEADER);
                    }
                    exchange.sendResponseHeaders(200, -1);
                } else if (query != null && query.contains("list-type=2")) {
                    listQueries.add(URLDecoder.decode(query, StandardCharsets.UTF_8));
                    StringBuilder xml = new StringBuilder(LISTING_HEAD);
                    for (String child : children) {
                        xml.append("<Contents><Key>").append(child).append(CONTENTS_TAIL);
                    }
                    for (String commonPrefix : commonPrefixes) {
                        xml.append("<CommonPrefixes><Prefix>")
                                .append(commonPrefix)
                                .append("</Prefix></CommonPrefixes>");
                    }
                    byte[] body =
                            xml.append("</ListBucketResult>")
                                    .toString()
                                    .getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(200, body.length);
                    exchange.getResponseBody().write(body);
                } else {
                    exchange.sendResponseHeaders(400, -1);
                }
            }
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
