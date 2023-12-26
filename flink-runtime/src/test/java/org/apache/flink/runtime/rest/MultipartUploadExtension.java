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

package org.apache.flink.runtime.rest;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.testutils.CustomExtension;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;
import org.apache.flink.runtime.rest.util.TestRestServerEndpoint;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.BiConsumerWithException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test base for verifying support of multipart uploads via REST. */
public class MultipartUploadExtension implements CustomExtension {
    private static final Logger LOG = LoggerFactory.getLogger(MultipartUploadExtension.class);

    private final Supplier<Path> tmpDirectorySupplier;

    private RestServerEndpoint serverEndpoint;
    protected String serverAddress;
    protected InetSocketAddress serverSocketAddress;

    protected MultipartMixedHandler mixedHandler;
    protected MultipartJsonHandler jsonHandler;
    protected MultipartFileHandler fileHandler;
    protected File file1;
    protected File file2;

    private Path configuredUploadDir;

    private BiConsumerWithException<HandlerRequest<?>, RestfulGateway, RestHandlerException>
            fileUploadVerifier;

    public MultipartUploadExtension(Supplier<Path> tmpDirectorySupplier) {
        this.tmpDirectorySupplier = tmpDirectorySupplier;
    }

    @Override
    public void before(ExtensionContext context) throws Exception {
        Path tmpDirectory = tmpDirectorySupplier.get();
        Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "0");
        config.setString(RestOptions.ADDRESS, "localhost");
        // set this to a lower value on purpose to test that files larger than the content limit are
        // still accepted
        config.setInteger(RestOptions.SERVER_MAX_CONTENT_LENGTH, 1024 * 1024);
        configuredUploadDir = TempDirUtils.newFolder(tmpDirectory).toPath();
        config.setString(WebOptions.UPLOAD_DIR, configuredUploadDir.toString());

        RestfulGateway mockRestfulGateway = new TestingRestfulGateway();

        final GatewayRetriever<RestfulGateway> mockGatewayRetriever =
                () -> CompletableFuture.completedFuture(mockRestfulGateway);

        file1 = TempDirUtils.newFile(tmpDirectory);
        try (RandomAccessFile rw = new RandomAccessFile(file1, "rw")) {
            // magic value that reliably reproduced https://github.com/netty/netty/issues/11668
            rw.setLength(5043444);
        }
        file2 = TempDirUtils.newFile(tmpDirectory);
        Files.write(file2.toPath(), "world".getBytes(ConfigConstants.DEFAULT_CHARSET));

        mixedHandler = new MultipartMixedHandler(mockGatewayRetriever);
        jsonHandler = new MultipartJsonHandler(mockGatewayRetriever);
        fileHandler = new MultipartFileHandler(mockGatewayRetriever);

        serverEndpoint =
                TestRestServerEndpoint.builder(config)
                        .withHandler(mixedHandler)
                        .withHandler(jsonHandler)
                        .withHandler(fileHandler)
                        .buildAndStart();

        serverAddress = serverEndpoint.getRestBaseUrl();
        serverSocketAddress = serverEndpoint.getServerAddress();

        this.setFileUploadVerifier(
                (request, restfulGateway) -> {
                    // the default verifier checks for identiy (i.e. same name and content) of all
                    // uploaded files
                    assertUploadedFilesEqual(request, getFilesToUpload());
                });
    }

    public static void assertUploadedFilesEqual(HandlerRequest<?> request, Collection<File> files)
            throws IOException {
        List<Path> expectedFiles = files.stream().map(File::toPath).collect(Collectors.toList());
        List<Path> uploadedFiles =
                request.getUploadedFiles().stream().map(File::toPath).collect(Collectors.toList());

        assertThat(uploadedFiles).hasSameSizeAs(expectedFiles);

        List<Path> expectedList = new ArrayList<>(expectedFiles);
        List<Path> actualList = new ArrayList<>(uploadedFiles);
        expectedList.sort(Comparator.comparing(Path::toString));
        actualList.sort(Comparator.comparing(Path::toString));

        for (int x = 0; x < expectedList.size(); x++) {
            Path expected = expectedList.get(x);
            Path actual = actualList.get(x);

            assertThat(actual.getFileName()).hasToString(expected.getFileName().toString());

            byte[] originalContent = Files.readAllBytes(expected);
            byte[] receivedContent = Files.readAllBytes(actual);
            assertThat(receivedContent).isEqualTo(originalContent);
        }
    }

    public void setFileUploadVerifier(
            BiConsumerWithException<
                            HandlerRequest<? extends RequestBody>, RestfulGateway, Exception>
                    verifier) {
        this.fileUploadVerifier =
                (request, restfulGateway) -> {
                    try {
                        verifier.accept(request, restfulGateway);
                    } catch (Exception e) {
                        // return 505 to differentiate from common BAD_REQUEST responses in this
                        // test
                        throw new RestHandlerException(
                                "Test verification failed.",
                                HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED,
                                e);
                    }
                };
    }

    public Collection<File> getFilesToUpload() {
        return Arrays.asList(file1, file2);
    }

    public String getServerAddress() {
        return serverAddress;
    }

    public InetSocketAddress getServerSocketAddress() {
        return serverSocketAddress;
    }

    public MultipartMixedHandler getMixedHandler() {
        return mixedHandler;
    }

    public MultipartFileHandler getFileHandler() {
        return fileHandler;
    }

    public MultipartJsonHandler getJsonHandler() {
        return jsonHandler;
    }

    public Path getUploadDirectory() {
        return configuredUploadDir;
    }

    public void resetState() {
        mixedHandler.lastReceivedRequest = null;
        jsonHandler.lastReceivedRequest = null;
    }

    @Override
    public void after(ExtensionContext context) throws Exception {
        if (serverEndpoint != null) {
            try {
                serverEndpoint.close();
            } catch (Exception e) {
                LOG.warn("Could not properly shutdown RestServerEndpoint.", e);
            }
            serverEndpoint = null;
        }
    }

    public void assertUploadDirectoryIsEmpty() throws IOException {
        Path actualUploadDir;
        try (Stream<Path> containedFiles = Files.list(configuredUploadDir)) {
            List<Path> files = containedFiles.collect(Collectors.toList());
            Preconditions.checkArgument(
                    1 == files.size(),
                    "Directory structure in rest upload directory has changed. Test must be adjusted");
            actualUploadDir = files.get(0);
        }
        try (Stream<Path> containedFiles = Files.list(actualUploadDir)) {
            assertThat(containedFiles).withFailMessage("Not all files were cleaned up.").isEmpty();
        }
    }

    /**
     * Handler that accepts a mixed request consisting of a {@link TestRequestBody} and {@link
     * #file1} and {@link #file2}.
     */
    public class MultipartMixedHandler
            extends AbstractRestHandler<
                    RestfulGateway, TestRequestBody, EmptyResponseBody, EmptyMessageParameters> {

        volatile TestRequestBody lastReceivedRequest = null;

        MultipartMixedHandler(GatewayRetriever<RestfulGateway> leaderRetriever) {
            super(
                    leaderRetriever,
                    RpcUtils.INF_TIMEOUT,
                    Collections.emptyMap(),
                    MultipartMixedHeaders.INSTANCE);
        }

        @Override
        protected CompletableFuture<EmptyResponseBody> handleRequest(
                @Nonnull HandlerRequest<TestRequestBody> request, @Nonnull RestfulGateway gateway)
                throws RestHandlerException {
            MultipartUploadExtension.this.fileUploadVerifier.accept(request, gateway);
            this.lastReceivedRequest = request.getRequestBody();
            return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
        }
    }

    private static final class MultipartMixedHeaders
            implements RuntimeMessageHeaders<
                    TestRequestBody, EmptyResponseBody, EmptyMessageParameters> {
        private static final MultipartMixedHeaders INSTANCE = new MultipartMixedHeaders();

        private MultipartMixedHeaders() {}

        @Override
        public Class<TestRequestBody> getRequestClass() {
            return TestRequestBody.class;
        }

        @Override
        public Class<EmptyResponseBody> getResponseClass() {
            return EmptyResponseBody.class;
        }

        @Override
        public HttpResponseStatus getResponseStatusCode() {
            return HttpResponseStatus.OK;
        }

        @Override
        public String getDescription() {
            return "";
        }

        @Override
        public EmptyMessageParameters getUnresolvedMessageParameters() {
            return EmptyMessageParameters.getInstance();
        }

        @Override
        public HttpMethodWrapper getHttpMethod() {
            return HttpMethodWrapper.POST;
        }

        @Override
        public String getTargetRestEndpointURL() {
            return "/test/upload/mixed";
        }

        @Override
        public boolean acceptsFileUploads() {
            return true;
        }
    }

    /** Handler that accepts a json request consisting of a {@link TestRequestBody}. */
    public static class MultipartJsonHandler
            extends AbstractRestHandler<
                    RestfulGateway, TestRequestBody, EmptyResponseBody, EmptyMessageParameters> {
        volatile TestRequestBody lastReceivedRequest = null;

        MultipartJsonHandler(GatewayRetriever<RestfulGateway> leaderRetriever) {
            super(
                    leaderRetriever,
                    RpcUtils.INF_TIMEOUT,
                    Collections.emptyMap(),
                    MultipartJsonHeaders.INSTANCE);
        }

        @Override
        protected CompletableFuture<EmptyResponseBody> handleRequest(
                @Nonnull HandlerRequest<TestRequestBody> request, @Nonnull RestfulGateway gateway)
                throws RestHandlerException {
            Collection<Path> uploadedFiles =
                    request.getUploadedFiles().stream()
                            .map(File::toPath)
                            .collect(Collectors.toList());
            if (!uploadedFiles.isEmpty()) {
                throw new RestHandlerException(
                        "This handler should not have received file uploads.",
                        HttpResponseStatus.INTERNAL_SERVER_ERROR);
            }
            this.lastReceivedRequest = request.getRequestBody();
            return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
        }

        private static final class MultipartJsonHeaders extends TestHeadersBase<TestRequestBody> {
            private static final MultipartJsonHandler.MultipartJsonHeaders INSTANCE =
                    new MultipartJsonHandler.MultipartJsonHeaders();

            private MultipartJsonHeaders() {}

            @Override
            public Class<TestRequestBody> getRequestClass() {
                return TestRequestBody.class;
            }

            @Override
            public String getTargetRestEndpointURL() {
                return "/test/upload/json";
            }

            @Override
            public boolean acceptsFileUploads() {
                return false;
            }
        }
    }

    /**
     * Handler that accepts a file request and calls {@link
     * MultipartUploadExtension#fileUploadVerifier} to verify it.
     */
    public class MultipartFileHandler
            extends AbstractRestHandler<
                    RestfulGateway, EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters> {

        MultipartFileHandler(GatewayRetriever<RestfulGateway> leaderRetriever) {
            super(
                    leaderRetriever,
                    RpcUtils.INF_TIMEOUT,
                    Collections.emptyMap(),
                    MultipartFileHeaders.INSTANCE);
        }

        @Override
        protected CompletableFuture<EmptyResponseBody> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
                throws RestHandlerException {
            MultipartUploadExtension.this.fileUploadVerifier.accept(request, gateway);
            return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
        }
    }

    private static class MultipartFileHeaders extends TestHeadersBase<EmptyRequestBody> {
        static final MultipartFileHeaders INSTANCE = new MultipartFileHeaders();

        private MultipartFileHeaders() {}

        @Override
        public Class<EmptyRequestBody> getRequestClass() {
            return EmptyRequestBody.class;
        }

        @Override
        public String getTargetRestEndpointURL() {
            return "/test/upload/file";
        }

        @Override
        public boolean acceptsFileUploads() {
            return true;
        }
    }

    private abstract static class TestHeadersBase<R extends RequestBody>
            implements RuntimeMessageHeaders<R, EmptyResponseBody, EmptyMessageParameters> {

        @Override
        public Class<EmptyResponseBody> getResponseClass() {
            return EmptyResponseBody.class;
        }

        @Override
        public HttpResponseStatus getResponseStatusCode() {
            return HttpResponseStatus.OK;
        }

        @Override
        public String getDescription() {
            return "";
        }

        @Override
        public EmptyMessageParameters getUnresolvedMessageParameters() {
            return EmptyMessageParameters.getInstance();
        }

        @Override
        public HttpMethodWrapper getHttpMethod() {
            return HttpMethodWrapper.POST;
        }
    }

    /** Simple test {@link RequestBody}. */
    protected static final class TestRequestBody implements RequestBody {
        private static final String FIELD_NAME_INDEX = "index";

        @JsonProperty(FIELD_NAME_INDEX)
        private final int index;

        TestRequestBody() {
            // magic value that reliably reproduced https://github.com/netty/netty/issues/11668
            this(-766974635);
        }

        @JsonCreator
        TestRequestBody(@JsonProperty(FIELD_NAME_INDEX) int index) {
            this.index = index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestRequestBody that = (TestRequestBody) o;
            return index == that.index;
        }

        @Override
        public int hashCode() {
            return Objects.hash(index);
        }

        @Override
        public String toString() {
            return "TestRequestBody{" + "index=" + index + '}';
        }
    }
}
