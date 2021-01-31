/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.docs.rest;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.docs.TestUtils;
import org.apache.flink.runtime.rest.compatibility.CompatibilityRoutines;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** This test verifies that all rest docs are documented well with latest rest APIs. */
@RunWith(Parameterized.class)
public class RestOptionsDocsCompletenessITCase {
    private static final String SNAPSHOT_RESOURCE_PATTERN = "rest_api_%s.snapshot";

    @Parameterized.Parameters(name = "version = {0}")
    public static Iterable<RestAPIVersion> getStableVersions() {
        return Arrays.stream(RestAPIVersion.values())
                .filter(RestAPIVersion::isStableVersion)
                .collect(Collectors.toList());
    }

    private final RestAPIVersion apiVersion;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public RestOptionsDocsCompletenessITCase(RestAPIVersion apiVersion) {
        this.apiVersion = apiVersion;
    }

    @Test
    public void testCompleteness() throws IOException {
        final String versionedSnapshotFileName =
                String.format(SNAPSHOT_RESOURCE_PATTERN, apiVersion.getURLVersionPrefix());

        final String rootDir = TestUtils.getProjectRootDir();

        RestAPISnapshot restAPISnapshot = parseRestSnapshot(rootDir, versionedSnapshotFileName);

        Map<String, DocumentedOption> documentedOptions =
                parseDocumentedOptions(rootDir, apiVersion.getURLVersionPrefix());

        for (RestAPI restAPI : restAPISnapshot.calls) {
            // we will never check undocumented option.
            if (RestAPIDocGenerator.UNDOCUMENTED_APIS.contains(restAPI.url)) {
                continue;
            }
            DocumentedOption documentedOption =
                    documentedOptions.remove(getUniqueKey(restAPI.url, restAPI.method));
            Assert.assertNotNull(documentedOption);
            Assert.assertEquals(restAPI.method, documentedOption.method);
            Assert.assertEquals(restAPI.statusCode, documentedOption.statusCode);
            Assert.assertEquals(restAPI.methodDescription, documentedOption.methodDescription);

            Map<String, String> expectedPathParameters =
                    restAPI.pathParameters.pathParameters.stream()
                            .collect(Collectors.toMap(t -> t.key, t -> t.description));
            Map<String, String> actualPathParameters =
                    documentedOption.pathParameters.stream()
                            .collect(Collectors.toMap(t -> t.key, t -> t.description));
            Assert.assertEquals(expectedPathParameters, actualPathParameters);

            Map<String, Tuple2<Boolean, String>> expectedQueryParameters =
                    restAPI.queryParameters.queryParameters.stream()
                            .collect(
                                    Collectors.toMap(
                                            t -> t.key,
                                            t -> Tuple2.of(t.mandatory, t.description)));
            Map<String, Tuple2<Boolean, String>> actualQueryParameters =
                    documentedOption.queryParameters.stream()
                            .collect(
                                    Collectors.toMap(
                                            t -> t.key,
                                            t -> Tuple2.of(t.isMandatory, t.description)));
            Assert.assertEquals(expectedQueryParameters, actualQueryParameters);
        }

        Assert.assertTrue(documentedOptions.isEmpty());
    }

    private static final class DocumentedOption {
        private final String url;
        private final String method;
        private final String statusCode;
        private final String methodDescription;
        private final Collection<RestAPIDocGenerator.PathParameter> pathParameters;
        private final Collection<RestAPIDocGenerator.QueryParameter> queryParameters;

        private DocumentedOption(
                String url,
                String method,
                String statusCode,
                String methodDescription,
                Collection<RestAPIDocGenerator.PathParameter> pathParameters,
                Collection<RestAPIDocGenerator.QueryParameter> queryParameters) {
            this.url = url;
            this.method = method;
            this.statusCode = statusCode;
            this.methodDescription = methodDescription;
            this.pathParameters = pathParameters;
            this.queryParameters = queryParameters;
        }
    }

    private static RestAPISnapshot parseRestSnapshot(
            String rootDir, String versionedSnapshotFileName) throws IOException {
        final Path restSnapshotFolder =
                Paths.get(rootDir, "flink-runtime-web", "src", "test", "resources")
                        .toAbsolutePath();
        return OBJECT_MAPPER.readValue(
                new File(restSnapshotFolder.toFile(), versionedSnapshotFileName),
                RestAPISnapshot.class);
    }

    private static Map<String, DocumentedOption> parseDocumentedOptions(
            String rootDir, String version) throws IOException {
        String restDispatcherFileName = RestAPIDocGenerator.getRestDispatcherFileName(version);
        Path restApiHtmlFile =
                Paths.get(rootDir, "docs", "_includes", "generated", restDispatcherFileName)
                        .toAbsolutePath();
        Collection<DocumentedOption> documentedOptions =
                parseDocumentedOptionsFromFile(restApiHtmlFile);
        return documentedOptions.stream()
                .collect(Collectors.toMap(t -> getUniqueKey(t.url, t.method), t -> t));
    }

    private static String getUniqueKey(String url, String method) {
        return url + "-" + method;
    }

    private static Collection<DocumentedOption> parseDocumentedOptionsFromFile(Path file)
            throws IOException {
        Document document = Jsoup.parse(file.toFile(), StandardCharsets.UTF_8.name());
        document.outputSettings().syntax(Document.OutputSettings.Syntax.xml);
        document.outputSettings().prettyPrint(false);
        return document.getElementsByTag("table").stream()
                .map(
                        element -> {
                            Element tbody = element.getElementsByTag("tbody").get(0);
                            Elements elements = tbody.getElementsByTag("td");
                            String url = elements.get(0).wholeText();
                            String method =
                                    elements.get(1).getElementsByTag("code").get(0).wholeText();
                            String statusCode =
                                    elements.get(2).getElementsByTag("code").get(0).wholeText();
                            String methodDescription = elements.get(3).wholeText();
                            String fourthText = elements.get(4).wholeText();
                            List<RestAPIDocGenerator.PathParameter> pathParameters =
                                    new ArrayList<>();
                            List<RestAPIDocGenerator.QueryParameter> queryParameters =
                                    new ArrayList<>();
                            if (fourthText.equals(RestAPIDocGenerator.PATH_PARAMETERS)) {
                                pathParameters.addAll(
                                        parsePathParameters(
                                                elements.get(5).getElementsByTag("li")));

                                String sixthText = elements.get(6).wholeText();
                                if (sixthText.equals(RestAPIDocGenerator.QUERY_PARAMETERS)) {
                                    queryParameters.addAll(
                                            parseQueryParameters(
                                                    elements.get(7).getElementsByTag("li")));
                                }
                            } else if (fourthText.equals(RestAPIDocGenerator.QUERY_PARAMETERS)) {
                                Elements liElements = elements.get(5).getElementsByTag("li");
                                queryParameters.addAll(parseQueryParameters(liElements));
                            }
                            return new DocumentedOption(
                                    url,
                                    method,
                                    statusCode,
                                    methodDescription,
                                    pathParameters,
                                    queryParameters);
                        })
                .collect(Collectors.toList());
    }

    private static List<RestAPIDocGenerator.PathParameter> parsePathParameters(Elements elements) {
        List<RestAPIDocGenerator.PathParameter> result = new ArrayList<>();
        for (Element element : elements) {
            result.add(RestAPIDocGenerator.unformatPathParameterViaLiTagText(element.wholeText()));
        }
        return result;
    }

    private static List<RestAPIDocGenerator.QueryParameter> parseQueryParameters(
            Elements elements) {
        List<RestAPIDocGenerator.QueryParameter> result = new ArrayList<>();
        for (Element element : elements) {
            result.add(RestAPIDocGenerator.unformatQueryParameterViaLiTagText(element.wholeText()));
        }
        return result;
    }

    private static final class RestAPISnapshot {
        public List<RestAPI> calls;

        private RestAPISnapshot() {
            // required by jackson
        }

        RestAPISnapshot(List<RestAPI> restApis) {
            this.calls = restApis;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class RestAPI {
        public String url;
        public String method;

        @JsonProperty("status-code")
        public String statusCode;

        @JsonProperty("method-description")
        public String methodDescription;

        @JsonProperty("path-parameters")
        public CompatibilityRoutines.PathParameterContainer pathParameters;

        @JsonProperty("query-parameters")
        public CompatibilityRoutines.QueryParameterContainer queryParameters;

        private RestAPI() {}

        RestAPI(
                String url,
                String method,
                String statusCode,
                String methodDescription,
                CompatibilityRoutines.PathParameterContainer pathParameters,
                CompatibilityRoutines.QueryParameterContainer queryParameters) {
            this.url = url;
            this.method = method;
            this.statusCode = statusCode;
            this.methodDescription = methodDescription;
            this.pathParameters = pathParameters;
            this.queryParameters = queryParameters;
        }
    }
}
