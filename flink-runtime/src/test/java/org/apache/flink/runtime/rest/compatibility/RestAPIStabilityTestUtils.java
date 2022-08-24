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

package org.apache.flink.runtime.rest.compatibility;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.rest.util.DocumentingRestEndpoint;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.util.DefaultIndenter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** The util of stability test and snapshot generator for the REST API. */
public final class RestAPIStabilityTestUtils {

    private static final ObjectMapper OBJECT_MAPPER = JacksonMapperFactory.createObjectMapper();

    public static void testStability(
            String snapshotFileName,
            String snapshotRegenerateProperty,
            RestAPIVersion apiVersion,
            DocumentingRestEndpoint documentingRestEndpoint)
            throws IOException {
        final String versionedSnapshotFileName =
                String.format(snapshotFileName, apiVersion.getURLVersionPrefix());

        final RestAPISnapshot currentSnapshot = createSnapshot(documentingRestEndpoint, apiVersion);

        if (System.getProperty(snapshotRegenerateProperty) != null) {
            writeSnapshot(versionedSnapshotFileName, currentSnapshot);
        }

        final URL resource =
                RestAPIStabilityTestUtils.class
                        .getClassLoader()
                        .getResource(versionedSnapshotFileName);
        if (resource == null) {
            Assertions.fail(
                    "Snapshot file does not exist. If you added a new version, re-run this test with"
                            + " -D"
                            + snapshotFileName
                            + " being set.");
        }
        final RestAPISnapshot previousSnapshot =
                OBJECT_MAPPER.readValue(resource, RestAPISnapshot.class);

        assertCompatible(snapshotRegenerateProperty, previousSnapshot, currentSnapshot);
    }

    private static void writeSnapshot(
            final String versionedSnapshotFileName, final RestAPISnapshot snapshot)
            throws IOException {
        OBJECT_MAPPER
                .writer(
                        new DefaultPrettyPrinter()
                                .withObjectIndenter(new DefaultIndenter().withLinefeed("\n")))
                .writeValue(new File("src/test/resources/" + versionedSnapshotFileName), snapshot);
        System.out.println(
                "REST API snapshot "
                        + versionedSnapshotFileName
                        + " was updated, please remember to commit the snapshot.");
    }

    private static RestAPISnapshot createSnapshot(
            final DocumentingRestEndpoint restEndpoint, RestAPIVersion apiVersion) {
        final List<JsonNode> calls =
                restEndpoint.getSpecs().stream()
                        // we only compare compatibility within the given version
                        .filter(spec -> spec.getSupportedAPIVersions().contains(apiVersion))
                        .map(
                                spec -> {
                                    final ObjectNode json = OBJECT_MAPPER.createObjectNode();

                                    for (final CompatibilityRoutine<?> routine :
                                            CompatibilityRoutines.ROUTINES) {
                                        final Object extract = routine.getContainer(spec);
                                        json.set(
                                                routine.getKey(),
                                                OBJECT_MAPPER.valueToTree(extract));
                                    }

                                    return json;
                                })
                        .collect(Collectors.toList());

        return new RestAPISnapshot(calls);
    }

    private static void assertCompatible(
            final String snapshotRegenerateProperty,
            final RestAPISnapshot old,
            final RestAPISnapshot cur) {
        for (final JsonNode oldCall : old.calls) {
            final List<Tuple2<JsonNode, CompatibilityCheckResult>> compatibilityCheckResults =
                    cur.calls.stream()
                            .map(
                                    curCall ->
                                            Tuple2.of(
                                                    curCall, checkCompatibility(oldCall, curCall)))
                            .collect(Collectors.toList());

            if (compatibilityCheckResults.stream()
                    .allMatch(
                            result ->
                                    result.f1.getBackwardCompatibility()
                                            == Compatibility.INCOMPATIBLE)) {
                fail(oldCall, compatibilityCheckResults);
            }

            if (compatibilityCheckResults.stream()
                    .noneMatch(
                            result ->
                                    result.f1.getBackwardCompatibility()
                                            == Compatibility.IDENTICAL)) {
                Assertions.fail(
                        "The Rest API was modified in a compatible way, but the snapshot was not updated. "
                                + "To update the snapshot, re-run this test with -D"
                                + snapshotRegenerateProperty
                                + " being set. If you see this message in a CI pipeline, rerun the test locally and commit the generated changes.");
            }
        }

        // check for entirely new calls, for which the snapshot should be updated
        for (final JsonNode curCall : cur.calls) {
            final List<Tuple2<JsonNode, CompatibilityCheckResult>> compatibilityCheckResults =
                    old.calls.stream()
                            .map(
                                    oldCall ->
                                            Tuple2.of(
                                                    curCall, checkCompatibility(oldCall, curCall)))
                            .collect(Collectors.toList());

            if (compatibilityCheckResults.stream()
                    .noneMatch(
                            result ->
                                    result.f1.getBackwardCompatibility()
                                            == Compatibility.IDENTICAL)) {
                Assertions.fail(
                        "API was modified in a compatible way, but the snapshot was not updated. "
                                + "To update the snapshot, re-run this test with -D"
                                + snapshotRegenerateProperty
                                + " being set.");
            }
        }
    }

    private static void fail(
            final JsonNode oldCall,
            final List<Tuple2<JsonNode, CompatibilityCheckResult>> compatibilityCheckResults) {
        final StringBuilder sb = new StringBuilder();
        sb.append("No compatible call could be found for " + oldCall + '.');
        compatibilityCheckResults.stream()
                .sorted(
                        Collections.reverseOrder(
                                Comparator.comparingInt(
                                        tuple -> tuple.f1.getBackwardCompatibilityGrade())))
                .forEachOrdered(
                        result -> {
                            sb.append(System.lineSeparator());
                            sb.append("\tRejected by candidate: " + result.f0 + '.');

                            sb.append(System.lineSeparator());
                            sb.append(
                                    "\tCompatibility grade: "
                                            + result.f1.getBackwardCompatibilityGrade()
                                            + '/'
                                            + CompatibilityRoutines.ROUTINES.size());

                            sb.append(System.lineSeparator());
                            sb.append("\tIncompatibilities: ");

                            for (AssertionError error :
                                    result.f1.getBackwardCompatibilityErrors()) {
                                sb.append(System.lineSeparator());
                                sb.append("\t\t" + error.getMessage());
                            }
                        });
        Assertions.fail(sb.toString());
    }

    private static CompatibilityCheckResult checkCompatibility(
            final JsonNode oldCall, final JsonNode newCall) {
        return CompatibilityRoutines.ROUTINES.stream()
                .map(routine -> checkCompatibility(routine, oldCall, newCall))
                .reduce(CompatibilityCheckResult::merge)
                .get();
    }

    private static <X> CompatibilityCheckResult checkCompatibility(
            final CompatibilityRoutine<X> routine, final JsonNode oldCall, final JsonNode curCall) {
        final Optional<X> old = readJson(routine, oldCall);
        final Optional<X> cur = readJson(routine, curCall);

        return routine.checkCompatibility(old, cur);
    }

    private static <X> Optional<X> readJson(
            final CompatibilityRoutine<X> routine, final JsonNode call) {
        final Optional<JsonNode> jsonContainer = Optional.ofNullable(call.get(routine.getKey()));
        return jsonContainer.map(container -> jsonToObject(container, routine.getContainerClass()));
    }

    private static <X> X jsonToObject(final JsonNode jsonContainer, final Class<X> containerClass) {
        try {
            return OBJECT_MAPPER.treeToValue(jsonContainer, containerClass);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    static final class RestAPISnapshot {
        public List<JsonNode> calls;

        private RestAPISnapshot() {
            // required by jackson
        }

        RestAPISnapshot(List<JsonNode> calls) {
            this.calls = calls;
        }
    }
}
