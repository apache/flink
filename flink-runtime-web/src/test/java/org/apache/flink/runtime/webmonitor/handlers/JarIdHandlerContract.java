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

import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assumptions.assumeThatCode;

/**
 * Tests that every handler taking a jar ID must pass. The jar ID is given to the handler without
 * going through {@link JarIdPathParameter}, so the handler's own checks are tested.
 */
interface JarIdHandlerContract {

    /**
     * Handles a request for {@code jarId} with a handler whose upload directory is {@code jarDir},
     * and waits for the result.
     */
    void handleJarIdRequest(Path jarDir, String jarId) throws Exception;

    /** Asserts that the handler did its job for {@code jar}, e.g. submitted or deleted it. */
    void assertJarHandled(Path jar) throws Exception;

    /** A jar that the handler can handle successfully. */
    Path programJar();

    @ParameterizedTest(name = "jarId={0}")
    @MethodSource(
            "org.apache.flink.runtime.webmonitor.handlers.JarIdHandlerContract#jarIdsThatAreNotFileNames")
    default void testJarIdThatDoesNotNameAJarIsRejected(
            final String jarId, @TempDir final Path tempDir) throws Exception {
        // empty, so that deleting the upload directory itself would succeed
        final Path jarDir = Files.createDirectory(tempDir.resolve("jars"));

        assertBadRequest(() -> handleJarIdRequest(jarDir, jarId));

        assertThat(jarDir).isDirectory();
    }

    @Test
    default void testJarIdOfJarInSubdirectoryIsRejected(@TempDir final Path tempDir)
            throws Exception {
        final Path jarDir = Files.createDirectory(tempDir.resolve("jars"));
        final Path nestedJar =
                Files.copy(
                        programJar(),
                        Files.createDirectory(jarDir.resolve("subdir")).resolve("nested.jar"));

        assertBadRequest(() -> handleJarIdRequest(jarDir, "subdir/nested.jar"));

        assertThat(nestedJar).isRegularFile();
    }

    @Test
    default void testJarIdOfJarOutsideJarDirIsRejected(@TempDir final Path tempDir)
            throws Exception {
        final Path jarDir = Files.createDirectory(tempDir.resolve("jars"));
        final Path outsideJar = Files.copy(programJar(), tempDir.resolve("outside.jar"));

        assertBadRequest(() -> handleJarIdRequest(jarDir, outsideJar.toString()));

        assertThat(outsideJar).isRegularFile();
    }

    @Test
    default void testJarIdOfDirectoryIsRejected(@TempDir final Path tempDir) throws Exception {
        final Path jarDir = Files.createDirectory(tempDir.resolve("jars"));
        final Path directory = Files.createDirectory(jarDir.resolve("directory.jar"));

        assertBadRequest(() -> handleJarIdRequest(jarDir, "directory.jar"));

        assertThat(directory).isDirectory();
    }

    @ParameterizedTest(name = "jarDir={0}")
    @EnumSource(JarDirForm.class)
    default void testUploadedJarIdIsAcceptedWithJarDirGivenAs(
            final JarDirForm form, @TempDir final Path tempDir) throws Exception {
        final Path jarDir = Files.createDirectory(tempDir.resolve("jars"));
        final Path jar = Files.copy(programJar(), jarDir.resolve("program.jar"));

        handleJarIdRequest(form.of(jarDir), jar.getFileName().toString());

        assertJarHandled(jar);
    }

    static Stream<Named<String>> jarIdsThatAreNotFileNames() {
        return Stream.of(
                Named.of("..", ".."),
                Named.of(".", "."),
                Named.of("<empty>", ""),
                Named.of("/", "/"));
    }

    /** Ways to give a handler the jar directory other than its plain absolute path. */
    enum JarDirForm {
        UNNORMALIZED {
            @Override
            Path of(final Path jarDir) throws IOException {
                final Path detour = Files.createDirectories(jarDir.resolveSibling("detour"));
                return detour.resolve("..").resolve(jarDir.getFileName());
            }
        },
        RELATIVE {
            @Override
            Path of(final Path jarDir) {
                return Paths.get(".").resolve(Paths.get("").toAbsolutePath().relativize(jarDir));
            }
        },
        SYMLINK {
            @Override
            Path of(final Path jarDir) {
                final Path link = jarDir.resolveSibling("link");
                assumeThatCode(() -> Files.createSymbolicLink(link, jarDir))
                        .doesNotThrowAnyException();
                return link;
            }
        },
        SYMLINK_THEN_PARENT {
            @Override
            Path of(final Path jarDir) throws IOException {
                final Path detour = Files.createDirectories(jarDir.resolveSibling("detour"));
                final Path link =
                        Files.createDirectories(jarDir.resolveSibling("elsewhere")).resolve("link");
                assumeThatCode(() -> Files.createSymbolicLink(link, detour))
                        .doesNotThrowAnyException();
                return link.resolve("..").resolve(jarDir.getFileName());
            }
        };

        /** Returns another path to {@code jarDir} in this form. */
        abstract Path of(Path jarDir) throws IOException;
    }

    static void assertBadRequest(final ThrowingCallable call) {
        final Throwable thrown = catchThrowable(call);

        assertThat(ExceptionUtils.findThrowable(thrown, RestHandlerException.class))
                .as("RestHandlerException in %s", thrown)
                .hasValueSatisfying(
                        e -> {
                            assertThat(e).hasMessageContaining("jar not found");
                            assertThat(e.getHttpResponseStatus())
                                    .isEqualTo(HttpResponseStatus.BAD_REQUEST);
                        });
    }
}
