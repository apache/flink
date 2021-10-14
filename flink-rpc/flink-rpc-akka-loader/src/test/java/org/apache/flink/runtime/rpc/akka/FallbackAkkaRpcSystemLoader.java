/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.classloading.SubmoduleClassLoader;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcSystemLoader;
import org.apache.flink.util.OperatingSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Fallback {@link RpcSystemLoader} that does not rely on the flink-rpc-akka fat jar (like {@link
 * AkkaRpcSystemLoader}) but instead uses the flink-rpc-akka/target/classes and maven to load the
 * rpc system.
 */
public class FallbackAkkaRpcSystemLoader implements RpcSystemLoader {
    private static final Logger LOG = LoggerFactory.getLogger(FallbackAkkaRpcSystemLoader.class);

    @Override
    public RpcSystem loadRpcSystem(Configuration config) {
        try {
            LOG.debug(
                    "Using Fallback AkkaRpcSystemLoader; this loader will invoke maven to retrieve the dependencies of flink-rpc-akka.");

            final ClassLoader flinkClassLoader = RpcSystem.class.getClassLoader();

            // flink-rpc/flink-rpc-akka
            final Path akkaRpcModuleDirectory =
                    findAkkaRpcModuleDirectory(getCurrentWorkingDirectory());

            // flink-rpc/flink-rpc-akka/target/classes
            final Path akkaRpcModuleClassesDirectory =
                    akkaRpcModuleDirectory.resolve(Paths.get("target", "classes"));

            // flink-rpc/flink-rpc-akka/target/dependencies
            final Path akkaRpcModuleDependenciesDirectory =
                    akkaRpcModuleDirectory.resolve(Paths.get("target", "dependencies"));

            if (!Files.exists(akkaRpcModuleDependenciesDirectory)) {
                downloadDependencies(akkaRpcModuleDirectory, akkaRpcModuleDependenciesDirectory);
            } else {
                LOG.debug(
                        "Re-using previously downloaded flink-rpc-akka dependencies. If you are experiencing strange issues, try clearing '{}'.",
                        akkaRpcModuleDependenciesDirectory);
            }

            // assemble URL collection containing target/classes and each jar
            final List<URL> urls = new ArrayList<>();
            urls.add(akkaRpcModuleClassesDirectory.toUri().toURL());
            try (final Stream<Path> files = Files.list(akkaRpcModuleDependenciesDirectory)) {
                final List<Path> collect =
                        files.filter(path -> path.getFileName().toString().endsWith(".jar"))
                                .collect(Collectors.toList());

                for (Path path : collect) {
                    urls.add(path.toUri().toURL());
                }
            }

            final SubmoduleClassLoader submoduleClassLoader =
                    new SubmoduleClassLoader(urls.toArray(new URL[0]), flinkClassLoader);

            return new CleanupOnCloseRpcSystem(
                    ServiceLoader.load(RpcSystem.class, submoduleClassLoader).iterator().next(),
                    submoduleClassLoader,
                    null);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not initialize RPC system. Run 'mvn package -pl flink-rpc/flink-rpc-akka,flink-rpc/flink-rpc-akka-loader' on the command-line.",
                    e);
        }
    }

    private static Path getCurrentWorkingDirectory() {
        return Paths.get("").toAbsolutePath();
    }

    private static Path findAkkaRpcModuleDirectory(Path currentParentCandidate) throws IOException {
        try (Stream<Path> directoryContents = Files.list(currentParentCandidate)) {
            final Optional<Path> flinkRpcModuleDirectory =
                    directoryContents
                            .filter(path -> path.getFileName().toString().equals("flink-rpc"))
                            .findFirst();
            if (flinkRpcModuleDirectory.isPresent()) {
                return flinkRpcModuleDirectory
                        .map(path -> path.resolve(Paths.get("flink-rpc-akka")))
                        .get();
            }
        }
        return findAkkaRpcModuleDirectory(currentParentCandidate.getParent());
    }

    private static void downloadDependencies(Path workingDirectory, Path targetDirectory)
            throws IOException, InterruptedException {

        final String mvnExecutable = OperatingSystem.isWindows() ? "mvn.bat" : "mvn";

        final ProcessBuilder mvn =
                new ProcessBuilder()
                        .directory(workingDirectory.toFile())
                        .command(
                                mvnExecutable,
                                "dependency:copy-dependencies",
                                "-DincludeScope=runtime", // excludes provided/test dependencies
                                "-DoutputDirectory=" + targetDirectory)
                        .redirectOutput(ProcessBuilder.Redirect.INHERIT);
        mvn.start().waitFor();
    }
}
