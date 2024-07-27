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

package org.apache.flink.runtime.util;

import org.apache.flink.test.util.TestProcessBuilder;
import org.apache.flink.test.util.TestProcessBuilder.TestProcess;
import org.apache.flink.testutils.ClassLoaderUtils;
import org.apache.flink.testutils.ClassLoaderUtils.ClassLoaderBuilder;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ExceptionUtils} which require to spawn JVM process and set JVM memory args. */
class ExceptionUtilsITCase {
    private static final int DIRECT_MEMORY_SIZE = 10 * 1024; // 10Kb
    private static final int DIRECT_MEMORY_ALLOCATION_PAGE_SIZE = 1024; // 1Kb
    private static final int DIRECT_MEMORY_PAGE_NUMBER =
            DIRECT_MEMORY_SIZE / DIRECT_MEMORY_ALLOCATION_PAGE_SIZE;
    private static final long INITIAL_BIG_METASPACE_SIZE = 128 * (1 << 20); // 128Mb

    @Test
    void testIsDirectOutOfMemoryError() throws IOException, InterruptedException {
        String className = DummyDirectAllocatingProgram.class.getName();
        RunResult result = run(className, DIRECT_MEMORY_SIZE, -1);
        assertThat(result.getStandardOut()).isEmpty();
        assertThat(result.getErrorOut()).isEmpty();
    }

    @Test
    void testIsMetaspaceOutOfMemoryError(@TempDir File temporaryFolderForCompiledClasses)
            throws IOException, InterruptedException {
        final int classCount = 10;

        // compile the classes first
        final String sourcePattern =
                "public class %s { @Override public String toString() { return \"dummy\"; } }";
        final ClassLoaderBuilder classLoaderBuilder =
                ClassLoaderUtils.withRoot(temporaryFolderForCompiledClasses);
        for (int i = 0; i < classCount; i++) {
            final String dummyClassName = "DummyClass" + i;
            final String source = String.format(sourcePattern, dummyClassName);
            classLoaderBuilder.addClass(dummyClassName, source);
        }
        classLoaderBuilder.generateSourcesAndCompile();

        // load only one class and record required Metaspace
        final String className = DummyClassLoadingProgram.class.getName();
        final RunResult initialRun =
                run(
                        className,
                        -1,
                        INITIAL_BIG_METASPACE_SIZE,
                        1,
                        temporaryFolderForCompiledClasses.getAbsolutePath());

        // multiply the Metaspace size to stabilize the test - relying solely on the Metaspace size
        // of the initial run might cause OOMs to appear in the main thread (due to JVM-specific
        // artifacts being loaded)
        long okMetaspace = 3 * Long.parseLong(initialRun.getStandardOut());
        assertThat(initialRun.getErrorOut()).as("No error is expected.").isEmpty();

        // load more classes to cause 'OutOfMemoryError: Metaspace'
        final RunResult outOfMemoryErrorRun =
                run(
                        className,
                        -1,
                        okMetaspace,
                        classCount,
                        temporaryFolderForCompiledClasses.getAbsolutePath());
        assertThat(outOfMemoryErrorRun.getStandardOut())
                .as("OutOfMemoryError: Metaspace errors are caught and don't generate any output.")
                .isEmpty();
        assertThat(outOfMemoryErrorRun.getErrorOut())
                .as("Nothing should have been printed to stderr.")
                .isEmpty();
    }

    private static RunResult run(
            String className,
            long directMemorySize,
            long metaspaceSize,
            Object... mainClassParameters)
            throws InterruptedException, IOException {
        TestProcessBuilder taskManagerProcessBuilder = new TestProcessBuilder(className);
        if (directMemorySize > 0) {
            taskManagerProcessBuilder.addJvmArg(
                    String.format("-XX:MaxDirectMemorySize=%d", directMemorySize));
        }
        if (metaspaceSize > 0) {
            taskManagerProcessBuilder.addJvmArg("-XX:-UseCompressedOops");
            taskManagerProcessBuilder.addJvmArg(
                    String.format("-XX:MaxMetaspaceSize=%d", metaspaceSize));
        }

        for (Object parameterValue : mainClassParameters) {
            taskManagerProcessBuilder.addMainClassArg(String.valueOf(parameterValue));
        }

        // JAVA_TOOL_OPTIONS is configured on CI which would affect the process output
        taskManagerProcessBuilder.withCleanEnvironment();
        TestProcess p = taskManagerProcessBuilder.start();
        p.getProcess().waitFor();
        return new RunResult(
                p.getErrorOutput().toString().trim(), p.getProcessOutput().toString().trim());
    }

    private static final class RunResult {
        private final String errorOut;
        private final String standardOut;

        public RunResult(String errorOut, String standardOut) {
            this.errorOut = errorOut;
            this.standardOut = standardOut;
        }

        public String getErrorOut() {
            return errorOut;
        }

        public String getStandardOut() {
            return standardOut;
        }
    }

    /** Dummy java program to generate Direct OOM. */
    public static class DummyDirectAllocatingProgram {
        private DummyDirectAllocatingProgram() {}

        public static void main(String[] args) {
            try {
                Collection<ByteBuffer> buffers = new ArrayList<>();
                for (int page = 0; page < 2 * DIRECT_MEMORY_PAGE_NUMBER; page++) {
                    buffers.add(ByteBuffer.allocateDirect(DIRECT_MEMORY_ALLOCATION_PAGE_SIZE));
                }
                output("buffers: " + buffers);
            } catch (Throwable t) {
                if (!ExceptionUtils.isDirectOutOfMemoryError(t)) {
                    output("Wrong exception: " + t);
                }
            }
        }
    }

    /**
     * Dummy java program to generate Metaspace OOM. The program will catch Metaspace out of memory
     * errors and produce no output in this case.
     */
    public static class DummyClassLoadingProgram {
        private DummyClassLoadingProgram() {}

        public static void main(String[] args) {
            // trigger needed classes loaded
            output("");
            ExceptionUtils.isMetaspaceOutOfMemoryError(new Exception());

            Collection<Class<?>> classes = new ArrayList<>();
            int numberOfLoadedClasses = Integer.parseInt(args[0]);
            try {
                for (int index = 0; index < numberOfLoadedClasses; index++) {
                    classes.add(loadDummyClass(index, args[1]));
                }

                if (classes.size() == 1) {
                    output(String.valueOf(getMetaspaceUsage()));
                }
            } catch (Throwable t) {
                if (ExceptionUtils.isMetaspaceOutOfMemoryError(t)) {
                    return;
                }
                output("Wrong exception: " + t);
            }
        }

        private static Class<?> loadDummyClass(int index, String folderToSaveSource)
                throws ClassNotFoundException, IOException {
            final String className = "DummyClass" + index;
            final ClassLoader classLoader =
                    ClassLoaderUtils.withRoot(new File(folderToSaveSource))
                            .addClass(className)
                            .buildWithoutCompilation();
            return Class.forName(className, true, classLoader);
        }

        private static long getMetaspaceUsage() {
            for (MemoryPoolMXBean memoryMXBean : ManagementFactory.getMemoryPoolMXBeans()) {
                if ("Metaspace".equals(memoryMXBean.getName())) {
                    return memoryMXBean.getUsage().getUsed();
                }
            }
            throw new RuntimeException("Metaspace usage is not found");
        }
    }

    private static void output(String text) {
        System.out.println(text);
    }
}
