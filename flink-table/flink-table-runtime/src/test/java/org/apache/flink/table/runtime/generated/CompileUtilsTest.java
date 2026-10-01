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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.util.FlinkRuntimeException;

import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.SimpleCompiler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CompileUtils}. */
class CompileUtilsTest {

    @BeforeEach
    void before() {
        // cleanup cached class before tests
        CompileUtils.COMPILED_CLASS_CACHE.invalidateAll();
        CompileUtils.COMPILED_EXPRESSION_CACHE.invalidateAll();
        CompileUtils.BYTECODE_CACHE.invalidateAll();
    }

    @Test
    void testSameSourceIsCompiledOnceAcrossClassLoaders() throws Exception {
        String nonce = String.valueOf(System.nanoTime());
        // probe is resolvable only via the compile classloader, so cooking must go through it
        String probe = "UserProbe$" + nonce;
        byte[] probeBytes = compileToBytes(probe, "public class " + probe + " {}");

        String name = "Repro$" + nonce;
        String code =
                "public class " + name + " { public Object e() { return new " + probe + "(); } }";

        CompileUtils.compile(new ProbeClassLoader(probe, probeBytes, null), name, code);

        int[] resolveCount = new int[1];
        ProbeClassLoader loader2 = new ProbeClassLoader(probe, probeBytes, resolveCount);
        Class<?> reused = CompileUtils.compile(loader2, name, code);

        // a second cook would resolve the probe again; reused bytecode resolves it zero times
        assertThat(resolveCount[0]).isZero();

        // using the class links the probe through loader2, proving the shared bytecode re-resolves
        Object probeInstance =
                reused.getMethod("e").invoke(reused.getDeclaredConstructor().newInstance());
        assertThat(probeInstance.getClass().getClassLoader()).isSameAs(loader2);
    }

    private static byte[] compileToBytes(String className, String source) throws Exception {
        SimpleCompiler compiler = new SimpleCompiler();
        compiler.cook(source);
        return compiler.getBytecodes().get(className);
    }

    @Test
    void testCacheReuse() {
        String code = "public class Main {\n" + "  int i;\n" + "  int j;\n" + "}";

        Class<?> class1 = CompileUtils.compile(this.getClass().getClassLoader(), "Main", code);
        Class<?> class2 = CompileUtils.compile(this.getClass().getClassLoader(), "Main", code);
        Class<?> class3 = CompileUtils.compile(new TestClassLoader(), "Main", code);
        assertThat(class2).isSameAs(class1);
        assertThat(class3).isNotSameAs(class1);
    }

    @Test
    void testExpressionCacheReuse() {
        String code = "a + b";

        ExpressionEvaluator evaluator1 =
                CompileUtils.compileExpression(
                        code,
                        Arrays.asList("a", "b"),
                        Arrays.asList(Integer.class, Integer.class),
                        Integer.class);
        ExpressionEvaluator evaluator2 =
                CompileUtils.compileExpression(
                        code,
                        Arrays.asList("a", "b"),
                        Arrays.asList(Integer.class, Integer.class),
                        Integer.class);
        ExpressionEvaluator evaluator3 =
                CompileUtils.compileExpression(
                        code,
                        Arrays.asList("a", "b"),
                        Arrays.asList(String.class, String.class),
                        String.class);
        assertThat(evaluator2).isSameAs(evaluator1);
        assertThat(evaluator3).isNotSameAs(evaluator1);
    }

    @Test
    void testWrongCode() {
        String code = "public class111 Main {\n" + "  int i;\n" + "  int j;\n" + "}";
        assertThatThrownBy(
                        () -> CompileUtils.compile(this.getClass().getClassLoader(), "Main", code))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(
                        "Table program cannot be compiled. This is a bug. Please file an issue.");
    }

    private static class TestClassLoader extends URLClassLoader {

        TestClassLoader() {
            super(new URL[0], Thread.currentThread().getContextClassLoader());
        }
    }

    /**
     * Resolves only {@code probe} (from given bytes) and optionally counts how often it is asked.
     */
    private static class ProbeClassLoader extends URLClassLoader {
        private final String probe;
        private final byte[] probeBytes;
        private final int[] resolveCount;

        ProbeClassLoader(String probe, byte[] probeBytes, int[] resolveCount) {
            super(new URL[0], Thread.currentThread().getContextClassLoader());
            this.probe = probe;
            this.probeBytes = probeBytes;
            this.resolveCount = resolveCount;
        }

        @Override
        protected Class<?> findClass(String n) throws ClassNotFoundException {
            if (n.equals(probe)) {
                if (resolveCount != null) {
                    resolveCount[0]++;
                }
                return defineClass(n, probeBytes, 0, probeBytes.length);
            }
            return super.findClass(n);
        }
    }
}
