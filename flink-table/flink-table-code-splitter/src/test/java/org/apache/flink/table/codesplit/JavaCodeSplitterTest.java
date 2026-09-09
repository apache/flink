/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.codesplit;

import org.apache.flink.util.FileUtils;

import org.codehaus.janino.SimpleCompiler;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.lang.reflect.Method;

import static org.apache.flink.table.codesplit.CodeSplitTestUtil.trimLines;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link JavaCodeSplitter}. */
class JavaCodeSplitterTest {

    @Test
    void testSplitJavaCode() {
        runTest("TestSplitJavaCode", 100, 3);
    }

    @Test
    void testNotSplitJavaCode() {
        runTest("TestNotSplitJavaCode", 4000, 10000);
    }

    @ParameterizedTest
    @CsvSource({"break, 3, 2", "continue, 5, 4"})
    void testSplitLargeIfBlockContainingLoopWithJump(
            String jump, int expectedIterations, int expectedCompletedIterations) throws Exception {
        String code =
                "public class TestSplitWithJumps {\n"
                        + "  public void myFun(int[] a) {\n"
                        + "    if (a[0] >= 0) {\n"
                        // The unsplit branch exceeds the JVM's 64 KB method limit.
                        + "      a[1] += 1;\n".repeat(15000)
                        + "      for (int i = 0; i < 5; i++) {\n"
                        + "        a[2]++;\n"
                        + "        if (i == 2) { "
                        + jump
                        + "; }\n"
                        + "        a[3]++;\n"
                        + "      }\n"
                        + "    }\n"
                        + "    a[4] = 1;\n"
                        + "  }\n"
                        + "}\n";

        Object instance = compileSplitCode(code, 4000);
        Method method = instance.getClass().getMethod("myFun", int[].class);

        int[] takenBranch = {0, 0, 0, 0, 0};
        method.invoke(instance, takenBranch);
        assertThat(takenBranch)
                .containsExactly(0, 15000, expectedIterations, expectedCompletedIterations, 1);

        int[] skippedBranch = {-1, 0, 0, 0, 0};
        method.invoke(instance, skippedBranch);
        assertThat(skippedBranch).containsExactly(-1, 0, 0, 0, 1);
    }

    @Test
    void testSplitIfBlockPreservesLabeledJumps() throws Exception {
        String code =
                "public class TestSplitWithJumps {\n"
                        + "  public void myFun(int[] a) {\n"
                        + "    if (a[0] >= 0) {\n"
                        + "      a[1] = 3;\n"
                        + "      outer: for (int i = 0; i < 5; i++) {\n"
                        + "        for (int j = 0; j < 5; j++) {\n"
                        + "          a[1]++;\n"
                        + "          if (i == 1) { continue outer; }\n"
                        + "          if (i == 3) { break outer; }\n"
                        + "        }\n"
                        + "      }\n"
                        + "      a[2] = 17;\n"
                        + "    }\n"
                        + "    a[3] = 7;\n"
                        + "  }\n"
                        + "}\n";

        Object instance = compileSplitCode(code, 60);
        int[] values = {0, 0, 0, 0};
        instance.getClass().getMethod("myFun", int[].class).invoke(instance, values);
        assertThat(values).containsExactly(0, 15, 17, 7);
    }

    @Test
    void testSplitIfBlockPreservesEarlyReturn() throws Exception {
        String code =
                "public class TestSplitWithJumps {\n"
                        + "  public void myFun(int[] a) {\n"
                        + "    if (a[0] == 0) {\n"
                        + "      a[1] = 3;\n"
                        + "      a[2] = 4;\n"
                        + "    } else if (a[0] == 1) {\n"
                        + "      a[1] = 7;\n"
                        + "      return;\n"
                        + "    } else {\n"
                        + "      for (int i = 0; i < 5; i++) {\n"
                        + "        if (i == 2) { break; }\n"
                        + "        a[1] += i;\n"
                        + "      }\n"
                        + "      a[2] = 17;\n"
                        + "    }\n"
                        + "    a[3] = 7;\n"
                        + "  }\n"
                        + "}\n";

        Object instance = compileSplitCode(code, 60);
        Method method = instance.getClass().getMethod("myFun", int[].class);

        int[] returned = {1, 0, 0, 0};
        method.invoke(instance, returned);
        assertThat(returned).containsExactly(1, 7, 0, 0);

        int[] completed = {2, 0, 0, 0};
        method.invoke(instance, completed);
        assertThat(completed).containsExactly(2, 1, 17, 7);
    }

    @Test
    @Disabled("Disabled in because of https://issues.apache.org/jira/browse/FLINK-27702")
    void testInvalidJavaCode() {
        assertThatThrownBy(
                        () ->
                                JavaCodeSplitter.split(
                                        "public class InvalidClass { return 1; }", 4000, 10000))
                .hasMessageContaining(
                        "JavaCodeSplitter failed. This is a bug. Please file an issue.");
    }

    @Test
    public void testNullCode() {
        assertThatThrownBy(() -> JavaCodeSplitter.split(null, 4000, 10000))
                .cause()
                .hasMessage("code cannot be empty");
    }

    @Test
    public void testEmptyCode() {
        assertThatThrownBy(() -> JavaCodeSplitter.split("", 4000, 10000))
                .cause()
                .hasMessage("code cannot be empty");
    }

    @Test
    public void testWrongMaxMethodLength() {
        assertThatThrownBy(
                        () ->
                                JavaCodeSplitter.split(
                                        "public interface DummyInterface {}", 0, 10000))
                .cause()
                .hasMessage("maxMethodLength must be greater than 0");
    }

    @Test
    public void testWrongMaxClassMemberCount() {
        assertThatThrownBy(
                        () -> JavaCodeSplitter.split("public interface DummyInterface {}", 10, 0))
                .cause()
                .hasMessage("maxClassMemberCount must be greater than 0");
    }

    /**
     * Check whether the given and expected classes are actually a valid Java code -> it compiles.
     * If this test fails on "expected" files, it probably means that code split logic is invalid
     * and an issue was missed when preparing test files.
     */
    @Test
    void shouldCompileGivenAndExpectedCode() throws Exception {
        CodeSplitTestUtil.tryCompile("splitter/code/");
        CodeSplitTestUtil.tryCompile("splitter/expected/");
    }

    private Object compileSplitCode(String code, int maxLength) throws Exception {
        try {
            SimpleCompiler compiler = new SimpleCompiler();
            compiler.cook(JavaCodeSplitter.split(code, maxLength, 10000));
            return compiler.getClassLoader()
                    .loadClass("TestSplitWithJumps")
                    .getDeclaredConstructor()
                    .newInstance();
        } finally {
            CodeSplitUtil.getCounter().set(0L);
        }
    }

    private void runTest(String filename, int maxLength, int maxMembers) {
        try {
            String code =
                    FileUtils.readFileUtf8(
                            new File(
                                    JavaCodeSplitterTest.class
                                            .getClassLoader()
                                            .getResource("splitter/code/" + filename + ".java")
                                            .toURI()));
            String expected =
                    FileUtils.readFileUtf8(
                            new File(
                                    JavaCodeSplitterTest.class
                                            .getClassLoader()
                                            .getResource("splitter/expected/" + filename + ".java")
                                            .toURI()));

            // Trying to mitigate any indentation issues between all sort of platforms by simply
            // trim every line of the "class". Before this change, code-splitter test could fail on
            // Windows machines while passing on Unix.
            expected = trimLines(expected);
            String actual = JavaCodeSplitter.split(code, maxLength, maxMembers);

            assertThat(trimLines(actual)).isEqualTo(expected);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            // we reset the counter to ensure the variable names after rewrite are as expected
            CodeSplitUtil.getCounter().set(0L);
        }
    }
}
