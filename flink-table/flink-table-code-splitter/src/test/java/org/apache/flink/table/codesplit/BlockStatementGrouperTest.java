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

import org.apache.flink.table.codesplit.BlockStatementGrouper.RewriteGroupedCode;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.flink.table.codesplit.CodeSplitTestUtil.readResource;
import static org.apache.flink.table.codesplit.CodeSplitTestUtil.trimLines;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BlockStatementGrouper}. */
class BlockStatementGrouperTest {

    @Test
    public void testExtractIfInWhileGroups() {
        String parameters = "a, b";
        String givenBlock = readResource("groups/code/IfInWhile.txt");
        String expectedBlock = readResource("groups/expected/IfInWhile.txt");

        BlockStatementGrouper grouper = new BlockStatementGrouper(givenBlock, 10, parameters);

        RewriteGroupedCode rewriteGroupedCode = grouper.rewrite("myFun");

        String rewriteCode = rewriteGroupedCode.getRewriteCode();
        Map<String, List<String>> groups = rewriteGroupedCode.getGroups();

        // Trying to mitigate any indentation issues between all sort of platforms by simply
        // trim every line of the "class". Before this change, code-splitter test could fail on
        // Windows machines while passing on Unix.
        assertThat(trimLines(rewriteCode)).isEqualTo(trimLines(expectedBlock));

        assertThat(groups).hasSize(4);
        List<String> group1 = groups.get("myFun_rewriteGroup0_1_rewriteGroup3");
        assertThat(group1).hasSize(2);
        assertThat(group1.get(0)).isEqualTo("myFun_whileBody0_0(a, b);");
        assertThat(trimLines(group1.get(1)))
                .isEqualTo(
                        trimLines(
                                ""
                                        + " if (a[0] > 0) {\n"
                                        + "     myFun_whileBody0_0_ifBody0(a, b);\n"
                                        + " } else {\n"
                                        + "     myFun_whileBody0_0_ifBody1(a, b);\n"
                                        + " }"));

        List<String> group2 = groups.get("myFun_rewriteGroup0_1_rewriteGroup5");
        assertThat(group2).hasSize(3);
        assertThat(group2.get(0)).isEqualTo("a[2] += b[2];");
        assertThat(group2.get(1)).isEqualTo("b[3] += a[3];");
        assertThat(trimLines(group2.get(2)))
                .isEqualTo(
                        trimLines(
                                "if (a[0] > 0) {\n"
                                        + "            System.out.println(\"Hello\");\n"
                                        + "        } else {\n"
                                        + "            System.out.println(\"World\");\n"
                                        + "        }"));

        List<String> group3 = groups.get("myFun_rewriteGroup6");
        assertThat(group3).hasSize(3);
        assertThat(group3.get(0)).isEqualTo("a[0] += b[1];");
        assertThat(group3.get(1)).isEqualTo("b[1] += a[1];");
        assertThat(trimLines(group3.get(2)))
                .isEqualTo(
                        trimLines(
                                " while (counter > 0) {\n"
                                        + "    myFun_rewriteGroup0_1_rewriteGroup3(a, b);\n"
                                        + "    \n"
                                        + "    myFun_rewriteGroup0_1_rewriteGroup5(a, b);\n"
                                        + "    \n"
                                        + "    counter--;\n"
                                        + "}"));

        List<String> group4 = groups.get("myFun_rewriteGroup7");
        assertThat(group4).containsExactly("a[4] += b[4];", "b[5] += a[5];");
    }

    @Test
    public void testExtractWhileInIfGroups() {
        String parameters = "a, b";
        String givenBlock = readResource("groups/code/WhileInIf.txt");
        String expectedBlock = readResource("groups/expected/WhileInIf.txt");

        BlockStatementGrouper grouper = new BlockStatementGrouper(givenBlock, 10, parameters);

        RewriteGroupedCode rewriteGroupedCode = grouper.rewrite("myFun");

        String rewriteCode = rewriteGroupedCode.getRewriteCode();
        Map<String, List<String>> groups = rewriteGroupedCode.getGroups();

        // Trying to mitigate any indentation issues between all sort of platforms by simply
        // trim every line of the "class". Before this change, code-splitter test could fail on
        // Windows machines while passing on Unix.
        assertThat(trimLines(rewriteCode)).isEqualTo(trimLines(expectedBlock));

        assertThat(groups).hasSize(5);
        List<String> group1 = groups.get("myFun_rewriteGroup0_1_rewriteGroup2_3_rewriteGroup5");
        assertThat(group1).hasSize(2);
        assertThat(group1.get(0)).isEqualTo("myFun_whileBody0_0(a, b);");
        assertThat(trimLines(group1.get(1)))
                .isEqualTo(
                        trimLines(
                                ""
                                        + " if (a[0] > 0) {\n"
                                        + "     myFun_whileBody0_0_ifBody0(a, b);\n"
                                        + " } else {\n"
                                        + "     myFun_whileBody0_0_ifBody1(a, b);\n"
                                        + " }"));

        List<String> group2 = groups.get("myFun_rewriteGroup0_1_rewriteGroup6");
        assertThat(group2).hasSize(1);
        assertThat(trimLines(group2.get(0)))
                .isEqualTo(
                        trimLines(
                                ""
                                        + "while (counter > 0) {\n"
                                        + "    myFun_rewriteGroup0_1_rewriteGroup2_3_rewriteGroup5(a, b);\n"
                                        + "    counter--;\n"
                                        + "}"));

        List<String> group3 = groups.get("myFun_rewriteGroup0_1_rewriteGroup7");
        assertThat(group3).containsExactly("a[2] += b[2];", "b[3] += a[3];");

        List<String> group4 = groups.get("myFun_rewriteGroup8");
        assertThat(group4).hasSize(3);
        assertThat(group4.get(0)).isEqualTo("a[0] += b[1];");
        assertThat(group4.get(1)).isEqualTo("b[1] += a[1];");
        assertThat(trimLines(group4.get(2)))
                .isEqualTo(
                        trimLines(
                                "if (a.length < 100) {\n"
                                        + "      myFun_rewriteGroup0_1_rewriteGroup6(a, b);\n"
                                        + "     \n"
                                        + "      myFun_rewriteGroup0_1_rewriteGroup7(a, b);\n"
                                        + "    } else {\n"
                                        + "      while (counter < 100) {\n"
                                        + "          b[4] = b[4]++;\n"
                                        + "          counter++;\n"
                                        + "      }\n"
                                        + "}"));

        List<String> group5 = groups.get("myFun_rewriteGroup9");
        assertThat(group5).containsExactly("a[5] += b[5];", "b[6] += a[6];");
    }
}
