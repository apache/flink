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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.flink.table.codesplit.CodeSplitTestUtil.trimLines;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BlockStatementSplitter}. */
class BlockStatementSplitterTest {

    private static final String GIVEN_WHILE_IF_BLOCK =
            ""
                    + " while (counter > 0) {\n"
                    + "     int localA = a + 1000;\n"
                    + "     System.out.println(localA);\n"
                    + "     if (a > 0) {\n"
                    + "         b = a * 2;\n"
                    + "         c = b * 2;\n"
                    + "         System.out.println(b);\n"
                    + "     } else {\n"
                    + "         b = a * 3;\n"
                    + "         System.out.println(b);\n"
                    + "     }\n"
                    + "     counter--;\n"
                    + " }";

    private static final String EXPECTED_WHILE_IF_BLOCK =
            ""
                    + "while (counter > 0) {\n"
                    + "  myFun_0_1(a, b, c);\n"
                    + "  if (a > 0) {\n"
                    + "    myFun_0_1_2(a, b, c);\n"
                    + "  } else {\n"
                    + "    myFun_0_1_3(a, b, c);\n"
                    + "  }\n"
                    + "  counter--;\n"
                    + " }";

    @Test
    public void testWhileIfBlockExtract() {
        String parameters = "a, b, c";
        BlockStatementSplitter splitter =
                new BlockStatementSplitter(GIVEN_WHILE_IF_BLOCK, parameters);

        String rewriteBlock = splitter.rewriteBlock("myFun");
        Map<String, List<String>> extractedBlocks = splitter.extractBlocks();

        assertThat(trimLines(rewriteBlock)).isEqualTo(trimLines(EXPECTED_WHILE_IF_BLOCK));
        assertThat(extractedBlocks.size()).isEqualTo(3);
        assertThat(extractedBlocks.get("myFun_0_1"))
                .containsExactly("int localA = a + 1000;", "System.out.println(localA);");
        assertThat(extractedBlocks.get("myFun_0_1_3"))
                .containsExactly("b = a * 3;", "System.out.println(b);");
        assertThat(extractedBlocks.get("myFun_0_1_2"))
                .containsExactly("b = a * 2;", "c = b * 2;", "System.out.println(b);");
    }
}
