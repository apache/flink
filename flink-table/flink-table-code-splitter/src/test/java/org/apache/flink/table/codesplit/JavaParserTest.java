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

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link JavaParser}. */
public class JavaParserTest {

    @Test
    public void testConstructorCall() {
        String code =
                "public class A extends B {\n"
                        + "  private final int a, b;\n"
                        + "  public A(int a) {\n"
                        + "    this(a, 0);\n"
                        + "  }\n"
                        + "  public A(int a, int b) {\n"
                        + "    super(a, b);\n"
                        + "    this.a = a;\n"
                        + "    this.b = b;\n"
                        + "  }\n"
                        + "}";
        CommonTokenStream tokenStream =
                new CommonTokenStream(new JavaLexer(CharStreams.fromString(code)));
        JavaParser parser = new JavaParser(tokenStream);
        TestConstructorCallVisitor visitor = new TestConstructorCallVisitor();
        visitor.visit(parser.compilationUnit());
        Assert.assertEquals(1, visitor.thisCount);
        Assert.assertEquals(1, visitor.superCount);
    }

    private static class TestConstructorCallVisitor extends JavaParserBaseVisitor<Void> {
        private int thisCount = 0;
        private int superCount = 0;

        @Override
        public Void visitConstructorCall(JavaParser.ConstructorCallContext ctx) {
            if (ctx.THIS() != null) {
                thisCount++;
            } else if (ctx.SUPER() != null) {
                superCount++;
            }
            return visitChildren(ctx);
        }
    }
}
