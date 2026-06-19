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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.codesplit.BlockStatementGrouper.RewriteGroupedCode;
import org.apache.flink.table.codesplit.JavaParser.StatementContext;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.atn.PredictionMode;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;

/**
 * Extract branch of IFs, ELSEs statements and WHILEs code blocks into smaller methods. Single line
 * statement blocks, with more than one statement, like field or local variable operations will be
 * extracted also to separate methods.
 *
 * <p>This rewriter only deals with functions without return values. Functions with return values
 * should have been converted by {@link ReturnValueRewriter}. Also, this rewriter will not extract
 * blocks containing <code>return</code> statements for correctness.
 *
 * <p><i>Before</i>
 *
 * <pre><code>
 * public class Example {
 *
 *     int b;
 *     int c;
 *
 *     public void myFun(int a) {
 *
 *         int counter = 10;
 *         while (counter > 0) {
 *             int localA = a + 1000;
 *             System.out.println(localA);
 *             if (a > 0) {
 *                 b = a * 2;
 *                 c = b * 2;
 *                 System.out.println(b);
 *             } else {
 *                 b = a * 3;
 *                 System.out.println(b);
 *             }
 *             counter--;
 *         }
 *     }
 * }
 * </code></pre>
 *
 * <p><i>After</i>
 *
 * <pre><code>
 * public class Example {
 *
 *     int b;
 *     int c;
 *
 *     public void myFun(int a) {
 *
 *         int counter = 10;
 *
 *         while (counter > 0) {
 *             myFun_rewriteGroup1(a);
 *             counter--;
 *         }
 *     }
 *
 *     void myFun_rewriteGroup1(int a) {
 *         myFun_whileBody0_0(a);
 *         if (a > 0) {
 *             myFun_whileBody0_0_ifBody0(a);
 *         } else {
 *             myFun_whileBody0_0_ifBody1(a);
 *         }
 *     }
 *
 *     void myFun_whileBody0_0(int a) {
 *         int localA = a + 1000;
 *         System.out.println(localA);
 *     }
 *
 *     void myFun_whileBody0_0_ifBody1(int a) {
 *         b = a * 3;
 *         System.out.println(b);
 *     }
 *
 *     void myFun_whileBody0_0_ifBody0(int a) {
 *         b = a * 2;
 *         c = b * 2;
 *         System.out.println(b);
 *     }
 * }
 * </code></pre>
 */
@Internal
public class BlockStatementRewriter implements CodeRewriter {

    private final long maxMethodLength;

    private final BlockVisitor visitor;

    public BlockStatementRewriter(String code, long maxMethodLength) {
        this.maxMethodLength = maxMethodLength;
        this.visitor = new BlockVisitor(code);
    }

    @Override
    public String rewrite() {
        return visitor.rewriteAndGetCode();
    }

    private class BlockVisitor extends JavaParserBaseVisitor<Void> {

        private final CommonTokenStream tokenStream;

        private final TokenStreamRewriter rewriter;

        private BlockVisitor(String code) {
            this.tokenStream = new CommonTokenStream(new JavaLexer(CharStreams.fromString(code)));
            this.rewriter = new TokenStreamRewriter(tokenStream);
        }

        private String rewriteAndGetCode() {
            JavaParser javaParser = new JavaParser(tokenStream);
            javaParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            visit(javaParser.compilationUnit());
            return rewriter.getText();
        }

        @Override
        public Void visitMethodDeclaration(JavaParser.MethodDeclarationContext ctx) {

            if (!"void".equals(ctx.typeTypeOrVoid().getText())) {
                return null;
            }

            // function real parameters
            LinkedHashSet<String> declarationContext = new LinkedHashSet<>();
            new JavaParserBaseVisitor<Void>() {
                @Override
                public Void visitFormalParameter(JavaParser.FormalParameterContext ctx) {
                    declarationContext.add(ctx.variableDeclaratorId().getText());
                    return null;
                }
            }.visit(ctx);

            String type = CodeSplitUtil.getContextString(ctx.typeTypeOrVoid());
            String functionName = ctx.IDENTIFIER().getText();
            String parameters = CodeSplitUtil.getContextString(ctx.formalParameters());

            String methodQualifier = "";
            if (ctx.THROWS() != null) {
                methodQualifier =
                        " throws " + CodeSplitUtil.getContextString(ctx.qualifiedNameList());
            }

            int counter = 0;
            for (JavaParser.BlockStatementContext blockStatementContext :
                    ctx.methodBody().block().blockStatement()) {

                StatementContext statement = blockStatementContext.statement();
                if (statement != null
                        && statement.getText().length() > maxMethodLength
                        && (statement.IF() != null
                                || statement.ELSE() != null
                                || statement.WHILE() != null)) {

                    BlockStatementSplitter splitter =
                            new BlockStatementSplitter(
                                    CodeSplitUtil.getContextString(statement),
                                    String.join(", ", declarationContext));

                    // create rewrite context for every block that will be rewritten. This is for
                    // case
                    // when we can have many IF/ELSE/WHILE blocks in single method and
                    String context = String.format(functionName + "_%d", counter++);
                    // Rewrite function's body to include calls to extracted methods.
                    String blockRewrittenBody = splitter.rewriteBlock(context);

                    // Get extract methods from block's original body.
                    Map<String, List<String>> newMethods = splitter.extractBlocks();

                    // Try to group statements in function's new body
                    StringBuilder functionGroupedBody = new StringBuilder();
                    BlockStatementGrouper statementGrouper =
                            new BlockStatementGrouper(
                                    blockRewrittenBody,
                                    maxMethodLength,
                                    String.join(", ", declarationContext));

                    RewriteGroupedCode groupedCode = statementGrouper.rewrite(context);
                    // add new methods, representing extracted groups.
                    newMethods.putAll(groupedCode.getGroups());

                    functionGroupedBody.append("\n").append(groupedCode.getRewriteCode());

                    // replace original functions body with new, rewritten and grouped body.
                    rewriter.replace(statement.start, statement.stop, functionGroupedBody);

                    // build new method definitions.
                    StringJoiner newMethodDefinitions =
                            new StringJoiner(System.lineSeparator() + System.lineSeparator());
                    for (Entry<String, List<String>> entry : newMethods.entrySet()) {
                        StringBuilder sb =
                                new StringBuilder(type)
                                        .append(" ")
                                        .append(entry.getKey())
                                        .append(parameters)
                                        .append(methodQualifier)
                                        .append(" {")
                                        .append(System.lineSeparator());

                        StringJoiner bodyLines = new StringJoiner(System.lineSeparator());
                        for (String bodyLine : entry.getValue()) {
                            bodyLines.add(bodyLine);
                        }

                        sb.append(bodyLines).append(System.lineSeparator()).append("}");
                        newMethodDefinitions.add(sb.toString());
                    }

                    // add new method definitions to class
                    rewriter.insertAfter(
                            ctx.getParent().stop, "\n\n" + newMethodDefinitions + "\n");
                }
            }
            return null;
        }
    }
}
