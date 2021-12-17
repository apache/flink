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

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.atn.PredictionMode;

import java.util.LinkedHashSet;

/**
 * Extract true and false branch of IFs and ELSEs in long methods into two smaller methods.
 *
 * <p>This rewriter only deals with functions without return values. Functions with return values
 * should have been converted by {@link ReturnValueRewriter}. Also, this rewriter will not extract
 * blocks containing <code>return</code> statements for correctness.
 *
 * <p><i>Before</i>
 *
 * <pre><code>
 * public class Example {
 *     int b;
 *     public void myFun(int a) {
 *         if (a > 0) {
 *             b = a * 2;
 *             System.out.println(b);
 *         } else {
 *             b = a * 3;
 *             System.out.println(b);
 *         }
 *     }
 * }
 * </code></pre>
 *
 * <p><i>After</i>
 *
 * <pre><code>
 * public class Example {
 *     int b;
 *     public void myFun(int a) {
 *         if (a > 0) {
 *             myFun_trueFilter1(a);
 *         } else {
 *             myFun_falseFilter2(a);
 *         }
 *     }
 *     void myFun_trueFilter1(int a) {
 *         b = a * 2;
 *         System.out.println(b);
 *     }
 *     void myFun_falseFilter2(int a) {
 *         b = a * 3;
 *         System.out.println(b);
 *     }
 * }
 * </code></pre>
 */
@Internal
public class IfStatementRewriter implements CodeRewriter {

    private final long maxMethodLength;
    private IfStatementVisitor visitor;

    public IfStatementRewriter(String code, long maxMethodLength) {
        this.maxMethodLength = maxMethodLength;
        this.visitor = new IfStatementVisitor(code);
    }

    public String rewrite() {
        String rewriterCode = visitor.rewriteAndGetCode();
        while (visitor.hasRewrite()) {
            visitor = new IfStatementVisitor(rewriterCode);
            rewriterCode = visitor.rewriteAndGetCode();
        }
        return rewriterCode;
    }

    private class IfStatementVisitor extends JavaParserBaseVisitor<Void> {

        private final CommonTokenStream tokenStream;

        private final TokenStreamRewriter rewriter;

        private long rewriteCount;

        private IfStatementVisitor(String code) {
            this.tokenStream = new CommonTokenStream(new JavaLexer(CharStreams.fromString(code)));
            this.rewriter = new TokenStreamRewriter(tokenStream);
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

            for (JavaParser.BlockStatementContext blockStatementContext :
                    ctx.methodBody().block().blockStatement()) {

                if (blockStatementContext.statement() != null
                        && blockStatementContext.statement().IF() != null
                        && blockStatementContext.statement().getText().length() > maxMethodLength) {
                    if (shouldExtract(blockStatementContext.statement().statement(0))) {
                        long counter = CodeSplitUtil.getCounter().incrementAndGet();

                        String methodDef =
                                type
                                        + " "
                                        + functionName
                                        + "_trueFilter"
                                        + counter
                                        + parameters
                                        + methodQualifier;

                        String newMethod =
                                methodDef
                                        + CodeSplitUtil.getContextString(
                                                blockStatementContext
                                                        .statement()
                                                        .statement(0)
                                                        .block())
                                        + "\n";

                        String newMethodCall =
                                functionName
                                        + "_trueFilter"
                                        + counter
                                        + "("
                                        + String.join(", ", declarationContext)
                                        + ");\n";
                        rewriter.replace(
                                blockStatementContext.statement().statement(0).block().start,
                                blockStatementContext.statement().statement(0).block().stop,
                                "{\n" + newMethodCall + "\n}\n");
                        rewriter.insertAfter(ctx.getParent().stop, "\n" + newMethod + "\n");
                        rewriteCount++;
                    }

                    if (shouldExtract(blockStatementContext.statement().statement(1))) {
                        long counter = CodeSplitUtil.getCounter().incrementAndGet();

                        String methodDef =
                                type
                                        + " "
                                        + functionName
                                        + "_falseFilter"
                                        + counter
                                        + parameters
                                        + methodQualifier;

                        String newMethod =
                                methodDef
                                        + CodeSplitUtil.getContextString(
                                                blockStatementContext
                                                        .statement()
                                                        .statement(1)
                                                        .block())
                                        + "\n";

                        String newMethodCall =
                                functionName
                                        + "_falseFilter"
                                        + counter
                                        + "("
                                        + String.join(", ", declarationContext)
                                        + ");\n";
                        rewriter.replace(
                                blockStatementContext.statement().statement(1).block().start,
                                blockStatementContext.statement().statement(1).block().stop,
                                "{\n" + newMethodCall + "\n}\n");
                        rewriter.insertAfter(ctx.getParent().stop, "\n" + newMethod + "\n");
                        rewriteCount++;
                    }
                }
            }
            return null;
        }

        private boolean shouldExtract(JavaParser.StatementContext ctx) {
            return ctx != null
                    && ctx.block() != null
                    && ctx.block().blockStatement() != null
                    // if there is only one statement in the block it's useless to extract
                    // it into a separate function
                    && ctx.block().blockStatement().size() > 1
                    // should not extract blocks with return statements
                    && getNumReturnsInContext(ctx.block()) == 0;
        }

        private String rewriteAndGetCode() {
            JavaParser javaParser = new JavaParser(tokenStream);
            javaParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            visit(javaParser.compilationUnit());
            return rewriter.getText();
        }

        private boolean hasRewrite() {
            return rewriteCount > 0L;
        }
    }

    private int getNumReturnsInContext(ParserRuleContext ctx) {
        ReturnCounter counter = new ReturnCounter();
        counter.visit(ctx);
        return counter.returnCount;
    }

    private static class ReturnCounter extends JavaParserBaseVisitor<Void> {

        private int returnCount = 0;

        @Override
        public Void visitStatement(JavaParser.StatementContext ctx) {
            if (ctx.RETURN() != null) {
                returnCount++;
            }
            return visitChildren(ctx);
        }
    }
}
