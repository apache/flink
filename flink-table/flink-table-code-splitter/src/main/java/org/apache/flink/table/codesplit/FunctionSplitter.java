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
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.atn.PredictionMode;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Split long functions into several smaller functions.
 *
 * <p>This rewriter only deals with functions without return values. Functions with return values
 * should have been converted by {@link ReturnValueRewriter}. For functions with <code>return</code>
 * statements, this rewriter will add a check for early returns with the help of {@link
 * AddBoolBeforeReturnRewriter}.
 *
 * <p><i>Before</i>
 *
 * <pre><code>
 * public class Example {
 *     public void myFun(int a, int b) {
 *         a += b;
 *         b += a;
 *         if (a > 0) {
 *             return;
 *         }
 *         a *= 2;
 *         b *= 2;
 *         System.out.println(a);
 *         System.out.println(b);
 *     }
 * }
 * </code></pre>
 *
 * <p><i>After</i>
 *
 * <pre><code>
 * public class Example {
 *     boolean myFunHasReturned$0;
 *
 *     public void myFun(int a, int b) {
 *         myFunHasReturned$0 = false;
 *         myFun_split1(a, b);
 *
 *         myFun_split2(a, b);
 *         if (myFunHasReturned$0) {
 *             return;
 *         }
 *
 *         myFun_split3(a, b);
 *     }
 *
 *     void myFun_split1(int a, int b) {
 *         a += b;
 *         b += a;
 *     }
 *
 *     void myFun_split2(int a, int b) {
 *         if (a > 0) {
 *             {
 *                 myFunHasReturned$0 = true;
 *                 return;
 *             }
 *         }
 *     }
 *
 *     void myFun_split3(int a, int b) {
 *         a *= 2;
 *         b *= 2;
 *         System.out.println(a);
 *         System.out.println(b);
 *     }
 * }
 * </code></pre>
 */
@Internal
public class FunctionSplitter implements CodeRewriter {

    private String code;
    private final int maxMethodLength;

    public FunctionSplitter(String code, int maxMethodLength) {
        this.code = code;
        this.maxMethodLength = maxMethodLength;
    }

    public String rewrite() {
        AddBoolBeforeReturnRewriter boolRewriter =
                new AddBoolBeforeReturnRewriter(this.code, maxMethodLength);
        code = boolRewriter.rewrite();
        FunctionSplitVisitor visitor = new FunctionSplitVisitor(boolRewriter.getBoolVarNames());
        JavaParser javaParser = new JavaParser(visitor.tokenStream);
        javaParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        visitor.visit(javaParser.compilationUnit());
        return visitor.rewriter.getText();
    }

    private class FunctionSplitVisitor extends JavaParserBaseVisitor<Void> {

        private final CommonTokenStream tokenStream;
        private final TokenStreamRewriter rewriter;

        private final List<Map<String, String>> boolVarNames;

        private int classCount;

        private FunctionSplitVisitor(List<Map<String, String>> boolVarNames) {
            this.tokenStream = new CommonTokenStream(new JavaLexer(CharStreams.fromString(code)));
            this.rewriter = new TokenStreamRewriter(tokenStream);

            this.boolVarNames = boolVarNames;

            this.classCount = -1;
        }

        @Override
        public Void visitClassBody(JavaParser.ClassBodyContext ctx) {
            classCount++;
            return visitChildren(ctx);
        }

        @Override
        public Void visitMethodDeclaration(JavaParser.MethodDeclarationContext ctx) {

            if (!"void".equals(ctx.typeTypeOrVoid().getText())) {
                return null;
            }

            long methodBodyLength = CodeSplitUtil.getContextTextLength(ctx.methodBody().block());

            if (methodBodyLength < maxMethodLength) {
                return null;
            }

            if (ctx.methodBody().block().blockStatement() == null
                    || ctx.methodBody().block().blockStatement().size() <= 1) {
                return null;
            }

            List<String> splitFuncBodies = new ArrayList<>();
            List<JavaParser.BlockStatementContext> blockStatementContexts = new ArrayList<>();

            // function real parameters
            LinkedHashSet<String> declarations = new LinkedHashSet<>();
            new JavaParserBaseVisitor<Void>() {
                @Override
                public Void visitFormalParameter(JavaParser.FormalParameterContext ctx) {
                    declarations.add(ctx.variableDeclaratorId().getText());
                    return null;
                }
            }.visit(ctx);

            // function definition
            String type = CodeSplitUtil.getContextString(ctx.typeTypeOrVoid());
            String functionName = ctx.IDENTIFIER().getText();
            String parameters = CodeSplitUtil.getContextString(ctx.formalParameters());

            for (JavaParser.BlockStatementContext blockStatementContext :
                    ctx.methodBody().block().blockStatement()) {
                blockStatementContexts.add(blockStatementContext);
                splitFuncBodies.add(CodeSplitUtil.getContextString(blockStatementContext));
            }

            List<String> mergedCodeBlocks = getMergedCodeBlocks(splitFuncBodies);
            List<String> newSplitMethods = new ArrayList<>();
            List<String> newSplitMethodCalls = new ArrayList<>();

            String methodQualifier = "";
            if (ctx.THROWS() != null) {
                methodQualifier =
                        " throws " + CodeSplitUtil.getContextString(ctx.qualifiedNameList());
            }

            String hasReturnedVarName = boolVarNames.get(classCount).get(functionName + parameters);
            if (hasReturnedVarName != null) {
                rewriter.insertAfter(
                        ctx.methodBody().block().start,
                        String.format("\n%s = false;", hasReturnedVarName));
            }

            for (String methodBody : mergedCodeBlocks) {
                long counter = CodeSplitUtil.getCounter().getAndIncrement();

                // void f_splitXX(int x, String y)
                String splitMethodDef =
                        type
                                + " "
                                + functionName
                                + "_split"
                                + counter
                                + parameters
                                + methodQualifier;

                String newSplitMethod = splitMethodDef + " {\n" + methodBody + "\n}\n";

                String newSplitMethodCall =
                        functionName
                                + "_split"
                                + counter
                                + "("
                                + String.join(", ", declarations)
                                + ");\n";
                if (hasReturnedVarName != null && newSplitMethod.contains(hasReturnedVarName)) {
                    newSplitMethodCall +=
                            String.format("if (%s) { return; }\n", hasReturnedVarName);
                }

                newSplitMethods.add(newSplitMethod);
                newSplitMethodCalls.add(newSplitMethodCall);
            }

            for (int i = 0; i < blockStatementContexts.size(); i++) {
                if (i < newSplitMethods.size()) {
                    rewriter.replace(
                            blockStatementContexts.get(i).start,
                            blockStatementContexts.get(i).stop,
                            newSplitMethodCalls.get(i));
                    rewriter.insertAfter(ctx.getParent().stop, "\n" + newSplitMethods.get(i));
                } else {
                    rewriter.delete(
                            blockStatementContexts.get(i).start,
                            blockStatementContexts.get(i).stop);
                }
            }
            return null;
        }

        private List<String> getMergedCodeBlocks(List<String> codeBlock) {
            List<String> mergedCodeBlocks = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            codeBlock.forEach(
                    code -> {
                        if (sb.length() + code.length() + 1 <= maxMethodLength) {
                            sb.append("\n").append(code);
                        } else {
                            if (sb.length() > 0) {
                                mergedCodeBlocks.add(sb.toString());
                                sb.delete(0, sb.length());
                            }
                            sb.append(code);
                        }
                    });
            if (sb.length() > 0) {
                mergedCodeBlocks.add(sb.toString());
            }
            return mergedCodeBlocks;
        }
    }
}
