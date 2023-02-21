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

import java.util.LinkedHashSet;
import java.util.Stack;

/**
 * Rewrite functions with return values by splitting them into two functions and storing the return
 * values into member variables.
 *
 * <p><i>Before</i>
 *
 * <pre><code>
 * public class Example {
 *     public int myFun(int a) {
 *         a += 1;
 *         return a;
 *     }
 * }
 * </code></pre>
 *
 * <p><<i>After</i>
 *
 * <pre><code>
 * public class Example {
 *     int myFunReturnValue$0;
 *
 *     public int myFun(int a) {
 *         myFunImpl(a);
 *         return myFunReturnValue$0;
 *     }
 *
 *     void myFunImpl(int a) {
 *         a += 1;
 *         {
 *             myFunReturnValue$0 = a;
 *             return;
 *         }
 *     }
 * }
 * </code></pre>
 */
@Internal
public class ReturnValueRewriter implements CodeRewriter {

    private final int maxMethodLength;

    private final CommonTokenStream tokenStream;
    private final TokenStreamRewriter rewriter;

    public ReturnValueRewriter(String code, int maxMethodLength) {
        this.maxMethodLength = maxMethodLength;

        this.tokenStream = new CommonTokenStream(new JavaLexer(CharStreams.fromString(code)));
        this.rewriter = new TokenStreamRewriter(tokenStream);
    }

    public String rewrite() {
        OuterVisitor visitor = new OuterVisitor();
        JavaParser javaParser = new JavaParser(tokenStream);
        javaParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        visitor.visit(javaParser.compilationUnit());
        return rewriter.getText();
    }

    private class OuterVisitor extends JavaParserBaseVisitor<Void> {

        private final Stack<StringBuilder> newFields;

        private OuterVisitor() {
            this.newFields = new Stack<>();
        }

        @Override
        public Void visitClassBody(JavaParser.ClassBodyContext ctx) {
            newFields.push(new StringBuilder());
            Void ret = visitChildren(ctx);
            rewriter.insertAfter(ctx.start, "\n" + newFields.pop().toString());
            return ret;
        }

        @Override
        public Void visitMethodDeclaration(JavaParser.MethodDeclarationContext ctx) {
            if ("void".equals(ctx.typeTypeOrVoid().getText())) {
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
            String methodQualifier = "";
            if (ctx.THROWS() != null) {
                methodQualifier =
                        " throws " + CodeSplitUtil.getContextString(ctx.qualifiedNameList());
            }

            // make new return variable
            String returnVarName = CodeSplitUtil.newName(functionName + "ReturnValue");
            newFields.peek().append(String.format("%s %s;\n", type, returnVarName));

            // replace return statements
            InnerVisitor visitor = new InnerVisitor(returnVarName);
            visitor.visitMethodDeclaration(ctx);

            // make new function call
            String newMethodBody =
                    String.format(
                            "{ %sImpl(%s); return %s; }",
                            functionName, String.join(", ", declarations), returnVarName);
            String implMethodDeclaration =
                    String.format("void %sImpl%s%s ", functionName, parameters, methodQualifier);
            rewriter.insertBefore(
                    ctx.methodBody().start, newMethodBody + "\n\n" + implMethodDeclaration);

            return null;
        }
    }

    private class InnerVisitor extends JavaParserBaseVisitor<Void> {

        private final String returnVarName;

        private InnerVisitor(String returnVarName) {
            this.returnVarName = returnVarName;
        }

        @Override
        public Void visitClassBody(JavaParser.ClassBodyContext ctx) {
            // skip anonymous class
            return null;
        }

        @Override
        public Void visitLambdaBody(JavaParser.LambdaBodyContext ctx) {
            // skip lambda
            return null;
        }

        @Override
        public Void visitStatement(JavaParser.StatementContext ctx) {
            if (ctx.RETURN() != null) {
                String newReturnStatement =
                        String.format(
                                // we add a pair of {} around these statements to avoid the original
                                // return statement without {}, for example:
                                // if (...) return;
                                "{ %s = %s; return; }",
                                returnVarName,
                                CodeSplitUtil.getContextString(ctx.expression().get(0)));
                rewriter.replace(ctx.start, ctx.stop, newReturnStatement);
            }
            return visitChildren(ctx);
        }
    }
}
