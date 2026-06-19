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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A helper class of {@link FunctionSplitter}.
 *
 * <p>Add a boolean variable before each return statement so that when the function is split, the
 * outer function can determine whether to return early with this boolean variable.
 *
 * <p>The name of the variable is stored in a list of map. Index of list is the index of class
 * (outer class is 0, 1st inner class is 1, 2nd inner class is 2, etc.), key of map is function name
 * and its parameters.
 *
 * <p><i>Before</i>
 *
 * <pre><code>
 * public class Example {
 *     public void myFun(int[] a) {
 *         if (a[0] > 0) {
 *             a[0] += 5;
 *             return;
 *         }
 *         a[0] -= 5;
 *         return;
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
 *     public void myFun(int[] a) {
 *         if (a[0] > 0) {
 *             a[0] += 5;
 *             {
 *                 myFunHasReturned$0 = true;
 *                 return;
 *             }
 *         }
 *         a[0] -= 5;
 *         {
 *             myFunHasReturned$0 = true;
 *             return;
 *         }
 *     }
 * }
 * </code></pre>
 */
@Internal
public class AddBoolBeforeReturnRewriter implements CodeRewriter {

    private final int maxMethodLength;

    private final CommonTokenStream tokenStream;
    private final TokenStreamRewriter rewriter;

    private final List<Map<String, String>> boolVarNames;

    public AddBoolBeforeReturnRewriter(String code, int maxMethodLength) {
        this.maxMethodLength = maxMethodLength;

        this.tokenStream = new CommonTokenStream(new JavaLexer(CharStreams.fromString(code)));
        this.rewriter = new TokenStreamRewriter(tokenStream);

        this.boolVarNames = new ArrayList<>();
    }

    public String rewrite() {
        OuterVisitor visitor = new OuterVisitor();
        JavaParser javaParser = new JavaParser(tokenStream);
        javaParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        visitor.visit(javaParser.compilationUnit());
        return rewriter.getText();
    }

    public List<Map<String, String>> getBoolVarNames() {
        return boolVarNames;
    }

    private class OuterVisitor extends JavaParserBaseVisitor<Void> {

        @Override
        public Void visitClassBody(JavaParser.ClassBodyContext ctx) {
            Map<String, String> classBoolNames = new HashMap<>();
            boolVarNames.add(classBoolNames);
            Void ret = visitChildren(ctx);
            for (String varName : classBoolNames.values()) {
                rewriter.insertAfter(ctx.start, String.format("\nboolean %s;", varName));
            }
            return ret;
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

            // function definition
            String functionName = ctx.IDENTIFIER().getText();
            String parameters = CodeSplitUtil.getContextString(ctx.formalParameters());

            // add boolean variable setting before return
            String boolVarName = CodeSplitUtil.newName(functionName + "HasReturned");
            InnerVisitor visitor = new InnerVisitor(functionName + parameters, boolVarName);
            visitor.visitMethodDeclaration(ctx);

            return null;
        }
    }

    private class InnerVisitor extends JavaParserBaseVisitor<Void> {

        private final String key;
        private final String boolVarName;

        private InnerVisitor(String key, String boolVarName) {
            this.key = key;
            this.boolVarName = boolVarName;
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
                // we add a pair of {} around these statements to avoid the original
                // return statement without {}, for example:
                // if (...) return;
                String newReturnStatement = String.format("{ %s = true; return; }", boolVarName);
                rewriter.replace(ctx.start, ctx.stop, newReturnStatement);
                boolVarNames.get(boolVarNames.size() - 1).put(key, boolVarName);
            }
            return visitChildren(ctx);
        }
    }
}
