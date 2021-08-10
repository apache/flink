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
import org.apache.flink.util.Preconditions;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.atn.PredictionMode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

/**
 * Extract and rename local variables into member variables.
 *
 * <p>This rewriter only deals with functions without return values. Functions with return values
 * should have been converted by {@link ReturnValueRewriter}.
 *
 * <p><i>Before</i>
 *
 * <pre><code>
 * public class Example {
 *     public void myFun1(int[] arr) {
 *         int a = 1;
 *         for (int b : arr) {
 *             System.out.println(b);
 *         }
 *     }
 *     public void myFun2() {
 *         int a = 2;
 *     }
 * }
 * </code></pre>
 *
 * <p><i>After</i>
 *
 * <pre><code>
 * public class Example {
 *     int a;
 *     int local$0;
 *     int local$1;
 *     public void myFun1(int[] arr) {
 *         a = 1;
 *         for (int b : arr) {
 *             local$0 = b;
 *             System.out.println(local$0);
 *         }
 *     }
 *     public void myFun2() {
 *         local$1 = 2;
 *     }
 * }
 * </code></pre>
 */
@Internal
public class DeclarationRewriter implements CodeRewriter {

    private final String code;
    private final int maxMethodLength;

    private final CommonTokenStream tokenStream;
    private final TokenStreamRewriter rewriter;

    private boolean hasRewrite = false;

    public DeclarationRewriter(String code, int maxMethodLength) {
        this.code = code;
        this.maxMethodLength = maxMethodLength;

        this.tokenStream = new CommonTokenStream(new JavaLexer(CharStreams.fromString(code)));
        this.rewriter = new TokenStreamRewriter(tokenStream);
    }

    public String rewrite() {
        JavaParser javaParser = new JavaParser(tokenStream);
        javaParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        new OuterBlockStatementExtractor().visit(javaParser.compilationUnit());
        String text = rewriter.getText();
        return hasRewrite ? text : null;
    }

    private class OuterBlockStatementExtractor extends JavaParserBaseVisitor<Void> {

        private final Stack<StringBuilder> newFields;
        private final Set<String> allVarNames;

        OuterBlockStatementExtractor() {
            this.newFields = new Stack<>();
            this.allVarNames = new HashSet<>();
        }

        @Override
        public Void visitClassBody(JavaParser.ClassBodyContext ctx) {
            newFields.push(new StringBuilder());
            Void ret = visitChildren(ctx);
            rewriter.insertAfter(ctx.start, "\n" + newFields.pop().toString());
            return ret;
        }

        @Override
        public Void visitFieldDeclaration(JavaParser.FieldDeclarationContext ctx) {
            // it might be the case that local variables have the same name with existing member
            // variables, so we should also add member variable names into the map
            if (ctx.variableDeclarators() != null) {
                for (JavaParser.VariableDeclaratorContext dec :
                        ctx.variableDeclarators().variableDeclarator()) {
                    allVarNames.add(dec.variableDeclaratorId().getText());
                }
            }
            return visitChildren(ctx);
        }

        @Override
        public Void visitMethodDeclaration(JavaParser.MethodDeclarationContext ctx) {
            if ("void".equals(ctx.typeTypeOrVoid().getText())) {
                visitMethodBody(ctx.methodBody());
            }
            return null;
        }

        @Override
        public Void visitMethodBody(JavaParser.MethodBodyContext ctx) {
            // this condition must be the same with the condition in
            // FunctionSplitter::visitMethodDeclaration
            if (CodeSplitUtil.getContextTextLength(ctx.block()) <= maxMethodLength) {
                return null;
            }

            hasRewrite = true;
            InnerBlockStatementExtractor extractor = new InnerBlockStatementExtractor();
            if (ctx.block() != null && ctx.block().blockStatement() != null) {
                for (JavaParser.BlockStatementContext blockStatementContext :
                        ctx.block().blockStatement()) {
                    extractor.visitBlockStatement(blockStatementContext);
                }
                newFields.peek().append(extractor.getNewLocalVariables());
            }
            return null;
        }

        private class InnerBlockStatementExtractor extends JavaParserBaseVisitor<Void> {

            private final StringBuilder newLocalVariables;
            private final Map<String, String> replaceMap;

            InnerBlockStatementExtractor() {
                this.newLocalVariables = new StringBuilder();
                this.replaceMap = new HashMap<>();
            }

            @Override
            public Void visitLocalVariableDeclaration(
                    JavaParser.LocalVariableDeclarationContext ctx) {
                Preconditions.checkArgument(
                        ctx.variableDeclarators().variableDeclarator().size() == 1,
                        "%s\nCodegen rewrite failed. You can only declare one local variable in one statement.",
                        code);

                JavaParser.VariableDeclaratorContext dec =
                        ctx.variableDeclarators().variableDeclarator(0);
                JavaParser.VariableDeclaratorIdContext decId = dec.variableDeclaratorId();

                extractLocalVariable(decId, ctx.typeType(), false);

                if (dec.variableInitializer() == null) {
                    rewriter.delete(ctx.start, ctx.getParent().stop);
                    return null;
                } else {
                    if (ctx.variableModifier() != null) {
                        for (JavaParser.VariableModifierContext modifier : ctx.variableModifier()) {
                            rewriter.delete(modifier.start, modifier.stop);
                        }
                    }
                    rewriter.delete(ctx.typeType().start, ctx.typeType().stop);
                    replaceLocalVar(decId.getText(), decId.IDENTIFIER().getSymbol());
                    return visitChildren(ctx);
                }
            }

            @Override
            public Void visitEnhancedForControl(JavaParser.EnhancedForControlContext ctx) {
                JavaParser.VariableDeclaratorIdContext decId = ctx.variableDeclaratorId();
                String newName = extractLocalVariable(decId, ctx.typeType(), true);

                JavaParser.StatementContext stmt =
                        (JavaParser.StatementContext) ctx.getParent().getParent();
                Preconditions.checkState(
                        stmt.statement(0) != null && stmt.statement(0).block() != null,
                        "%s\nCodegen rewrite failed. For statements must be placed inside a block.\n",
                        code);
                rewriter.insertAfter(
                        stmt.statement(0).block().start, newName + " = " + decId.getText() + ";");

                return visitChildren(ctx);
            }

            /** @return new name. */
            private String extractLocalVariable(
                    JavaParser.VariableDeclaratorIdContext decId,
                    JavaParser.TypeTypeContext typeType,
                    boolean forceNewName) {
                String name = decId.getText();
                if (forceNewName || allVarNames.contains(name)) {
                    // here we assume that the original code can be successfully compiled.
                    // that is to say, the scope of two variables with the same name will not
                    // overlap.
                    String newName = CodeSplitUtil.newName("local");
                    replaceMap.put(name, newName);
                    newLocalVariables
                            .append(typeType.getText())
                            .append(" ")
                            .append(newName)
                            .append(";\n");
                    return newName;
                } else {
                    newLocalVariables
                            .append(typeType.getText())
                            .append(" ")
                            .append(name)
                            .append(";\n");
                    allVarNames.add(name);
                    return name;
                }
            }

            @Override
            public Void visitPrimary(JavaParser.PrimaryContext ctx) {
                if (ctx.IDENTIFIER() != null) {
                    replaceLocalVar(ctx.IDENTIFIER().getText(), ctx.IDENTIFIER().getSymbol());
                    return null;
                } else {
                    return visitChildren(ctx);
                }
            }

            private void replaceLocalVar(String name, Token token) {
                String rep = replaceMap.get(name);
                if (rep != null) {
                    rewriter.replace(token, rep);
                }
            }

            String getNewLocalVariables() {
                return newLocalVariables.toString();
            }
        }
    }
}
