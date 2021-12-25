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
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.atn.PredictionMode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

/**
 * Group member variables with the same type into arrays to reduce the number of members.
 *
 * <p><i>Before</i>
 *
 * <pre><code>
 * public class Example {
 *     int a;
 *     long b;
 *     int c = 1;
 *     long d = 2;
 *     public void myFun() {
 *         System.out.println(a + b + c + d);
 *     }
 * }
 * </code></pre>
 *
 * <p><i>After</i>
 *
 * <pre><code>
 * public class Example {
 *     int[] rewrite$0 = new int[2];
 *     long[] rewrite$1 = new long[2];
 *     {
 *         rewrite$0[1] = 1;
 *         rewrite$1[1] = 2;
 *     }
 *     public void myFun() {
 *         System.out.println(rewrite$0[0] + rewrite$1[0] + rewrite$0[1] + rewrite$1[1]);
 *     }
 * }
 * </code></pre>
 */
@Internal
public class MemberFieldRewriter implements CodeRewriter {

    private final int maxFieldCount;

    private String code;
    private TokenStreamRewriter rewriter;

    public MemberFieldRewriter(String code, int maxFieldCount) {
        this.code = code;
        this.maxFieldCount = maxFieldCount;
    }

    public String rewrite() {
        MemberFieldVisitor fieldVisitor = new MemberFieldVisitor();
        fieldVisitor.visit(prepareRewrite().compilationUnit());
        if (fieldVisitor.fieldCount >= maxFieldCount) {
            code = rewriter.getText();
            new MemberFieldReplaceVisitor(fieldVisitor.replaceMap)
                    .visit(prepareRewrite().compilationUnit());
            return rewriter.getText();
        } else {
            return code;
        }
    }

    private JavaParser prepareRewrite() {
        CommonTokenStream tokenStream =
                new CommonTokenStream(new JavaLexer(CharStreams.fromString(code)));
        this.rewriter = new TokenStreamRewriter(tokenStream);
        JavaParser javaParser = new JavaParser(tokenStream);
        javaParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        return javaParser;
    }

    private static class MemberField {
        String oldName;
        String type;
        int id;
        String init;

        MemberField(String oldName, String type, int id, String init) {
            this.oldName = oldName;
            this.type = type;
            this.id = id;
            this.init = init;
        }
    }

    private static class StackElement {
        List<MemberField> fields;
        Map<String, Integer> typeCounts;

        StackElement() {
            fields = new ArrayList<>();
            typeCounts = new HashMap<>();
        }
    }

    private class MemberFieldVisitor extends JavaParserBaseVisitor<Void> {

        private final Stack<StackElement> classStack;
        private final Map<String, String> replaceMap;
        private final Set<String> varNames;
        private int fieldCount = 0;

        MemberFieldVisitor() {
            classStack = new Stack<>();
            replaceMap = new HashMap<>();
            varNames = new HashSet<>();
        }

        @Override
        public Void visitClassDeclaration(JavaParser.ClassDeclarationContext ctx) {
            classStack.push(new StackElement());
            Void ret = visitChildren(ctx);
            rewriteClassDeclaration(ctx);
            classStack.pop();
            return ret;
        }

        @Override
        public Void visitMemberDeclaration(JavaParser.MemberDeclarationContext ctx) {
            if (ctx.fieldDeclaration() == null) {
                return null;
            }

            checkMemberDeclaration(ctx);

            for (JavaParser.ModifierContext modifier :
                    ((JavaParser.ClassBodyDeclarationContext) ctx.getParent()).modifier()) {
                if ("static".equals(modifier.getText())) {
                    // we will not modify static fields
                    return null;
                }
            }

            String fieldName =
                    ctx.fieldDeclaration()
                            .variableDeclarators()
                            .variableDeclarator(0)
                            .variableDeclaratorId()
                            .getText();
            if ("references".equals(fieldName)) {
                // this is a special field name used by all code generator
                return null;
            }

            String type = ctx.fieldDeclaration().typeType().getText();
            String init =
                    CodeSplitUtil.getContextString(
                            ctx.fieldDeclaration()
                                    .variableDeclarators()
                                    .variableDeclarator(0)
                                    .variableInitializer());

            StackElement classInfo = classStack.peek();
            Integer typeCount = classInfo.typeCounts.get(type);
            int id = typeCount == null ? 0 : typeCount;
            classInfo.typeCounts.put(type, id + 1);
            classInfo.fields.add(new MemberField(fieldName, type, id, init));

            rewriter.delete(ctx.getParent().start, ctx.getParent().stop);
            fieldCount++;
            return null;
        }

        private void rewriteClassDeclaration(JavaParser.ClassDeclarationContext ctx) {
            Map<String, String> typeFieldNames = new HashMap<>();
            StringBuilder newDeclaration = new StringBuilder("\n");

            for (Map.Entry<String, Integer> typeCount : classStack.peek().typeCounts.entrySet()) {
                String type = typeCount.getKey();
                String typeWithoutArgs =
                        type.indexOf('<') == -1
                                ? type
                                : type.substring(0, type.indexOf('<'))
                                        + type.substring(type.lastIndexOf('>') + 1);
                int count = typeCount.getValue();
                String fieldName = CodeSplitUtil.newName("rewrite");
                typeFieldNames.put(type, fieldName);

                StringBuilder newField = new StringBuilder();
                newField.append(type).append("[] ").append(fieldName).append(" = new ");
                int pos = typeWithoutArgs.indexOf("[");
                if (pos == -1) {
                    newField.append(typeWithoutArgs).append("[").append(count).append("]");
                } else {
                    newField.append(typeWithoutArgs, 0, pos)
                            .append("[")
                            .append(count)
                            .append("]")
                            .append(typeWithoutArgs, pos, typeWithoutArgs.length());
                }
                newField.append(";\n");

                newDeclaration.append(newField);
            }

            boolean hasInit = false;
            for (MemberField field : classStack.peek().fields) {
                String newName = typeFieldNames.get(field.type) + "[" + field.id + "]";
                replaceMap.put(field.oldName, newName);
                if (field.init.length() == 0) {
                    continue;
                }

                if (!hasInit) {
                    newDeclaration.append("\n{\n");
                    hasInit = true;
                }
                newDeclaration.append(newName).append(" = ").append(field.init).append(";\n");
            }
            if (hasInit) {
                newDeclaration.append("}\n");
            }

            rewriter.insertAfter(ctx.classBody().start, newDeclaration.toString());
        }

        private void checkMemberDeclaration(JavaParser.MemberDeclarationContext ctx) {
            if (ctx.fieldDeclaration() == null) {
                return;
            }

            Preconditions.checkArgument(
                    ctx.fieldDeclaration().variableDeclarators().variableDeclarator().size() == 1,
                    "%s\nCodegen rewrite failed. You can only declare one field in one statement.",
                    code);
            for (JavaParser.VariableDeclaratorContext v :
                    ctx.fieldDeclaration().variableDeclarators().variableDeclarator()) {
                String identifier = v.variableDeclaratorId().getText();
                Preconditions.checkArgument(
                        !varNames.contains(identifier),
                        "%s\nCodegen rewrite failed. Field names should not be the same. Name: %s",
                        code,
                        identifier);
                varNames.add(identifier);
            }
        }
    }

    private class MemberFieldReplaceVisitor extends JavaParserBaseVisitor<Void> {

        private final Map<String, String> replaceMap;
        // this set is to prevent us from mistakenly replacing method parameters
        private final Set<String> excludedNames;

        MemberFieldReplaceVisitor(Map<String, String> replaceMap) {
            this.replaceMap = replaceMap;
            excludedNames = new HashSet<>();
        }

        @Override
        public Void visitMethodDeclaration(JavaParser.MethodDeclarationContext ctx) {
            if (ctx.formalParameters().formalParameterList() != null) {
                for (JavaParser.FormalParameterContext formalParameter :
                        ctx.formalParameters().formalParameterList().formalParameter()) {
                    excludedNames.add(formalParameter.variableDeclaratorId().getText());
                }
            }
            visitChildren(ctx);
            excludedNames.clear();
            return null;
        }

        @Override
        public Void visitConstructorDeclaration(JavaParser.ConstructorDeclarationContext ctx) {
            if (ctx.formalParameters().formalParameterList() != null) {
                for (JavaParser.FormalParameterContext formalParameter :
                        ctx.formalParameters().formalParameterList().formalParameter()) {
                    excludedNames.add(formalParameter.variableDeclaratorId().getText());
                }
            }
            visitChildren(ctx);
            excludedNames.clear();
            return null;
        }

        @Override
        public Void visitLocalVariableDeclaration(JavaParser.LocalVariableDeclarationContext ctx) {
            for (JavaParser.VariableDeclaratorContext dec :
                    ctx.variableDeclarators().variableDeclarator()) {
                excludedNames.add(dec.variableDeclaratorId().getText());
            }
            return visitChildren(ctx);
        }

        @Override
        public Void visitPrimary(JavaParser.PrimaryContext ctx) {
            if (ctx.THIS() != null) {
                ParserRuleContext parent = getThisParentContext(ctx);
                if (parent instanceof JavaParser.ExpressionContext) {
                    JavaParser.ExpressionContext expressionContext =
                            (JavaParser.ExpressionContext) parent;
                    if (expressionContext.bop != null && expressionContext.IDENTIFIER() != null) {
                        String rep = replaceMap.get(expressionContext.IDENTIFIER().getText());
                        if (rep != null) {
                            rewriter.replace(expressionContext.IDENTIFIER().getSymbol(), rep);
                        }
                    }
                }
            } else if (ctx.IDENTIFIER() != null) {
                String identifier = ctx.IDENTIFIER().getText();
                if (excludedNames.contains(identifier)) {
                    return null;
                }
                String rep = replaceMap.get(identifier);
                if (rep != null) {
                    rewriter.replace(ctx.IDENTIFIER().getSymbol(), rep);
                }
            } else {
                visitChildren(ctx);
            }
            return null;
        }

        private ParserRuleContext getThisParentContext(JavaParser.PrimaryContext ctx) {
            return ctx.getParent().getParent();
        }
    }
}
