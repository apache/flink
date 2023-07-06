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
import org.apache.flink.table.codesplit.JavaParser.BlockStatementContext;
import org.apache.flink.table.codesplit.JavaParser.StatementContext;
import org.apache.flink.util.CollectionUtil;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Groups end extract single line statements such as operations on fields/local variables, IF and
 * WHILE statements and extract new method for each group making them smaller.
 *
 * <p>BlockStatementGrouper does not recognize if statement operates on local of class member
 * variable. Because of that, code must be preprocessed by {@link DeclarationRewriter} which
 * converts all local variables extracted as to member variables.
 *
 * <p><i>Before</i>
 *
 * <pre><code>
 * {
 *     a[0] += b[1];
 *     b[1] += a[1];
 *     while (counter > 0) {
 *         myFun_whileBody0_0(a, b);
 *         if (a[0] > 0) {
 *             myFun_whileBody0_0_ifBody0(a, b);
 *         } else {
 *             myFun_whileBody0_0_ifBody1(a, b);
 *         }
 *         counter--;
 *     }
 *
 *     a[2] += b[2];
 *     b[3] += a[3];
 * }
 *
 * </code></pre>
 *
 * <p><i>After</i>
 *
 * <pre><code>
 * {
 *     myFun_rewriteGroup4(a, b);
 *     myFun_rewriteGroup5(a, b);
 * }
 * </code></pre>
 *
 * <p>Where bodies of extracted "methods" are:
 *
 * <pre><code>
 *     myFun_rewriteGroup4 ->
 *         a[0] += b[1];
 *         b[1] += a[1];
 *         while (counter > 0) {
 *             myFun_rewriteGroup0_1_rewriteGroup3(a, b);
 *             counter--;
 *         }
 * </code></pre>
 *
 * <pre><code>
 *     myFun_rewriteGroup5 ->
 *         a[2] += b[2];
 *         b[3] += a[3];
 * </code></pre>
 *
 * <pre><code>
 *     myFun_rewriteGroup0_1_rewriteGroup3 ->
 *         myFun_whileBody0_0(a, b);
 *         if (a[0] > 0) {
 *             myFun_whileBody0_0_ifBody0(a, b);
 *         } else {
 *             myFun_whileBody0_0_ifBody1(a, b);
 *         }
 * </code></pre>
 */
@Internal
public class BlockStatementGrouper {

    private final String code;

    private final long maxMethodLength;

    private final String parameters;

    /**
     * Initialize new BlockStatementGrouper.
     *
     * @param code code block that should be rewritten for statement grouping.
     * @param maxMethodLength maximal length of the extracted code block.
     * @param parameters parameters definition that should be used for extracted methods.
     */
    public BlockStatementGrouper(String code, long maxMethodLength, String parameters) {
        this.code = code;
        this.maxMethodLength = maxMethodLength;
        this.parameters = parameters;
    }

    /**
     * Rewrite code block used for initialization of this object. The code block is grouped into new
     * methods.
     *
     * @param context prefix used for extracted group names.
     * @return {@link RewriteGroupedCode} representing rewritten code block and containing extracted
     *     groups with their names and content.
     */
    public RewriteGroupedCode rewrite(String context) {

        BlockStatementGrouperVisitor visitor =
                new BlockStatementGrouperVisitor(maxMethodLength, parameters);
        CommonTokenStream tokenStream =
                new CommonTokenStream(new JavaLexer(CharStreams.fromString(code)));
        JavaParser javaParser = new JavaParser(tokenStream);
        javaParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        TokenStreamRewriter rewriter = new TokenStreamRewriter(tokenStream);
        visitor.visitStatement(javaParser.statement(), context, rewriter);

        visitor.rewrite();
        Map<String, Pair<TokenStreamRewriter, List<LocalGroupElement>>> groups = visitor.groups;

        Map<String, List<String>> groupStrings =
                CollectionUtil.newHashMapWithExpectedSize(groups.size());
        for (Entry<String, Pair<TokenStreamRewriter, List<LocalGroupElement>>> group :
                groups.entrySet()) {
            List<String> collectedStringGroups =
                    group.getValue().getValue().stream()
                            .map(LocalGroupElement::getBody)
                            .collect(Collectors.toList());

            groupStrings.put(group.getKey(), collectedStringGroups);
        }

        return new RewriteGroupedCode(rewriter.getText(), groupStrings);
    }

    private static class BlockStatementGrouperVisitor {

        private final Map<String, Pair<TokenStreamRewriter, List<LocalGroupElement>>> groups =
                new HashMap<>();

        private final long maxMethodLength;

        private final String parameters;

        private int counter = 0;

        private BlockStatementGrouperVisitor(long maxMethodLength, String parameters) {
            this.maxMethodLength = maxMethodLength;
            this.parameters = parameters;
        }

        public void visitStatement(
                StatementContext ctx, String context, TokenStreamRewriter rewriter) {

            if (ctx.getChildCount() == 0) {
                return;
            }

            // For these statements here we want to process all "branches" separately, for example
            // TRUE and FALSE branch of IF/ELSE block.
            // each statement can be rewritten and extracted.
            if (ctx.WHILE() != null || ctx.IF() != null || ctx.ELSE() != null) {
                for (StatementContext statement : ctx.statement()) {
                    if (shouldExtract(statement)) {
                        String localContext = String.format("%s_%d", context, counter++);
                        groupBlock(statement, localContext, rewriter);
                    }
                }
            } else {
                // The block did not start from IF/ELSE/WHILE statement
                if (shouldExtract(ctx)) {
                    groupBlock(ctx, context, rewriter);
                }
            }
        }

        // Group continuous block of statements together. If Statement is an IF/ELSE/WHILE,
        // its body can be further grouped by recursive call to visitStatement method.
        private void groupBlock(
                StatementContext ctx, String context, TokenStreamRewriter rewriter) {
            int localGroupCodeLength = 0;
            List<LocalGroupElement> localGroup = new ArrayList<>();
            for (BlockStatementContext bsc : ctx.block().blockStatement()) {

                StatementContext statement = bsc.statement();
                if (statement.IF() != null
                        || statement.ELSE() != null
                        || statement.WHILE() != null) {
                    String localContext = context + "_rewriteGroup" + this.counter++;

                    CommonTokenStream tokenStream =
                            new CommonTokenStream(
                                    new JavaLexer(
                                            CharStreams.fromString(
                                                    CodeSplitUtil.getContextString(statement))));
                    TokenStreamRewriter localRewriter = new TokenStreamRewriter(tokenStream);
                    JavaParser javaParser = new JavaParser(tokenStream);
                    javaParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                    visitStatement(javaParser.statement(), localContext, localRewriter);

                    localGroup.add(new RewriteContextGroupElement(statement, localRewriter));

                    // new method call length to the localGroupCodeLength. The "3" contains two
                    // brackets for parameters and semicolon at the end of method call
                    localGroupCodeLength += 3 + localContext.length() + parameters.length();
                } else {

                    if (localGroupCodeLength + 1 + bsc.getText().length() <= maxMethodLength) {
                        localGroup.add(new ContextGroupElement(bsc));
                        localGroupCodeLength += bsc.getText().length();
                    } else {
                        if (addLocalGroup(localGroup, context, rewriter)) {
                            localGroup = new ArrayList<>();
                            localGroupCodeLength = 0;
                        }
                        localGroupCodeLength += bsc.getText().length();
                        localGroup.add(new ContextGroupElement(bsc));
                    }
                }
            }

            // Groups that have only one statement that is "single line statement" such as
            // "a[2] += b[2];" will not be extracted.
            addLocalGroup(localGroup, context, rewriter);
        }

        private boolean addLocalGroup(
                List<LocalGroupElement> localGroup, String context, TokenStreamRewriter rewriter) {
            if (localGroup.size() > 1
                    || (localGroup.size() == 1
                            && canGroupAsSingleStatement(localGroup.get(0).getContext()))) {
                String localContext = context + "_rewriteGroup" + this.counter++;
                groups.put(localContext, Pair.of(rewriter, localGroup));
                return true;
            }

            return false;
        }

        private boolean canGroupAsSingleStatement(ParserRuleContext context) {

            StatementContext statement;

            if (context instanceof StatementContext) {
                statement = (StatementContext) context;
            } else if (context instanceof BlockStatementContext) {
                statement = ((BlockStatementContext) context).statement();
            } else {
                return false;
            }

            return statement != null
                    && (statement.IF() != null
                            || statement.ELSE() != null
                            || statement.WHILE() != null);
        }

        private boolean shouldExtract(StatementContext ctx) {
            return ctx != null
                    && ctx.block() != null
                    && ctx.block().blockStatement() != null
                    // if there is only one statement in the block it's useless to extract
                    // it into a separate function
                    && ctx.block().blockStatement().size() > 1
                    // should not extract blocks with return statements
                    && getNumOfReturnOrJumpStatements(ctx.block()) == 0;
        }

        private int getNumOfReturnOrJumpStatements(ParserRuleContext ctx) {
            ReturnAndJumpCounter counter = new ReturnAndJumpCounter();
            counter.visit(ctx);
            return counter.getCounter();
        }

        private void rewrite() {
            for (Entry<String, Pair<TokenStreamRewriter, List<LocalGroupElement>>> group :
                    groups.entrySet()) {
                Pair<TokenStreamRewriter, List<LocalGroupElement>> pair = group.getValue();
                TokenStreamRewriter rewriter = pair.getKey();
                List<LocalGroupElement> value = pair.getValue();
                rewriter.replace(
                        value.get(0).getStart(),
                        value.get(value.size() - 1).getStop(),
                        group.getKey() + "(" + this.parameters + ");");
            }
        }
    }

    /**
     * Represents an extracted statement, it boundaries (start and stop token) and its String
     * representation. For example single line statement like: int a = 3; or block statement like
     * IF/ELSE/WHILE bodies.
     */
    private interface LocalGroupElement {

        /** @return start {@link Token} for this group element. */
        Token getStart();

        /** @return stop {@link Token} for this group element. */
        Token getStop();

        /** @return String representation of this group element. */
        String getBody();

        ParserRuleContext getContext();
    }

    /**
     * Extracted element that is represented solely by {@link ParserRuleContext}. It's used for
     * extracted statements that represent single line statement such as variable operation that
     * will not be further rewritten.
     */
    private static class ContextGroupElement implements LocalGroupElement {

        private final ParserRuleContext parserRuleContext;

        private ContextGroupElement(ParserRuleContext parserRuleContext) {
            this.parserRuleContext = parserRuleContext;
        }

        @Override
        public Token getStart() {
            return this.parserRuleContext.start;
        }

        @Override
        public Token getStop() {
            return this.parserRuleContext.stop;
        }

        @Override
        public String getBody() {
            return CodeSplitUtil.getContextString(this.parserRuleContext);
        }

        @Override
        public ParserRuleContext getContext() {
            return this.parserRuleContext;
        }
    }

    /**
     * Extracted element that is represented by {@link ParserRuleContext} and {@link
     * TokenStreamRewriter} It's used for extracted block statements that represent TRUE/FALSE
     * branches of IF/ELSE statements or WHILE's statement body. The string representation is
     * extracted from {@link TokenStreamRewriter} which could be further rewritten.
     *
     * <p>This element can be used for IF/ELSE/WHILE statements that will be rewritten in further
     * processing. Then this statement in the original code cen be rewritten using Start and Stop
     * tokens, whereas the getBody() method backed by TokenStreamRewriter will return the rewritten
     * value of the original statement.
     *
     * <p>The example would parserRuleContext and rewriter representing below statement:
     *
     * <pre><code>
     *   while (counter > 0) {
     *     myFun_whileBody0_0(a, b);
     *     if (a[0] > 0) {
     *       myFun_whileBody0_0_ifBody0(a, b);
     *     } else {
     *       myFun_whileBody0_0_ifBody1(a, b);
     *     }
     *
     *     a[2] += b[2];
     *     b[3] += a[3];
     *     if (a[0] > 0) {
     *       System.out.println("Hello");
     *     } else {
     *       System.out.println("World");
     *     }
     *
     *         counter--;
     *     }
     * </code></pre>
     *
     * <p>This statement, being a part of a different statement will be further rewritten, new
     * methods will be extracted from its body. At the end in order to rewrite the original
     * statement we need to know what is the current form of this expression. For that we can call
     * getBody() method, which in this case will return:
     *
     * <pre><code>
     * while (counter > 0) {
     *   myFun_rewriteGroup0_1_rewriteGroup3(a, b);
     *
     *   myFun_rewriteGroup0_1_rewriteGroup5(a, b);
     *
     *   counter--;
     * }
     * </code></pre>
     */
    private static class RewriteContextGroupElement implements LocalGroupElement {

        private final ParserRuleContext parserRuleContext;

        private final TokenStreamRewriter rewriter;

        private RewriteContextGroupElement(
                ParserRuleContext parserRuleContext, TokenStreamRewriter rewriter) {
            this.parserRuleContext = parserRuleContext;
            this.rewriter = rewriter;
        }

        @Override
        public Token getStart() {
            return this.parserRuleContext.start;
        }

        @Override
        public Token getStop() {
            return this.parserRuleContext.stop;
        }

        @Override
        public String getBody() {
            return this.rewriter.getText();
        }

        @Override
        public ParserRuleContext getContext() {
            return this.parserRuleContext;
        }
    }

    /**
     * This object represents a rewritten code block. It contains its new form along with all
     * extracted groups and their names.
     */
    public static class RewriteGroupedCode {

        /** Rewritten code block containing calls to extracted methods. */
        private final String rewriteCode;

        /** All extracted groups with their names. */
        private final Map<String, List<String>> groups;

        public RewriteGroupedCode(String rewriteCode, Map<String, List<String>> groups) {
            this.rewriteCode = rewriteCode;
            this.groups = groups;
        }

        public String getRewriteCode() {
            return rewriteCode;
        }

        public Map<String, List<String>> getGroups() {
            return this.groups;
        }
    }
}
