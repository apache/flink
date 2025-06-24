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
import org.apache.flink.util.Preconditions;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.atn.PredictionMode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Extract statements from IFs, ELSEs and WHILEs blocks making them smaller.
 *
 * <p>BlockStatementSplitter does not recognize if statement operates on local of class member
 * variable. Because of that, code must be preprocessed by {@link DeclarationRewriter} which
 * converts all local variables extracted as to member variables.
 *
 * <p><i>Before</i>
 *
 * <pre><code>
 * while (counter > 0) {
 *     int localA = a + 1000;
 *     System.out.println(localA);
 *     if (a > 0) {
 *         b = a * 2;
 *         c = b * 2;
 *         System.out.println(b);
 *     } else {
 *         b = a * 3;
 *         System.out.println(b);
 *     }
 *     counter--;
 * }
 *
 * </code></pre>
 *
 * <p><i>After</i>
 *
 * <pre><code>
 * while (counter > 0) {
 *     myFun_0_1(a, b, c);
 *     if (a > 0) {
 *         myFun_0_1_2(a, b, c);
 *     } else {
 *         myFun_0_1_3(a, b, c);
 *     }
 *     counter--;
 * }
 * </code></pre>
 *
 * <p>Where bodies of extracted "methods" are:
 *
 * <pre><code>
 * myFun_0_1(int a, int b, int c) ->
 *     int localA = a + 1000;
 *     System.out.println(localA);
 * </code></pre>
 *
 * <pre><code>
 * myFun_0_1_3(int a, int b, int c) ->
 *     b = a * 2;
 *     c = b * 2;
 *     System.out.println(b);
 * </code></pre>
 *
 * <pre><code>
 * myFun_whileBody0_0_ifBody1(int a) ->
 *     b = a * 3;
 *     System.out.println(b);
 * </code></pre>
 */
@Internal
public class BlockStatementSplitter {

    private final String code;

    private final String parameters;

    private BlockStatementVisitor visitor;

    /**
     * Initialize new BlockStatementSplitter.
     *
     * @param code a code block that should be rewritten.
     * @param parameters parameters definition that should be used for extracted methods.
     */
    public BlockStatementSplitter(String code, String parameters) {
        this.code = code;
        this.parameters = parameters;
    }

    /**
     * Rewrite code block that was used for this object initialization.
     *
     * @param context prefix for extracted blocks.
     * @return a map which key represent rewritten block name and value represents rewritten code
     *     block, including calls to extracted methods
     */
    public String rewriteBlock(String context) {

        this.visitor = new BlockStatementVisitor(code, parameters);
        JavaParser javaParser = new JavaParser(visitor.tokenStream);
        javaParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        visitor.visitStatement(javaParser.statement(), context);
        visitor.rewrite();
        return visitor.rewriter.getText();
    }

    /**
     * This method extracts statements from IFs, ELSE's and WHILE blocks from block code used during
     * initialization of this object. Every entry of returned map can be seen as new method name
     * (map key) and method's body (map value). The block names will be prefixed with provided
     * context.
     *
     * @return a map of block name to block statements mappings. The key can be interpreted as name
     *     of extracted block/method and corresponding List represents individual statements (block'
     *     lines) for this block.
     */
    public Map<String, List<String>> extractBlocks() {

        Map<String, List<String>> allBlocks =
                CollectionUtil.newHashMapWithExpectedSize(visitor.blocks.size());

        for (Entry<String, List<ParserRuleContext>> entry : visitor.blocks.entrySet()) {

            List<String> blocks =
                    entry.getValue().stream()
                            .map(CodeSplitUtil::getContextString)
                            .collect(Collectors.toList());
            allBlocks.put(entry.getKey(), blocks);
        }

        return allBlocks;
    }

    private static class BlockStatementVisitor {

        private final Map<String, List<ParserRuleContext>> blocks = new HashMap<>();

        private final CommonTokenStream tokenStream;

        private final TokenStreamRewriter rewriter;

        private final String parameters;

        private int counter = 0;

        private BlockStatementVisitor(String code, String parameters) {
            this.tokenStream = new CommonTokenStream(new JavaLexer(CharStreams.fromString(code)));
            this.rewriter = new TokenStreamRewriter(tokenStream);
            this.parameters = parameters;
        }

        public void visitStatement(StatementContext ctx, String context) {

            if (ctx.getChildCount() == 0 || getNumOfReturnOrJumpStatements(ctx) != 0) {
                return;
            }

            if (ctx.block() == null) {
                for (StatementContext statementContext : ctx.statement()) {
                    String localContext = String.format("%s_%d", context, counter++);
                    visitStatement(statementContext, localContext);
                }
            } else {
                List<ParserRuleContext> extractedSingleBlocks = new ArrayList<>();
                for (BlockStatementContext bsc : ctx.block().blockStatement()) {
                    if (bsc.statement() != null
                            && (bsc.statement().IF() != null
                                    || bsc.statement().ELSE() != null
                                    || bsc.statement().WHILE() != null)) {

                        String localContext = String.format("%s_%d", context, counter++);

                        tryGroupAsSingleStatement(extractedSingleBlocks, localContext);

                        extractedSingleBlocks = new ArrayList<>();
                        visitStatement(bsc.statement(), localContext);
                    } else {
                        extractedSingleBlocks.add(bsc);
                    }
                }

                tryGroupAsSingleStatement(extractedSingleBlocks, context);
            }
        }

        private void tryGroupAsSingleStatement(
                List<ParserRuleContext> extractedSingleBlocks, String context) {
            // if there is only one statement in the block, and it is not IF/ELSE/WHILE
            // statement it's pointless to extract it into a separate function.
            if (canGroupAsSingleStatement(extractedSingleBlocks)) {
                List<ParserRuleContext> previous = blocks.put(context, extractedSingleBlocks);
                Preconditions.checkState(
                        previous == null,
                        String.format(
                                "Overriding extracted block %s - this should not happen.",
                                context));
            }
        }

        private void rewrite() {

            for (Entry<String, List<ParserRuleContext>> entry : blocks.entrySet()) {
                List<ParserRuleContext> statements = entry.getValue();
                String statementContext = entry.getKey();
                // if there is only one statement in the block, and it is not IF/ELSE/WHILE
                // statement
                // it's pointless to extract it into a separate function.
                if (statements.size() > 1
                        || (statements.size() == 1
                                && canGroupAsSingleStatement(statements.get(0)))) {
                    rewriter.replace(
                            statements.get(0).start,
                            statements.get(statements.size() - 1).stop,
                            statementContext + "(" + parameters + ");");
                }
            }
        }

        private boolean canGroupAsSingleStatement(List<ParserRuleContext> extractedSingleBlocks) {
            return extractedSingleBlocks.size() > 1
                    || (extractedSingleBlocks.size() == 1
                            && canGroupAsSingleStatement(extractedSingleBlocks.get(0)));
        }

        private boolean canGroupAsSingleStatement(ParserRuleContext parserRuleContext) {

            StatementContext statement;

            if (parserRuleContext instanceof StatementContext) {
                statement = (StatementContext) parserRuleContext;
            } else if (parserRuleContext instanceof BlockStatementContext) {
                statement = ((BlockStatementContext) parserRuleContext).statement();
            } else {
                return false;
            }

            return statement != null
                    && (statement.IF() != null
                            || statement.ELSE() != null
                            || statement.WHILE() != null);
        }

        private int getNumOfReturnOrJumpStatements(ParserRuleContext ctx) {
            ReturnAndJumpCounter counter = new ReturnAndJumpCounter();
            counter.visit(ctx);
            return counter.getCounter();
        }
    }
}
