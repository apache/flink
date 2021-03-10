/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.delegation.hive.parse;

import org.apache.flink.table.planner.delegation.hive.HiveParserASTBuilder;
import org.apache.flink.table.planner.delegation.hive.HiveParserContext;
import org.apache.flink.util.Preconditions;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.PTFUtils;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/** Counterpart of hive's org.apache.hadoop.hive.ql.parse.ParseUtils. */
public class HiveASTParseUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HiveASTParseUtils.class);

    private HiveASTParseUtils() {}

    /** Parses the Hive query. */
    public static ASTNode parse(String command, HiveParserContext ctx)
            throws HiveASTParseException {
        return parse(command, ctx, null);
    }

    /** Parses the Hive query. */
    public static ASTNode parse(
            String command, HiveParserContext ctx, String viewFullyQualifiedName)
            throws HiveASTParseException {
        HiveASTParseDriver pd = new HiveASTParseDriver();
        ASTNode tree = pd.parse(command, ctx, viewFullyQualifiedName);
        tree = findRootNonNullToken(tree);
        handleSetColRefs(tree);
        return tree;
    }

    private static ASTNode findRootNonNullToken(ASTNode tree) {
        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }
        return tree;
    }

    private static void handleSetColRefs(ASTNode tree) {
        ASTSearcher astSearcher = new ASTSearcher();
        while (true) {
            astSearcher.reset();
            ASTNode setCols = astSearcher.depthFirstSearch(tree, HiveASTParser.TOK_SETCOLREF);
            if (setCols == null) {
                break;
            }
            processSetColsNode(setCols, astSearcher);
        }
    }

    private static void processSetColsNode(ASTNode setCols, ASTSearcher searcher) {
        searcher.reset();
        CommonTree rootNode = setCols;
        while (rootNode != null && rootNode.getType() != HiveASTParser.TOK_INSERT) {
            rootNode = rootNode.parent;
        }
        if (rootNode == null || rootNode.parent == null) {
            // Couldn't find the parent insert; replace with ALLCOLREF.
            LOG.debug(
                    "Replacing SETCOLREF with ALLCOLREF because we couldn't find the root INSERT");
            setCols.token.setType(HiveASTParser.TOK_ALLCOLREF);
            return;
        }
        rootNode = rootNode.parent; // TOK_QUERY above insert
        Tree fromNode = null;
        for (int j = 0; j < rootNode.getChildCount(); ++j) {
            Tree child = rootNode.getChild(j);
            if (child.getType() == HiveASTParser.TOK_FROM) {
                fromNode = child;
                break;
            }
        }
        if (!(fromNode instanceof ASTNode)) {
            // Couldn't find the from that contains subquery; replace with ALLCOLREF.
            LOG.debug("Replacing SETCOLREF with ALLCOLREF because we couldn't find the FROM");
            setCols.token.setType(HiveASTParser.TOK_ALLCOLREF);
            return;
        }
        // We are making what we are trying to do more explicit if there's a union alias; so
        // that if we do something we didn't expect to do, it'd be more likely to fail.
        String alias = null;
        if (fromNode.getChildCount() > 0) {
            Tree fromWhat = fromNode.getChild(0);
            if (fromWhat.getType() == HiveASTParser.TOK_SUBQUERY && fromWhat.getChildCount() > 1) {
                Tree child = fromWhat.getChild(fromWhat.getChildCount() - 1);
                if (child.getType() == HiveASTParser.Identifier) {
                    alias = child.getText();
                }
            }
        }
        // Note: we assume that this isn't an already malformed query;
        //       we don't check for that here - it will fail later anyway.
        // First, we find the SELECT closest to the top.
        ASTNode select =
                searcher.simpleBreadthFirstSearchAny(
                        (ASTNode) fromNode, HiveASTParser.TOK_SELECT, HiveASTParser.TOK_SELECTDI);
        if (select == null) {
            // Couldn't find the from that contains subquery; replace with ALLCOLREF.
            LOG.debug("Replacing SETCOLREF with ALLCOLREF because we couldn't find the SELECT");
            setCols.token.setType(HiveASTParser.TOK_ALLCOLREF);
            return;
        }

        // Then, find the leftmost logical sibling select, because that's what Hive uses for
        // aliases.
        while (true) {
            CommonTree queryOfSelect = select.parent;
            while (queryOfSelect != null && queryOfSelect.getType() != HiveASTParser.TOK_QUERY) {
                queryOfSelect = queryOfSelect.parent;
            }
            // We should have some QUERY; and also its parent because by supposition we are in subq.
            if (queryOfSelect == null || queryOfSelect.parent == null) {
                LOG.debug("Replacing SETCOLREF with ALLCOLREF because we couldn't find the QUERY");
                setCols.token.setType(HiveASTParser.TOK_ALLCOLREF);
                return;
            }
            if (queryOfSelect.childIndex == 0) {
                break; // We are the left-most child.
            }
            Tree moreToTheLeft = queryOfSelect.parent.getChild(0);
            Preconditions.checkState(moreToTheLeft != queryOfSelect);
            ASTNode newSelect =
                    searcher.simpleBreadthFirstSearchAny(
                            (ASTNode) moreToTheLeft,
                            HiveASTParser.TOK_SELECT,
                            HiveASTParser.TOK_SELECTDI);
            Preconditions.checkState(newSelect != select);
            select = newSelect;
            // Repeat the procedure for the new select.
        }

        // Found the proper columns.
        List<ASTNode> newChildren = new ArrayList<>(select.getChildCount());
        HashSet<String> aliases = new HashSet<>();
        for (int i = 0; i < select.getChildCount(); ++i) {
            Tree selExpr = select.getChild(i);
            assert selExpr.getType() == HiveASTParser.TOK_SELEXPR;
            assert selExpr.getChildCount() > 0;
            // Examine the last child. It could be an alias.
            Tree child = selExpr.getChild(selExpr.getChildCount() - 1);
            switch (child.getType()) {
                case HiveASTParser.TOK_SETCOLREF:
                    // We have a nested setcolref. Process that and start from scratch TODO: use
                    // stack?
                    processSetColsNode((ASTNode) child, searcher);
                    processSetColsNode(setCols, searcher);
                    return;
                case HiveASTParser.TOK_ALLCOLREF:
                    // We should find an alias of this insert and do (alias).*. This however won't
                    // fix e.g.
                    // positional order by alias case, cause we'd still have a star on the top
                    // level. Bail.
                    LOG.debug("Replacing SETCOLREF with ALLCOLREF because of nested ALLCOLREF");
                    setCols.token.setType(HiveASTParser.TOK_ALLCOLREF);
                    return;
                case HiveASTParser.TOK_TABLE_OR_COL:
                    Tree idChild = child.getChild(0);
                    assert idChild.getType() == HiveASTParser.Identifier : idChild;
                    if (!createChildColumnRef(idChild, alias, newChildren, aliases)) {
                        setCols.token.setType(HiveASTParser.TOK_ALLCOLREF);
                        return;
                    }
                    break;
                case HiveASTParser.Identifier:
                    if (!createChildColumnRef(child, alias, newChildren, aliases)) {
                        setCols.token.setType(HiveASTParser.TOK_ALLCOLREF);
                        return;
                    }
                    break;
                case HiveASTParser.DOT:
                    {
                        Tree colChild = child.getChild(child.getChildCount() - 1);
                        assert colChild.getType() == HiveASTParser.Identifier : colChild;
                        if (!createChildColumnRef(colChild, alias, newChildren, aliases)) {
                            setCols.token.setType(HiveASTParser.TOK_ALLCOLREF);
                            return;
                        }
                        break;
                    }
                default:
                    // Not really sure how to refer to this (or if we can).
                    // TODO: We could find a different from branch for the union, that might have an
                    // alias?
                    //       Or we could add an alias here to refer to, but that might break other
                    // branches.
                    LOG.debug(
                            "Replacing SETCOLREF with ALLCOLREF because of the nested node "
                                    + child.getType()
                                    + " "
                                    + child.getText());
                    setCols.token.setType(HiveASTParser.TOK_ALLCOLREF);
                    return;
            }
        }
        // Insert search in the beginning would have failed if these parents didn't exist.
        ASTNode parent = (ASTNode) setCols.parent.parent;
        int t = parent.getType();
        assert t == HiveASTParser.TOK_SELECT || t == HiveASTParser.TOK_SELECTDI : t;
        int ix = setCols.parent.childIndex;
        parent.deleteChild(ix);
        for (ASTNode node : newChildren) {
            parent.insertChild(ix++, node);
        }
    }

    private static boolean createChildColumnRef(
            Tree child, String alias, List<ASTNode> newChildren, HashSet<String> aliases) {
        String colAlias = child.getText();
        if (!aliases.add(colAlias)) {
            // TODO: if a side of the union has 2 columns with the same name, noone on the higher
            //       level can refer to them. We could change the alias in the original node.
            LOG.debug("Replacing SETCOLREF with ALLCOLREF because of duplicate alias " + colAlias);
            return false;
        }
        HiveParserASTBuilder selExpr =
                HiveParserASTBuilder.construct(HiveASTParser.TOK_SELEXPR, "TOK_SELEXPR");
        HiveParserASTBuilder toc =
                HiveParserASTBuilder.construct(HiveASTParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL");
        HiveParserASTBuilder id =
                HiveParserASTBuilder.construct(HiveASTParser.Identifier, colAlias);
        if (alias == null) {
            selExpr = selExpr.add(toc.add(id));
        } else {
            HiveParserASTBuilder dot = HiveParserASTBuilder.construct(HiveASTParser.DOT, ".");
            HiveParserASTBuilder aliasNode =
                    HiveParserASTBuilder.construct(HiveASTParser.Identifier, alias);
            selExpr = selExpr.add(dot.add(toc.add(aliasNode)).add(id));
        }
        newChildren.add(selExpr.node());
        return true;
    }

    public static boolean containsTokenOfType(ASTNode root, Integer... tokens) {
        final Set<Integer> tokensToMatch = new HashSet<>(Arrays.asList(tokens));

        return containsTokenOfType(
                root,
                new PTFUtils.Predicate<ASTNode>() {
                    @Override
                    public boolean apply(ASTNode node) {
                        return tokensToMatch.contains(node.getType());
                    }
                });
    }

    private static boolean containsTokenOfType(
            ASTNode root, PTFUtils.Predicate<ASTNode> predicate) {
        Queue<ASTNode> queue = new ArrayDeque<>();

        // BFS
        queue.add(root);
        while (!queue.isEmpty()) {
            ASTNode current = queue.remove();
            // If the predicate matches, then return true.
            // Otherwise visit the next set of nodes that haven't been seen.
            if (predicate.apply(current)) {
                return true;
            } else {
                // Guard because ASTNode.getChildren.iterator returns null if no children available
                // (bug).
                if (current.getChildCount() > 0) {
                    for (Node child : current.getChildren()) {
                        queue.add((ASTNode) child);
                    }
                }
            }
        }

        return false;
    }

    /** ASTSearcher. */
    private static class ASTSearcher {
        private final LinkedList<ASTNode> searchQueue = new LinkedList<>();

        public ASTNode depthFirstSearch(ASTNode ast, int token) {
            searchQueue.clear();
            searchQueue.add(ast);
            while (!searchQueue.isEmpty()) {
                ASTNode next = searchQueue.poll();
                if (next.getType() == token) {
                    return next;
                }
                for (int j = 0; j < next.getChildCount(); ++j) {
                    searchQueue.add((ASTNode) next.getChild(j));
                }
            }
            return null;
        }

        public ASTNode simpleBreadthFirstSearchAny(ASTNode ast, int... tokens) {
            searchQueue.clear();
            searchQueue.add(ast);
            while (!searchQueue.isEmpty()) {
                ASTNode next = searchQueue.poll();
                for (int i = 0; i < tokens.length; ++i) {
                    if (next.getType() == tokens[i]) {
                        return next;
                    }
                }
                for (int i = 0; i < next.getChildCount(); ++i) {
                    searchQueue.add((ASTNode) next.getChild(i));
                }
            }
            return null;
        }

        public void reset() {
            searchQueue.clear();
        }
    }

    public static CharTypeInfo getCharTypeInfo(ASTNode node) throws SemanticException {
        if (node.getChildCount() != 1) {
            throw new SemanticException("Bad params for type char");
        }

        String lengthStr = node.getChild(0).getText();
        return TypeInfoFactory.getCharTypeInfo(Integer.parseInt(lengthStr));
    }

    public static VarcharTypeInfo getVarcharTypeInfo(ASTNode node) throws SemanticException {
        if (node.getChildCount() != 1) {
            throw new SemanticException("Bad params for type varchar");
        }

        String lengthStr = node.getChild(0).getText();
        return TypeInfoFactory.getVarcharTypeInfo(Integer.parseInt(lengthStr));
    }

    public static DecimalTypeInfo getDecimalTypeTypeInfo(ASTNode node) throws SemanticException {
        if (node.getChildCount() > 2) {
            throw new SemanticException("Bad params for type decimal");
        }

        int precision = HiveDecimal.USER_DEFAULT_PRECISION;
        int scale = HiveDecimal.USER_DEFAULT_SCALE;

        if (node.getChildCount() >= 1) {
            String precStr = node.getChild(0).getText();
            precision = Integer.parseInt(precStr);
        }

        if (node.getChildCount() == 2) {
            String scaleStr = node.getChild(1).getText();
            scale = Integer.parseInt(scaleStr);
        }

        return TypeInfoFactory.getDecimalTypeInfo(precision, scale);
    }
}
