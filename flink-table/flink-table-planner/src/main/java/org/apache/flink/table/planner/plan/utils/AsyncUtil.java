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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.planner.plan.rules.logical.RemoteCallFinder;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

/** Utility class for working with async function calls in RexNodes. */
public class AsyncUtil {

    /**
     * Checks whether it contains the specified kind of async function call in the specified node.
     *
     * @param node the RexNode to check
     * @return true if it contains an async function call in the specified node.
     */
    public static boolean containsAsyncCall(RexNode node, FunctionKind functionKind) {
        Preconditions.checkArgument(
                functionKind == FunctionKind.ASYNC_SCALAR
                        || functionKind == FunctionKind.ASYNC_TABLE);
        return node.accept(new FunctionFinder(true, true, functionKind));
    }

    /** Variant that checks for any async call. */
    public static boolean containsAsyncCall(RexNode node) {
        return node.accept(new FunctionFinder(true, true, null));
    }

    /**
     * Checks whether it contains a function call in the node that is not the async kind specified.
     *
     * @param node the RexNode to check
     * @return true if it contains a non-async function call in the specified node.
     */
    public static boolean containsNonAsyncCall(RexNode node, FunctionKind functionKind) {
        Preconditions.checkArgument(
                functionKind == FunctionKind.ASYNC_SCALAR
                        || functionKind == FunctionKind.ASYNC_TABLE);
        return node.accept(new FunctionFinder(false, true, functionKind));
    }

    /** Variant that checks that it contains a function call not of any async type. */
    public static boolean containsNonAsyncCall(RexNode node) {
        return node.accept(new FunctionFinder(false, true, null));
    }

    /**
     * Checks whether the specified node is the specified kind of async function call.
     *
     * @param node the RexNode to check
     * @return true if the specified node is an async function call.
     */
    public static boolean isAsyncCall(RexNode node, FunctionKind functionKind) {
        Preconditions.checkArgument(
                functionKind == FunctionKind.ASYNC_SCALAR
                        || functionKind == FunctionKind.ASYNC_TABLE);
        return node.accept(new FunctionFinder(true, false, functionKind));
    }

    /** Variant that checks for any async call. */
    public static boolean isAsyncCall(RexNode node) {
        return node.accept(new FunctionFinder(true, false, null));
    }

    /**
     * Checks whether the node is a function call, not of the specified async type.
     *
     * @param node the RexNode to check
     * @return true if the specified node is a non-async function call.
     */
    public static boolean isNonAsyncCall(RexNode node, FunctionKind functionKind) {
        Preconditions.checkArgument(
                functionKind == FunctionKind.ASYNC_SCALAR
                        || functionKind == FunctionKind.ASYNC_TABLE);
        return node.accept(new FunctionFinder(false, false, functionKind));
    }

    /** Variant that checks that it is a function call not of any async type. */
    public static boolean isNonAsyncCall(RexNode node) {
        return node.accept(new FunctionFinder(false, false, null));
    }

    private static class FunctionFinder extends RexDefaultVisitor<Boolean> {

        private final boolean findAsyncCall;
        private final boolean recursive;
        private final FunctionKind functionKind;

        public FunctionFinder(boolean findAsyncCall, boolean recursive, FunctionKind functionKind) {
            this.findAsyncCall = findAsyncCall;
            this.recursive = recursive;
            this.functionKind = functionKind;
        }

        @Override
        public Boolean visitNode(RexNode rexNode) {
            return false;
        }

        private boolean isImmediateAsyncCall(RexCall call) {
            FunctionDefinition definition = ShortcutUtils.unwrapFunctionDefinition(call);
            return definition != null
                    && ((functionKind != null && definition.getKind() == functionKind)
                            || (functionKind == null
                                    && (definition.getKind() == FunctionKind.ASYNC_SCALAR
                                            || definition.getKind() == FunctionKind.ASYNC_TABLE)));
        }

        @Override
        public Boolean visitCall(RexCall call) {
            boolean isImmediateAsyncCall = isImmediateAsyncCall(call);
            return findAsyncCall == isImmediateAsyncCall
                    || (recursive
                            && call.getOperands().stream().anyMatch(node -> node.accept(this)));
        }
    }

    /**
     * An Async implementation of {@link RemoteCallFinder} which finds uses of {@link
     * org.apache.flink.table.functions.AsyncScalarFunction} and {@link
     * org.apache.flink.table.functions.AsyncTableFunction}.
     */
    public static class AsyncRemoteCallFinder implements RemoteCallFinder {

        private final FunctionKind functionKind;

        public AsyncRemoteCallFinder(FunctionKind functionKind) {
            this.functionKind = functionKind;
        }

        @Override
        public boolean containsRemoteCall(RexNode node) {
            return containsAsyncCall(node, functionKind);
        }

        @Override
        public boolean containsNonRemoteCall(RexNode node) {
            return containsNonAsyncCall(node, functionKind);
        }

        @Override
        public boolean isRemoteCall(RexNode node) {
            return isAsyncCall(node, functionKind);
        }

        @Override
        public boolean isNonRemoteCall(RexNode node) {
            return isNonAsyncCall(node, functionKind);
        }

        @Override
        public String getName() {
            return "Async";
        }

        @Override
        public boolean equals(Object obj) {
            return obj != null
                    && this.getClass() == obj.getClass()
                    && functionKind == ((AsyncRemoteCallFinder) obj).functionKind;
        }

        @Override
        public int hashCode() {
            return this.getClass().hashCode();
        }
    }
}
