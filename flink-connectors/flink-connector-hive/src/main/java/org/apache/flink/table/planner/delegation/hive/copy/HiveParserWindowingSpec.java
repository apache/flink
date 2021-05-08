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

package org.apache.flink.table.planner.delegation.hive.copy;

import org.apache.flink.table.planner.delegation.hive.copy.HiveParserPTFInvocationSpec.OrderSpec;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserPTFInvocationSpec.PartitionExpression;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserPTFInvocationSpec.PartitionSpec;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserPTFInvocationSpec.PartitioningSpec;
import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParser;

import org.antlr.runtime.CommonToken;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.WindowFunctionInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;

import java.util.ArrayList;
import java.util.HashMap;

/** Counterpart of hive's org.apache.hadoop.hive.ql.parse.WindowingSpec. */
public class HiveParserWindowingSpec {

    private final ArrayList<WindowExpressionSpec> windowExpressions = new ArrayList<>();
    private final HashMap<String, WindowSpec> windowSpecs = new HashMap<>();

    public void addWindowSpec(String name, WindowSpec wdwSpec) {
        windowSpecs.put(name, wdwSpec);
    }

    public void addWindowFunction(WindowFunctionSpec wFn) {
        windowExpressions.add(wFn);
    }

    public ArrayList<WindowExpressionSpec> getWindowExpressions() {
        return windowExpressions;
    }

    public HashMap<String, WindowSpec> getWindowSpecs() {
        return windowSpecs;
    }

    public void validateAndMakeEffective() throws SemanticException {
        for (WindowExpressionSpec expr : getWindowExpressions()) {
            WindowFunctionSpec wFn = (WindowFunctionSpec) expr;
            WindowSpec wdwSpec = wFn.getWindowSpec();

            // 1. For Wdw Specs that refer to Window Defns, inherit missing components
            if (wdwSpec != null) {
                ArrayList<String> sources = new ArrayList<>();
                fillInWindowSpec(wdwSpec.getSourceId(), wdwSpec, sources);
            }

            if (wdwSpec == null) {
                wdwSpec = new WindowSpec();
                wFn.setWindowSpec(wdwSpec);
            }

            // 2. A Window Spec with no Parition Spec, is Partitioned on a Constant(number 0)
            applyConstantPartition(wdwSpec);

            // 3. For missing Wdw Frames or for Frames with only a Start Boundary, completely
            //    specify them by the rules in {@link effectiveWindowFrame}
            effectiveWindowFrame(wFn);

            // 4. Validate the effective Window Frames with the rules in {@link validateWindowFrame}
            validateWindowFrame(wdwSpec);

            // 5. Add the Partition expressions as the Order if there is no Order and validate Order
            // spec.
            setAndValidateOrderSpec(wFn);
        }
    }

    private void setAndValidateOrderSpec(WindowFunctionSpec wFn) throws SemanticException {
        WindowSpec wdwSpec = wFn.getWindowSpec();
        wdwSpec.ensureOrderSpec(wFn);
        WindowFrameSpec wFrame = wdwSpec.getWindowFrame();
        OrderSpec order = wdwSpec.getOrder();

        BoundarySpec start = wFrame.getStart();
        BoundarySpec end = wFrame.getEnd();

        if (wFrame.getWindowType() == WindowType.RANGE) {
            if (order == null || order.getExpressions().size() == 0) {
                throw new SemanticException(
                        "Range based Window Frame needs to specify ORDER BY clause");
            }

            boolean currentRange =
                    start.getDirection() == Direction.CURRENT
                            && end.getDirection() == Direction.CURRENT;
            boolean defaultPreceding =
                    start.getDirection() == Direction.PRECEDING
                            && start.getAmt() == BoundarySpec.UNBOUNDED_AMOUNT
                            && end.getDirection() == Direction.CURRENT;
            boolean defaultFollowing =
                    start.getDirection() == Direction.CURRENT
                            && end.getDirection() == Direction.FOLLOWING
                            && end.getAmt() == BoundarySpec.UNBOUNDED_AMOUNT;
            boolean defaultPrecedingFollowing =
                    start.getDirection() == Direction.PRECEDING
                            && start.getAmt() == BoundarySpec.UNBOUNDED_AMOUNT
                            && end.getDirection() == Direction.FOLLOWING
                            && end.getAmt() == BoundarySpec.UNBOUNDED_AMOUNT;
            boolean multiOrderAllowed =
                    currentRange
                            || defaultPreceding
                            || defaultFollowing
                            || defaultPrecedingFollowing;
            if (order.getExpressions().size() != 1 && !multiOrderAllowed) {
                throw new SemanticException(
                        "Range value based Window Frame can have only 1 Sort Key");
            }
        }
    }

    private void validateWindowFrame(WindowSpec wdwSpec) throws SemanticException {
        WindowFrameSpec wFrame = wdwSpec.getWindowFrame();
        BoundarySpec start = wFrame.getStart();
        BoundarySpec end = wFrame.getEnd();

        if (start.getDirection() == Direction.FOLLOWING
                && start.getAmt() == BoundarySpec.UNBOUNDED_AMOUNT) {
            throw new SemanticException("Start of a WindowFrame cannot be UNBOUNDED FOLLOWING");
        }

        if (end.getDirection() == Direction.PRECEDING
                && end.getAmt() == BoundarySpec.UNBOUNDED_AMOUNT) {
            throw new SemanticException("End of a WindowFrame cannot be UNBOUNDED PRECEDING");
        }
    }

    private void effectiveWindowFrame(WindowFunctionSpec wFn) throws SemanticException {
        WindowSpec wdwSpec = wFn.getWindowSpec();
        WindowFunctionInfo wFnInfo = FunctionRegistry.getWindowFunctionInfo(wFn.getName());
        boolean supportsWindowing = wFnInfo == null || wFnInfo.isSupportsWindow();
        WindowFrameSpec wFrame = wdwSpec.getWindowFrame();
        OrderSpec orderSpec = wdwSpec.getOrder();
        if (wFrame == null) {
            if (!supportsWindowing) {
                if (wFn.getName().toLowerCase().equals(FunctionRegistry.LAST_VALUE_FUNC_NAME)
                        && orderSpec != null) {
                    /*
                     * last_value: when an Sort Key is specified, then last_value should return the
                     * last value among rows with the same Sort Key value.
                     */
                    wFrame =
                            new WindowFrameSpec(
                                    WindowType.ROWS,
                                    new BoundarySpec(Direction.CURRENT),
                                    new BoundarySpec(Direction.FOLLOWING, 0));
                } else {
                    wFrame =
                            new WindowFrameSpec(
                                    WindowType.ROWS,
                                    new BoundarySpec(
                                            Direction.PRECEDING, BoundarySpec.UNBOUNDED_AMOUNT),
                                    new BoundarySpec(
                                            Direction.FOLLOWING, BoundarySpec.UNBOUNDED_AMOUNT));
                }
            } else {
                if (orderSpec == null) {
                    wFrame =
                            new WindowFrameSpec(
                                    WindowType.ROWS,
                                    new BoundarySpec(
                                            Direction.PRECEDING, BoundarySpec.UNBOUNDED_AMOUNT),
                                    new BoundarySpec(
                                            Direction.FOLLOWING, BoundarySpec.UNBOUNDED_AMOUNT));
                } else {
                    wFrame =
                            new WindowFrameSpec(
                                    WindowType.RANGE,
                                    new BoundarySpec(
                                            Direction.PRECEDING, BoundarySpec.UNBOUNDED_AMOUNT),
                                    new BoundarySpec(Direction.CURRENT));
                }
            }

            wdwSpec.setWindowFrame(wFrame);
        } else if (wFrame.getEnd() == null) {
            wFrame.setEnd(new BoundarySpec(Direction.CURRENT));
        }
    }

    private void applyConstantPartition(WindowSpec wdwSpec) {
        PartitionSpec partSpec = wdwSpec.getPartition();
        if (partSpec == null) {
            partSpec = new PartitionSpec();
            PartitionExpression partExpr = new PartitionExpression();
            partExpr.setExpression(
                    new HiveParserASTNode(new CommonToken(HiveASTParser.Number, "0")));
            partSpec.addExpression(partExpr);
            wdwSpec.setPartition(partSpec);
        }
    }

    private void fillInWindowSpec(String sourceId, WindowSpec dest, ArrayList<String> visited)
            throws SemanticException {
        if (sourceId != null) {
            if (visited.contains(sourceId)) {
                visited.add(sourceId);
                throw new SemanticException(
                        String.format("Cycle in Window references %s", visited));
            }
            WindowSpec source = getWindowSpecs().get(sourceId);
            if (source == null || source.equals(dest)) {
                throw new SemanticException(String.format("%s refers to an unknown source", dest));
            }

            if (dest.getPartition() == null) {
                dest.setPartition(source.getPartition());
            }

            if (dest.getOrder() == null) {
                dest.setOrder(source.getOrder());
            }

            if (dest.getWindowFrame() == null) {
                dest.setWindowFrame(source.getWindowFrame());
            }

            visited.add(sourceId);

            fillInWindowSpec(source.getSourceId(), dest, visited);
        }
    }

    /** WindowType. */
    public enum WindowType {
        ROWS,
        RANGE
    }

    /** BoundarySpec. */
    public static class BoundarySpec implements Comparable<BoundarySpec> {
        public static final int UNBOUNDED_AMOUNT = Integer.MAX_VALUE;

        Direction direction;
        int amt;

        public BoundarySpec() {}

        public BoundarySpec(Direction direction) {
            this(direction, 0);
        }

        public BoundarySpec(Direction direction, int amt) {
            this.direction = direction;
            this.amt = amt;
        }

        public Direction getDirection() {
            return direction;
        }

        public void setDirection(Direction direction) {
            this.direction = direction;
        }

        public int getAmt() {
            return amt;
        }

        public void setAmt(int amt) {
            this.amt = amt;
        }

        @Override
        public String toString() {
            if (this.direction == Direction.CURRENT) {
                return "currentRow";
            }

            return String.format("%s %s", (amt == UNBOUNDED_AMOUNT ? "Unbounded" : amt), direction);
        }

        public int compareTo(BoundarySpec other) {
            int c = direction.compareTo(other.getDirection());
            if (c != 0) {
                return c;
            }

            // Valid range is "range/rows between 10 preceding and 2 preceding" for preceding case
            return this.direction == Direction.PRECEDING ? other.amt - amt : amt - other.amt;
        }
    }

    /** WindowSpec. */
    public static class WindowSpec {
        private String sourceId;
        private PartitioningSpec partitioning;
        private WindowFrameSpec windowFrame;

        public String getSourceId() {
            return sourceId;
        }

        public void setSourceId(String sourceId) {
            this.sourceId = sourceId;
        }

        public PartitioningSpec getPartitioning() {
            return partitioning;
        }

        public void setPartitioning(PartitioningSpec partitioning) {
            this.partitioning = partitioning;
        }

        public WindowFrameSpec getWindowFrame() {
            return windowFrame;
        }

        public void setWindowFrame(WindowFrameSpec windowFrame) {
            this.windowFrame = windowFrame;
        }

        public PartitionSpec getPartition() {
            return getPartitioning() == null ? null : getPartitioning().getPartSpec();
        }

        public void setPartition(PartitionSpec partSpec) {
            partitioning = partitioning == null ? new PartitioningSpec() : partitioning;
            partitioning.setPartSpec(partSpec);
        }

        public OrderSpec getOrder() {
            return getPartitioning() == null ? null : getPartitioning().getOrderSpec();
        }

        public void setOrder(OrderSpec orderSpec) {
            partitioning = partitioning == null ? new PartitioningSpec() : partitioning;
            partitioning.setOrderSpec(orderSpec);
        }

        /*
         * When there is no Order specified, we add the Partition expressions as
         * Order expressions. This is an implementation artifact. For UDAFS that
         * imply order (like rank, dense_rank) depend on the Order Expressions to
         * work. Internally we pass the Order Expressions as Args to these functions.
         * We could change the translation so that the Functions are setup with
         * Partition expressions when the OrderSpec is null; but for now we are setting up
         * an OrderSpec that copies the Partition expressions.
         */
        protected void ensureOrderSpec(WindowFunctionSpec wFn) throws SemanticException {
            if (getOrder() == null) {
                OrderSpec order = new OrderSpec();
                order.prefixBy(getPartition());
                setOrder(order);
            }
        }

        @Override
        public String toString() {
            return String.format(
                    "Window Spec=[%s%s%s]",
                    sourceId == null ? "" : "Name='" + sourceId + "'",
                    partitioning == null ? "" : partitioning,
                    windowFrame == null ? "" : windowFrame);
        }
    }

    /** WindowFrameSpec. */
    public static class WindowFrameSpec {
        private final WindowType windowType;
        private BoundarySpec start;
        private BoundarySpec end;

        public WindowFrameSpec(WindowType windowType, BoundarySpec start, BoundarySpec end) {
            this.windowType = windowType;
            this.start = start;
            this.end = end;
        }

        public BoundarySpec getStart() {
            return start;
        }

        public void setStart(BoundarySpec start) {
            this.start = start;
        }

        public BoundarySpec getEnd() {
            return end;
        }

        public void setEnd(BoundarySpec end) {
            this.end = end;
        }

        public WindowType getWindowType() {
            return this.windowType;
        }

        @Override
        public String toString() {
            return String.format("window(type=%s, start=%s, end=%s)", this.windowType, start, end);
        }
    }

    /** WindowExpressionSpec. */
    public static class WindowExpressionSpec {
        String alias;
        HiveParserASTNode expression;

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        public HiveParserASTNode getExpression() {
            return expression;
        }

        public void setExpression(HiveParserASTNode expression) {
            this.expression = expression;
        }
    }

    /** WindowFunctionSpec. */
    public static class WindowFunctionSpec extends WindowExpressionSpec {
        String name;
        boolean isStar;
        boolean isDistinct;
        ArrayList<HiveParserASTNode> args;
        WindowSpec windowSpec;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public boolean isStar() {
            return isStar;
        }

        public void setStar(boolean isStar) {
            this.isStar = isStar;
        }

        public boolean isDistinct() {
            return isDistinct;
        }

        public void setDistinct(boolean isDistinct) {
            this.isDistinct = isDistinct;
        }

        public ArrayList<HiveParserASTNode> getArgs() {
            args = args == null ? new ArrayList<HiveParserASTNode>() : args;
            return args;
        }

        public void setArgs(ArrayList<HiveParserASTNode> args) {
            this.args = args;
        }

        public void addArg(HiveParserASTNode arg) {
            args = args == null ? new ArrayList<HiveParserASTNode>() : args;
            args.add(arg);
        }

        public WindowSpec getWindowSpec() {
            return windowSpec;
        }

        public void setWindowSpec(WindowSpec windowSpec) {
            this.windowSpec = windowSpec;
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append(name).append("(");
            if (isStar) {
                buf.append("*");
            } else {
                if (isDistinct) {
                    buf.append("distinct ");
                }
                if (args != null) {
                    boolean first = true;
                    for (HiveParserASTNode arg : args) {
                        if (first) {
                            first = false;
                        } else {
                            buf.append(", ");
                        }
                        buf.append(arg.toStringTree());
                    }
                }
            }

            buf.append(")");

            if (windowSpec != null) {
                buf.append(" ").append(windowSpec.toString());
            }

            if (alias != null) {
                buf.append(" as ").append(alias);
            }

            return buf.toString();
        }
    }
}
