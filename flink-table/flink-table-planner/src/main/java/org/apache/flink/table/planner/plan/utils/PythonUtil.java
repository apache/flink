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

import org.apache.flink.table.functions.DeclarativeAggregateFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.table.functions.python.PythonFunctionKind;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.utils.AggSqlFunction;
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction;
import org.apache.flink.table.planner.functions.utils.TableSqlFunction;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.runtime.functions.aggregate.BuiltInAggregateFunction;

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Utility for Python. */
public class PythonUtil {

    /**
     * Checks whether it contains the specified kind of Python function call in the specified node.
     * If the parameter pythonFunctionKind is null, it will return true for any kind of Python
     * function.
     *
     * @param node the RexNode to check
     * @param pythonFunctionKind the kind of the python function
     * @return true if it contains the Python function call in the specified node.
     */
    public static boolean containsPythonCall(RexNode node, PythonFunctionKind pythonFunctionKind) {
        FunctionFinder functionFinder =
                new FunctionFinder(true, Optional.ofNullable(pythonFunctionKind), true);
        return node.accept(functionFinder);
    }

    public static boolean containsPythonCall(RexNode node) {
        return containsPythonCall(node, null);
    }

    /**
     * Checks whether it contains non-Python function call in the specified node.
     *
     * @param node the RexNode to check
     * @return true if it contains the non-Python function call in the specified node.
     */
    public static boolean containsNonPythonCall(RexNode node) {
        FunctionFinder functionFinder = new FunctionFinder(false, Optional.empty(), true);
        return node.accept(functionFinder);
    }

    /**
     * Checks whether the specified node is the specified kind of Python function call. If the
     * parameter pythonFunctionKind is null, it will return true for any kind of Python function.
     *
     * @param node the RexNode to check
     * @param pythonFunctionKind the kind of the python function
     * @return true if the specified node is a Python function call.
     */
    public static boolean isPythonCall(RexNode node, PythonFunctionKind pythonFunctionKind) {
        FunctionFinder functionFinder =
                new FunctionFinder(true, Optional.ofNullable(pythonFunctionKind), false);
        return node.accept(functionFinder);
    }

    public static boolean isPythonCall(RexNode node) {
        return isPythonCall(node, null);
    }

    /**
     * Checks whether the specified node is a non-Python function call.
     *
     * @param node the RexNode to check
     * @return true if the specified node is a non-Python function call.
     */
    public static boolean isNonPythonCall(RexNode node) {
        FunctionFinder functionFinder = new FunctionFinder(false, Optional.empty(), false);
        return node.accept(functionFinder);
    }

    public static boolean isPythonAggregate(AggregateCall call) {
        return isPythonAggregate(call, null);
    }

    /**
     * Checks whether the specified aggregate is the specified kind of Python function Aggregate.
     *
     * @param call the AggregateCall to check
     * @param pythonFunctionKind the kind of the python function
     * @return true if the specified call is a Python function Aggregate.
     */
    public static boolean isPythonAggregate(
            AggregateCall call, PythonFunctionKind pythonFunctionKind) {
        SqlAggFunction aggregation = call.getAggregation();
        if (aggregation instanceof AggSqlFunction) {
            return isPythonFunction(
                    ((AggSqlFunction) aggregation).aggregateFunction(), pythonFunctionKind);
        } else if (aggregation instanceof BridgingSqlAggFunction) {
            return isPythonFunction(
                    ((BridgingSqlAggFunction) aggregation).getDefinition(), pythonFunctionKind);
        } else {
            return false;
        }
    }

    public static boolean isBuiltInAggregate(AggregateCall call) {
        SqlAggFunction aggregation = call.getAggregation();
        if (aggregation instanceof AggSqlFunction) {
            AggSqlFunction aggSqlFunction = (AggSqlFunction) aggregation;
            return aggSqlFunction.aggregateFunction() instanceof BuiltInAggregateFunction;
        } else if (aggregation instanceof BridgingSqlAggFunction) {
            BridgingSqlAggFunction bridgingSqlAggFunction = (BridgingSqlAggFunction) aggregation;
            return bridgingSqlAggFunction.getDefinition() instanceof DeclarativeAggregateFunction;
        } else {
            return true;
        }
    }

    public static boolean takesRowAsInput(RexCall call) {
        if (call.getOperator() instanceof ScalarSqlFunction) {
            ScalarSqlFunction sfc = (ScalarSqlFunction) call.getOperator();
            return ((PythonFunction) sfc.scalarFunction()).takesRowAsInput();
        } else if (call.getOperator() instanceof TableSqlFunction) {
            TableSqlFunction tfc = (TableSqlFunction) call.getOperator();
            return ((PythonFunction) tfc.udtf()).takesRowAsInput();
        } else if (call.getOperator() instanceof BridgingSqlFunction) {
            BridgingSqlFunction bsf = (BridgingSqlFunction) call.getOperator();
            return ((PythonFunction) bsf.getDefinition()).takesRowAsInput();
        }
        return false;
    }

    private static boolean isPythonFunction(
            FunctionDefinition function, PythonFunctionKind pythonFunctionKind) {
        if (function instanceof PythonFunction) {
            PythonFunction pythonFunction = (PythonFunction) function;
            return pythonFunctionKind == null
                    || pythonFunction.getPythonFunctionKind() == pythonFunctionKind;
        } else {
            return false;
        }
    }

    public static boolean isFlattenCalc(FlinkLogicalCalc calc) {
        RelNode child = calc.getInput();
        if (child instanceof RelSubset) {
            child = ((RelSubset) child).getOriginal();
        } else if (child instanceof HepRelVertex) {
            child = ((HepRelVertex) child).getCurrentRel();
        } else {
            return false;
        }
        if (!(child instanceof FlinkLogicalCalc)) {
            return false;
        }

        if (calc.getProgram().getCondition() != null) {
            return false;
        }

        List<RelDataTypeField> inputFields = calc.getProgram().getInputRowType().getFieldList();
        if (inputFields.size() != 1 || !inputFields.get(0).getType().isStruct()) {
            return false;
        }

        List<RexNode> projects =
                calc.getProgram().getProjectList().stream()
                        .map(calc.getProgram()::expandLocalRef)
                        .collect(Collectors.toList());

        if (inputFields.get(0).getType().getFieldCount() != projects.size()) {
            return false;
        }

        return IntStream.range(0, projects.size())
                .allMatch(idx -> projects.get(idx).accept(new FieldReferenceDetector(idx)));
    }

    private static class FunctionFinder extends RexDefaultVisitor<Boolean> {
        private final boolean findPythonFunction;
        private final Optional<PythonFunctionKind> pythonFunctionKind;
        private final boolean recursive;

        /**
         * Checks whether it contains the specified kind of function in a RexNode.
         *
         * @param findPythonFunction true to find python function, false to find non-python function
         * @param pythonFunctionKind the kind of the python function
         * @param recursive whether check the inputs
         */
        public FunctionFinder(
                boolean findPythonFunction,
                Optional<PythonFunctionKind> pythonFunctionKind,
                boolean recursive) {
            this.findPythonFunction = findPythonFunction;
            this.pythonFunctionKind = pythonFunctionKind;
            this.recursive = recursive;
        }

        /**
         * Checks whether the specified rexCall is a python function call of the specified kind.
         *
         * @param rexCall the RexCall to check.
         * @return true if it is python function call of the specified kind.
         */
        private boolean isPythonRexCall(RexCall rexCall) {
            if (rexCall.getOperator() instanceof ScalarSqlFunction) {
                ScalarSqlFunction sfc = (ScalarSqlFunction) rexCall.getOperator();
                return isPythonFunction(sfc.scalarFunction());
            } else if (rexCall.getOperator() instanceof TableSqlFunction) {
                TableSqlFunction tfc = (TableSqlFunction) rexCall.getOperator();
                return isPythonFunction(tfc.udtf());
            } else if (rexCall.getOperator() instanceof BridgingSqlFunction) {
                BridgingSqlFunction bsf = (BridgingSqlFunction) rexCall.getOperator();
                return isPythonFunction(bsf.getDefinition());
            } else {
                return false;
            }
        }

        private boolean isPythonFunction(FunctionDefinition functionDefinition) {
            if (functionDefinition instanceof PythonFunction) {
                PythonFunction pythonFunction = (PythonFunction) functionDefinition;
                return !pythonFunctionKind.isPresent()
                        || pythonFunction.getPythonFunctionKind() == pythonFunctionKind.get();
            } else {
                return false;
            }
        }

        @Override
        public Boolean visitCall(RexCall call) {
            return findPythonFunction == isPythonRexCall(call)
                    || (recursive
                            && call.getOperands().stream()
                                    .anyMatch(operand -> operand.accept(this)));
        }

        @Override
        public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
            return fieldAccess.getReferenceExpr().accept(this);
        }

        @Override
        public Boolean visitNode(RexNode rexNode) {
            return false;
        }
    }

    /** Checks whether a rexNode is only a field reference of the given index. */
    private static class FieldReferenceDetector extends RexDefaultVisitor<Boolean> {
        private final int idx;

        public FieldReferenceDetector(int idx) {
            this.idx = idx;
        }

        @Override
        public Boolean visitNode(RexNode rexNode) {
            return false;
        }

        @Override
        public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
            if (fieldAccess.getField().getIndex() != idx) {
                return false;
            }
            RexNode expr = fieldAccess.getReferenceExpr();
            if (expr instanceof RexInputRef) {
                return ((RexInputRef) expr).getIndex() == 0;
            } else {
                return false;
            }
        }

        @Override
        public Boolean visitCall(RexCall call) {
            if (call.getKind() == SqlKind.AS) {
                return call.getOperands().get(0).accept(this);
            } else {
                return false;
            }
        }
    }
}
