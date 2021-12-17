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

package org.apache.flink.graph.spargel;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.utils.GraphUtils;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * This class represents iterative graph computations, programmed in a scatter-gather perspective.
 * It is a special case of <i>Bulk Synchronous Parallel</i> computation.
 *
 * <p>Scatter-Gather algorithms operate on graphs, which are defined through vertices and edges. The
 * algorithms send messages along the edges and update the state of vertices based on the old state
 * and the incoming messages. All vertices have an initial state. The computation terminates once no
 * vertex updates its state any more. Additionally, a maximum number of iterations (supersteps) may
 * be specified.
 *
 * <p>The computation is here represented by two functions:
 *
 * <ul>
 *   <li>The {@link GatherFunction} receives incoming messages and may updates the state for the
 *       vertex. If a state is updated, messages are sent from this vertex. Initially, all vertices
 *       are considered updated.
 *   <li>The {@link ScatterFunction} takes the new vertex state and sends messages along the
 *       outgoing edges of the vertex. The outgoing edges may optionally have an associated value,
 *       such as a weight.
 * </ul>
 *
 * <p>Scatter-Gather graph iterations are are run by calling {@link
 * Graph#runScatterGatherIteration(ScatterFunction, GatherFunction, int)}.
 *
 * @param <K> The type of the vertex key (the vertex identifier).
 * @param <VV> The type of the vertex value (the state of the vertex).
 * @param <Message> The type of the message sent between vertices along the edges.
 * @param <EV> The type of the values that are associated with the edges.
 */
public class ScatterGatherIteration<K, VV, Message, EV>
        implements CustomUnaryOperation<Vertex<K, VV>, Vertex<K, VV>> {
    private final ScatterFunction<K, VV, Message, EV> scatterFunction;

    private final GatherFunction<K, VV, Message> gatherFunction;

    private final DataSet<Edge<K, EV>> edgesWithValue;

    private final int maximumNumberOfIterations;

    private final TypeInformation<Message> messageType;

    private DataSet<Vertex<K, VV>> initialVertices;

    private ScatterGatherConfiguration configuration;

    // ----------------------------------------------------------------------------------

    private ScatterGatherIteration(
            ScatterFunction<K, VV, Message, EV> sf,
            GatherFunction<K, VV, Message> gf,
            DataSet<Edge<K, EV>> edgesWithValue,
            int maximumNumberOfIterations) {
        Preconditions.checkNotNull(sf);
        Preconditions.checkNotNull(gf);
        Preconditions.checkNotNull(edgesWithValue);
        Preconditions.checkArgument(
                maximumNumberOfIterations > 0,
                "The maximum number of iterations must be at least one.");

        this.scatterFunction = sf;
        this.gatherFunction = gf;
        this.edgesWithValue = edgesWithValue;
        this.maximumNumberOfIterations = maximumNumberOfIterations;
        this.messageType = getMessageType(sf);
    }

    private TypeInformation<Message> getMessageType(ScatterFunction<K, VV, Message, EV> mf) {
        return TypeExtractor.createTypeInfo(mf, ScatterFunction.class, mf.getClass(), 2);
    }

    // --------------------------------------------------------------------------------------------
    //  Custom Operator behavior
    // --------------------------------------------------------------------------------------------

    /**
     * Sets the input data set for this operator. In the case of this operator this input data set
     * represents the set of vertices with their initial state.
     *
     * @param inputData The input data set, which in the case of this operator represents the set of
     *     vertices with their initial state.
     * @see
     *     org.apache.flink.api.java.operators.CustomUnaryOperation#setInput(org.apache.flink.api.java.DataSet)
     */
    @Override
    public void setInput(DataSet<Vertex<K, VV>> inputData) {
        this.initialVertices = inputData;
    }

    /**
     * Creates the operator that represents this scatter-gather graph computation.
     *
     * @return The operator that represents this scatter-gather graph computation.
     */
    @Override
    public DataSet<Vertex<K, VV>> createResult() {
        if (this.initialVertices == null) {
            throw new IllegalStateException("The input data set has not been set.");
        }

        // prepare some type information
        TypeInformation<K> keyType = ((TupleTypeInfo<?>) initialVertices.getType()).getTypeAt(0);
        TypeInformation<Tuple2<K, Message>> messageTypeInfo =
                new TupleTypeInfo<>(keyType, messageType);

        // create a graph
        Graph<K, VV, EV> graph =
                Graph.fromDataSet(
                        initialVertices, edgesWithValue, initialVertices.getExecutionEnvironment());

        // check whether the numVertices option is set and, if so, compute the total number of
        // vertices
        // and set it within the scatter and gather functions

        DataSet<LongValue> numberOfVertices = null;
        if (this.configuration != null && this.configuration.isOptNumVertices()) {
            try {
                numberOfVertices = GraphUtils.count(this.initialVertices);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (this.configuration != null) {
            scatterFunction.setDirection(this.configuration.getDirection());
        } else {
            scatterFunction.setDirection(EdgeDirection.OUT);
        }

        // retrieve the direction in which the updates are made and in which the messages are sent
        EdgeDirection messagingDirection = scatterFunction.getDirection();

        // check whether the degrees option is set and, if so, compute the in and the out degrees
        // and
        // add them to the vertex value
        if (this.configuration != null && this.configuration.isOptDegrees()) {
            return createResultVerticesWithDegrees(
                    graph, messagingDirection, messageTypeInfo, numberOfVertices);
        } else {
            return createResultSimpleVertex(messagingDirection, messageTypeInfo, numberOfVertices);
        }
    }

    /**
     * Creates a new scatter-gather iteration operator for graphs where the edges are associated
     * with a value (such as a weight or distance).
     *
     * @param edgesWithValue The data set containing edges.
     * @param sf The function that turns changed vertex states into messages along the edges.
     * @param gf The function that updates the state of the vertices from the incoming messages.
     * @param <K> The type of the vertex key (the vertex identifier).
     * @param <VV> The type of the vertex value (the state of the vertex).
     * @param <Message> The type of the message sent between vertices along the edges.
     * @param <EV> The type of the values that are associated with the edges.
     * @return An in stance of the scatter-gather graph computation operator.
     */
    public static <K, VV, Message, EV> ScatterGatherIteration<K, VV, Message, EV> withEdges(
            DataSet<Edge<K, EV>> edgesWithValue,
            ScatterFunction<K, VV, Message, EV> sf,
            GatherFunction<K, VV, Message> gf,
            int maximumNumberOfIterations) {

        return new ScatterGatherIteration<>(sf, gf, edgesWithValue, maximumNumberOfIterations);
    }

    /**
     * Configures this scatter-gather iteration with the provided parameters.
     *
     * @param parameters the configuration parameters
     */
    public void configure(ScatterGatherConfiguration parameters) {
        this.configuration = parameters;
    }

    /** @return the configuration parameters of this scatter-gather iteration */
    public ScatterGatherConfiguration getIterationConfiguration() {
        return this.configuration;
    }

    // --------------------------------------------------------------------------------------------
    //  Wrapping UDFs
    // --------------------------------------------------------------------------------------------

    /*
     * UDF that encapsulates the message sending function for graphs where the edges have an associated value.
     */
    private abstract static class ScatterUdfWithEdgeValues<K, VVWithDegrees, VV, Message, EV>
            extends RichCoGroupFunction<Edge<K, EV>, Vertex<K, VVWithDegrees>, Tuple2<K, Message>>
            implements ResultTypeQueryable<Tuple2<K, Message>> {

        private static final long serialVersionUID = 1L;

        final ScatterFunction<K, VV, Message, EV> scatterFunction;

        private transient TypeInformation<Tuple2<K, Message>> resultType;

        private ScatterUdfWithEdgeValues(
                ScatterFunction<K, VV, Message, EV> scatterFunction,
                TypeInformation<Tuple2<K, Message>> resultType) {
            this.scatterFunction = scatterFunction;
            this.resultType = resultType;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            if (getRuntimeContext().hasBroadcastVariable("number of vertices")) {
                Collection<LongValue> numberOfVertices =
                        getRuntimeContext().getBroadcastVariable("number of vertices");
                this.scatterFunction.setNumberOfVertices(
                        numberOfVertices.iterator().next().getValue());
            }
            if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
                this.scatterFunction.init(getIterationRuntimeContext());
            }
            this.scatterFunction.preSuperstep();
        }

        @Override
        public void close() throws Exception {
            this.scatterFunction.postSuperstep();
        }

        @Override
        public TypeInformation<Tuple2<K, Message>> getProducedType() {
            return this.resultType;
        }
    }

    @SuppressWarnings("serial")
    private static final class ScatterUdfWithEVsSimpleVV<K, VV, Message, EV>
            extends ScatterUdfWithEdgeValues<K, VV, VV, Message, EV> {

        private ScatterUdfWithEVsSimpleVV(
                ScatterFunction<K, VV, Message, EV> scatterFunction,
                TypeInformation<Tuple2<K, Message>> resultType) {
            super(scatterFunction, resultType);
        }

        @Override
        public void coGroup(
                Iterable<Edge<K, EV>> edges,
                Iterable<Vertex<K, VV>> state,
                Collector<Tuple2<K, Message>> out)
                throws Exception {
            final Iterator<Vertex<K, VV>> stateIter = state.iterator();

            if (stateIter.hasNext()) {
                Vertex<K, VV> newVertexState = stateIter.next();
                scatterFunction.set(edges.iterator(), out, newVertexState.getId());
                scatterFunction.sendMessages(newVertexState);
            }
        }
    }

    @SuppressWarnings("serial")
    private static final class ScatterUdfWithEVsVVWithDegrees<K, VV, Message, EV>
            extends ScatterUdfWithEdgeValues<K, Tuple3<VV, LongValue, LongValue>, VV, Message, EV> {

        private Vertex<K, VV> nextVertex = new Vertex<>();

        private ScatterUdfWithEVsVVWithDegrees(
                ScatterFunction<K, VV, Message, EV> scatterFunction,
                TypeInformation<Tuple2<K, Message>> resultType) {
            super(scatterFunction, resultType);
        }

        @Override
        public void coGroup(
                Iterable<Edge<K, EV>> edges,
                Iterable<Vertex<K, Tuple3<VV, LongValue, LongValue>>> state,
                Collector<Tuple2<K, Message>> out)
                throws Exception {

            final Iterator<Vertex<K, Tuple3<VV, LongValue, LongValue>>> stateIter =
                    state.iterator();

            if (stateIter.hasNext()) {
                Vertex<K, Tuple3<VV, LongValue, LongValue>> vertexWithDegrees = stateIter.next();

                nextVertex.f0 = vertexWithDegrees.f0;
                nextVertex.f1 = vertexWithDegrees.f1.f0;

                scatterFunction.setInDegree(vertexWithDegrees.f1.f1.getValue());
                scatterFunction.setOutDegree(vertexWithDegrees.f1.f2.getValue());

                scatterFunction.set(edges.iterator(), out, vertexWithDegrees.getId());
                scatterFunction.sendMessages(nextVertex);
            }
        }
    }

    private abstract static class GatherUdf<K, VVWithDegrees, Message>
            extends RichCoGroupFunction<
                    Tuple2<K, Message>, Vertex<K, VVWithDegrees>, Vertex<K, VVWithDegrees>>
            implements ResultTypeQueryable<Vertex<K, VVWithDegrees>> {

        private static final long serialVersionUID = 1L;

        final GatherFunction<K, VVWithDegrees, Message> gatherFunction;

        final MessageIterator<Message> messageIter = new MessageIterator<>();

        private transient TypeInformation<Vertex<K, VVWithDegrees>> resultType;

        private GatherUdf(
                GatherFunction<K, VVWithDegrees, Message> gatherFunction,
                TypeInformation<Vertex<K, VVWithDegrees>> resultType) {

            this.gatherFunction = gatherFunction;
            this.resultType = resultType;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            if (getRuntimeContext().hasBroadcastVariable("number of vertices")) {
                Collection<LongValue> numberOfVertices =
                        getRuntimeContext().getBroadcastVariable("number of vertices");
                this.gatherFunction.setNumberOfVertices(
                        numberOfVertices.iterator().next().getValue());
            }
            if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
                this.gatherFunction.init(getIterationRuntimeContext());
            }
            this.gatherFunction.preSuperstep();
        }

        @Override
        public void close() throws Exception {
            this.gatherFunction.postSuperstep();
        }

        @Override
        public TypeInformation<Vertex<K, VVWithDegrees>> getProducedType() {
            return this.resultType;
        }
    }

    @SuppressWarnings("serial")
    private static final class GatherUdfSimpleVV<K, VV, Message> extends GatherUdf<K, VV, Message> {

        private GatherUdfSimpleVV(
                GatherFunction<K, VV, Message> gatherFunction,
                TypeInformation<Vertex<K, VV>> resultType) {
            super(gatherFunction, resultType);
        }

        @Override
        public void coGroup(
                Iterable<Tuple2<K, Message>> messages,
                Iterable<Vertex<K, VV>> vertex,
                Collector<Vertex<K, VV>> out)
                throws Exception {
            final Iterator<Vertex<K, VV>> vertexIter = vertex.iterator();

            if (vertexIter.hasNext()) {
                Vertex<K, VV> vertexState = vertexIter.next();

                @SuppressWarnings("unchecked")
                Iterator<Tuple2<?, Message>> downcastIter =
                        (Iterator<Tuple2<?, Message>>) (Iterator<?>) messages.iterator();
                messageIter.setSource(downcastIter);

                gatherFunction.setOutput(vertexState, out);
                gatherFunction.updateVertex(vertexState, messageIter);
            } else {
                final Iterator<Tuple2<K, Message>> messageIter = messages.iterator();
                if (messageIter.hasNext()) {
                    String message = "Target vertex does not exist!.";
                    try {
                        Tuple2<K, Message> next = messageIter.next();
                        message = "Target vertex '" + next.f0 + "' does not exist!.";
                    } catch (Throwable ignored) {
                    }
                    throw new Exception(message);
                } else {
                    throw new Exception();
                }
            }
        }
    }

    @SuppressWarnings("serial")
    private static final class GatherUdfVVWithDegrees<K, VV, Message>
            extends GatherUdf<K, Tuple3<VV, LongValue, LongValue>, Message> {

        private GatherUdfVVWithDegrees(
                GatherFunction<K, Tuple3<VV, LongValue, LongValue>, Message> gatherFunction,
                TypeInformation<Vertex<K, Tuple3<VV, LongValue, LongValue>>> resultType) {
            super(gatherFunction, resultType);
        }

        @Override
        public void coGroup(
                Iterable<Tuple2<K, Message>> messages,
                Iterable<Vertex<K, Tuple3<VV, LongValue, LongValue>>> vertex,
                Collector<Vertex<K, Tuple3<VV, LongValue, LongValue>>> out)
                throws Exception {

            final Iterator<Vertex<K, Tuple3<VV, LongValue, LongValue>>> vertexIter =
                    vertex.iterator();

            if (vertexIter.hasNext()) {
                Vertex<K, Tuple3<VV, LongValue, LongValue>> vertexWithDegrees = vertexIter.next();

                @SuppressWarnings("unchecked")
                Iterator<Tuple2<?, Message>> downcastIter =
                        (Iterator<Tuple2<?, Message>>) (Iterator<?>) messages.iterator();
                messageIter.setSource(downcastIter);

                gatherFunction.setInDegree(vertexWithDegrees.f1.f1.getValue());
                gatherFunction.setOutDegree(vertexWithDegrees.f1.f2.getValue());

                gatherFunction.setOutputWithDegrees(vertexWithDegrees, out);
                gatherFunction.updateVertexFromScatterGatherIteration(
                        vertexWithDegrees, messageIter);
            } else {
                final Iterator<Tuple2<K, Message>> messageIter = messages.iterator();
                if (messageIter.hasNext()) {
                    String message = "Target vertex does not exist!.";
                    try {
                        Tuple2<K, Message> next = messageIter.next();
                        message = "Target vertex '" + next.f0 + "' does not exist!.";
                    } catch (Throwable ignored) {
                    }
                    throw new Exception(message);
                } else {
                    throw new Exception();
                }
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    //  UTIL methods
    // --------------------------------------------------------------------------------------------

    /**
     * Method that builds the scatter function using a coGroup operator for a simple vertex (without
     * degrees). It afterwards configures the function with a custom name and broadcast variables.
     *
     * @param iteration
     * @param messageTypeInfo
     * @param whereArg the argument for the where within the coGroup
     * @param equalToArg the argument for the equalTo within the coGroup
     * @return the scatter function
     */
    private CoGroupOperator<?, ?, Tuple2<K, Message>> buildScatterFunction(
            DeltaIteration<Vertex<K, VV>, Vertex<K, VV>> iteration,
            TypeInformation<Tuple2<K, Message>> messageTypeInfo,
            int whereArg,
            int equalToArg,
            DataSet<LongValue> numberOfVertices) {

        // build the scatter function (co group)
        CoGroupOperator<?, ?, Tuple2<K, Message>> messages;
        ScatterUdfWithEdgeValues<K, VV, VV, Message, EV> messenger =
                new ScatterUdfWithEVsSimpleVV<>(scatterFunction, messageTypeInfo);

        messages =
                this.edgesWithValue
                        .coGroup(iteration.getWorkset())
                        .where(whereArg)
                        .equalTo(equalToArg)
                        .with(messenger);

        // configure coGroup message function with name and broadcast variables
        messages = messages.name("Messaging");
        if (this.configuration != null) {
            for (Tuple2<String, DataSet<?>> e : this.configuration.getScatterBcastVars()) {
                messages = messages.withBroadcastSet(e.f1, e.f0);
            }
            if (this.configuration.isOptNumVertices()) {
                messages = messages.withBroadcastSet(numberOfVertices, "number of vertices");
            }
        }

        return messages;
    }

    /**
     * Method that builds the scatter function using a coGroup operator for a vertex containing
     * degree information. It afterwards configures the function with a custom name and broadcast
     * variables.
     *
     * @param iteration
     * @param messageTypeInfo
     * @param whereArg the argument for the where within the coGroup
     * @param equalToArg the argument for the equalTo within the coGroup
     * @return the scatter function
     */
    private CoGroupOperator<?, ?, Tuple2<K, Message>> buildScatterFunctionVerticesWithDegrees(
            DeltaIteration<
                            Vertex<K, Tuple3<VV, LongValue, LongValue>>,
                            Vertex<K, Tuple3<VV, LongValue, LongValue>>>
                    iteration,
            TypeInformation<Tuple2<K, Message>> messageTypeInfo,
            int whereArg,
            int equalToArg,
            DataSet<LongValue> numberOfVertices) {

        // build the scatter function (co group)
        CoGroupOperator<?, ?, Tuple2<K, Message>> messages;
        ScatterUdfWithEdgeValues<K, Tuple3<VV, LongValue, LongValue>, VV, Message, EV> messenger =
                new ScatterUdfWithEVsVVWithDegrees<>(scatterFunction, messageTypeInfo);

        messages =
                this.edgesWithValue
                        .coGroup(iteration.getWorkset())
                        .where(whereArg)
                        .equalTo(equalToArg)
                        .with(messenger);

        // configure coGroup message function with name and broadcast variables
        messages = messages.name("Messaging");

        if (this.configuration != null) {
            for (Tuple2<String, DataSet<?>> e : this.configuration.getScatterBcastVars()) {
                messages = messages.withBroadcastSet(e.f1, e.f0);
            }
            if (this.configuration.isOptNumVertices()) {
                messages = messages.withBroadcastSet(numberOfVertices, "number of vertices");
            }
        }

        return messages;
    }

    /**
     * Helper method which sets up an iteration with the given vertex value(either simple or with
     * degrees).
     *
     * @param iteration
     */
    private void setUpIteration(DeltaIteration<?, ?> iteration) {

        // set up the iteration operator
        if (this.configuration != null) {

            iteration.name(
                    this.configuration.getName(
                            "Scatter-gather iteration ("
                                    + gatherFunction
                                    + " | "
                                    + scatterFunction
                                    + ")"));
            iteration.parallelism(this.configuration.getParallelism());
            iteration.setSolutionSetUnManaged(this.configuration.isSolutionSetUnmanagedMemory());

            // register all aggregators
            for (Map.Entry<String, Aggregator<?>> entry :
                    this.configuration.getAggregators().entrySet()) {
                iteration.registerAggregator(entry.getKey(), entry.getValue());
            }
        } else {
            // no configuration provided; set default name
            iteration.name(
                    "Scatter-gather iteration (" + gatherFunction + " | " + scatterFunction + ")");
        }
    }

    /**
     * Creates the operator that represents this scatter-gather graph computation for a simple
     * vertex.
     *
     * @param messagingDirection
     * @param messageTypeInfo
     * @param numberOfVertices
     * @return the operator
     */
    private DataSet<Vertex<K, VV>> createResultSimpleVertex(
            EdgeDirection messagingDirection,
            TypeInformation<Tuple2<K, Message>> messageTypeInfo,
            DataSet<LongValue> numberOfVertices) {

        DataSet<Tuple2<K, Message>> messages;

        TypeInformation<Vertex<K, VV>> vertexTypes = initialVertices.getType();

        final DeltaIteration<Vertex<K, VV>, Vertex<K, VV>> iteration =
                initialVertices.iterateDelta(initialVertices, this.maximumNumberOfIterations, 0);
        setUpIteration(iteration);

        switch (messagingDirection) {
            case IN:
                messages = buildScatterFunction(iteration, messageTypeInfo, 1, 0, numberOfVertices);
                break;
            case OUT:
                messages = buildScatterFunction(iteration, messageTypeInfo, 0, 0, numberOfVertices);
                break;
            case ALL:
                messages =
                        buildScatterFunction(iteration, messageTypeInfo, 1, 0, numberOfVertices)
                                .union(
                                        buildScatterFunction(
                                                iteration,
                                                messageTypeInfo,
                                                0,
                                                0,
                                                numberOfVertices));
                break;
            default:
                throw new IllegalArgumentException("Illegal edge direction");
        }

        GatherUdf<K, VV, Message> updateUdf = new GatherUdfSimpleVV<>(gatherFunction, vertexTypes);

        // build the update function (co group)
        CoGroupOperator<?, ?, Vertex<K, VV>> updates =
                messages.coGroup(iteration.getSolutionSet()).where(0).equalTo(0).with(updateUdf);

        if (this.configuration != null && this.configuration.isOptNumVertices()) {
            updates = updates.withBroadcastSet(numberOfVertices, "number of vertices");
        }

        configureUpdateFunction(updates);

        return iteration.closeWith(updates, updates);
    }

    /**
     * Creates the operator that represents this scatter-gather graph computation for a vertex with
     * in and out degrees added to the vertex value.
     *
     * @param graph
     * @param messagingDirection
     * @param messageTypeInfo
     * @param numberOfVertices
     * @return the operator
     */
    @SuppressWarnings("serial")
    private DataSet<Vertex<K, VV>> createResultVerticesWithDegrees(
            Graph<K, VV, EV> graph,
            EdgeDirection messagingDirection,
            TypeInformation<Tuple2<K, Message>> messageTypeInfo,
            DataSet<LongValue> numberOfVertices) {

        DataSet<Tuple2<K, Message>> messages;

        this.gatherFunction.setOptDegrees(this.configuration.isOptDegrees());

        DataSet<Tuple2<K, LongValue>> inDegrees = graph.inDegrees();
        DataSet<Tuple2<K, LongValue>> outDegrees = graph.outDegrees();

        DataSet<Tuple3<K, LongValue, LongValue>> degrees =
                inDegrees
                        .join(outDegrees)
                        .where(0)
                        .equalTo(0)
                        .with(
                                new FlatJoinFunction<
                                        Tuple2<K, LongValue>,
                                        Tuple2<K, LongValue>,
                                        Tuple3<K, LongValue, LongValue>>() {

                                    @Override
                                    public void join(
                                            Tuple2<K, LongValue> first,
                                            Tuple2<K, LongValue> second,
                                            Collector<Tuple3<K, LongValue, LongValue>> out) {
                                        out.collect(new Tuple3<>(first.f0, first.f1, second.f1));
                                    }
                                })
                        .withForwardedFieldsFirst("f0;f1")
                        .withForwardedFieldsSecond("f1");

        DataSet<Vertex<K, Tuple3<VV, LongValue, LongValue>>> verticesWithDegrees =
                initialVertices
                        .join(degrees)
                        .where(0)
                        .equalTo(0)
                        .with(
                                new FlatJoinFunction<
                                        Vertex<K, VV>,
                                        Tuple3<K, LongValue, LongValue>,
                                        Vertex<K, Tuple3<VV, LongValue, LongValue>>>() {
                                    @Override
                                    public void join(
                                            Vertex<K, VV> vertex,
                                            Tuple3<K, LongValue, LongValue> degrees,
                                            Collector<Vertex<K, Tuple3<VV, LongValue, LongValue>>>
                                                    out)
                                            throws Exception {
                                        out.collect(
                                                new Vertex<>(
                                                        vertex.getId(),
                                                        new Tuple3<>(
                                                                vertex.getValue(),
                                                                degrees.f1,
                                                                degrees.f2)));
                                    }
                                })
                        .withForwardedFieldsFirst("f0");

        // add type info
        TypeInformation<Vertex<K, Tuple3<VV, LongValue, LongValue>>> vertexTypes =
                verticesWithDegrees.getType();

        final DeltaIteration<
                        Vertex<K, Tuple3<VV, LongValue, LongValue>>,
                        Vertex<K, Tuple3<VV, LongValue, LongValue>>>
                iteration =
                        verticesWithDegrees.iterateDelta(
                                verticesWithDegrees, this.maximumNumberOfIterations, 0);
        setUpIteration(iteration);

        switch (messagingDirection) {
            case IN:
                messages =
                        buildScatterFunctionVerticesWithDegrees(
                                iteration, messageTypeInfo, 1, 0, numberOfVertices);
                break;
            case OUT:
                messages =
                        buildScatterFunctionVerticesWithDegrees(
                                iteration, messageTypeInfo, 0, 0, numberOfVertices);
                break;
            case ALL:
                messages =
                        buildScatterFunctionVerticesWithDegrees(
                                        iteration, messageTypeInfo, 1, 0, numberOfVertices)
                                .union(
                                        buildScatterFunctionVerticesWithDegrees(
                                                iteration,
                                                messageTypeInfo,
                                                0,
                                                0,
                                                numberOfVertices));
                break;
            default:
                throw new IllegalArgumentException("Illegal edge direction");
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        GatherUdf<K, Tuple3<VV, LongValue, LongValue>, Message> updateUdf =
                new GatherUdfVVWithDegrees(gatherFunction, vertexTypes);

        // build the update function (co group)
        CoGroupOperator<?, ?, Vertex<K, Tuple3<VV, LongValue, LongValue>>> updates =
                messages.coGroup(iteration.getSolutionSet()).where(0).equalTo(0).with(updateUdf);

        if (this.configuration != null && this.configuration.isOptNumVertices()) {
            updates = updates.withBroadcastSet(numberOfVertices, "number of vertices");
        }

        configureUpdateFunction(updates);

        return iteration
                .closeWith(updates, updates)
                .map(
                        new MapFunction<
                                Vertex<K, Tuple3<VV, LongValue, LongValue>>, Vertex<K, VV>>() {

                            public Vertex<K, VV> map(
                                    Vertex<K, Tuple3<VV, LongValue, LongValue>> vertex) {
                                return new Vertex<>(vertex.getId(), vertex.getValue().f0);
                            }
                        });
    }

    private <VVWithDegree> void configureUpdateFunction(
            CoGroupOperator<?, ?, Vertex<K, VVWithDegree>> updates) {

        // configure coGroup update function with name and broadcast variables
        updates = updates.name("Vertex State Updates");
        if (this.configuration != null) {
            for (Tuple2<String, DataSet<?>> e : this.configuration.getGatherBcastVars()) {
                updates = updates.withBroadcastSet(e.f1, e.f0);
            }
        }

        // let the operator know that we preserve the key field
        updates.withForwardedFieldsFirst("0").withForwardedFieldsSecond("0");
    }
}
