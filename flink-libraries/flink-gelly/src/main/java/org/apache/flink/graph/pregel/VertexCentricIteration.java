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

package org.apache.flink.graph.pregel;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.Either;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;
import java.util.Map;

/**
 * This class represents iterative graph computations, programmed in a vertex-centric perspective.
 * It is a special case of <i>Bulk Synchronous Parallel</i> computation. The paradigm has also been
 * implemented by Google's <i>Pregel</i> system and by <i>Apache Giraph</i>.
 *
 * <p>Vertex centric algorithms operate on graphs, which are defined through vertices and edges. The
 * algorithms send messages along the edges and update the state of vertices based on the old state
 * and the incoming messages. All vertices have an initial state. The computation terminates once no
 * vertex receives any message anymore. Additionally, a maximum number of iterations (supersteps)
 * may be specified.
 *
 * <p>The computation is here represented by one function:
 *
 * <ul>
 *   <li>The {@link ComputeFunction} receives incoming messages, may update the state for the
 *       vertex, and sends messages along the edges of the vertex.
 * </ul>
 *
 * <p>Vertex-centric graph iterations are run by calling {@link
 * Graph#runVertexCentricIteration(ComputeFunction, MessageCombiner, int)}.
 *
 * @param <K> The type of the vertex key (the vertex identifier).
 * @param <VV> The type of the vertex value (the state of the vertex).
 * @param <Message> The type of the message sent between vertices along the edges.
 * @param <EV> The type of the values that are associated with the edges.
 */
public class VertexCentricIteration<K, VV, EV, Message>
        implements CustomUnaryOperation<Vertex<K, VV>, Vertex<K, VV>> {

    private final ComputeFunction<K, VV, EV, Message> computeFunction;

    private final MessageCombiner<K, Message> combineFunction;

    private final DataSet<Edge<K, EV>> edgesWithValue;

    private final int maximumNumberOfIterations;

    private final TypeInformation<Message> messageType;

    private DataSet<Vertex<K, VV>> initialVertices;

    private VertexCentricConfiguration configuration;

    // ----------------------------------------------------------------------------------

    private VertexCentricIteration(
            ComputeFunction<K, VV, EV, Message> cf,
            DataSet<Edge<K, EV>> edgesWithValue,
            MessageCombiner<K, Message> mc,
            int maximumNumberOfIterations) {

        Preconditions.checkNotNull(cf);
        Preconditions.checkNotNull(edgesWithValue);
        Preconditions.checkArgument(
                maximumNumberOfIterations > 0,
                "The maximum number of iterations must be at least one.");

        this.computeFunction = cf;
        this.edgesWithValue = edgesWithValue;
        this.combineFunction = mc;
        this.maximumNumberOfIterations = maximumNumberOfIterations;
        this.messageType = getMessageType(cf);
    }

    private TypeInformation<Message> getMessageType(ComputeFunction<K, VV, EV, Message> cf) {
        return TypeExtractor.createTypeInfo(cf, ComputeFunction.class, cf.getClass(), 3);
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
     * Creates the operator that represents this vertex-centric graph computation.
     *
     * <p>The Pregel iteration is mapped to delta iteration as follows. The solution set consists of
     * the set of active vertices and the workset contains the set of messages send to vertices
     * during the previous superstep. Initially, the workset contains a null message for each
     * vertex. In the beginning of a superstep, the solution set is joined with the workset to
     * produce a dataset containing tuples of vertex state and messages (vertex inbox). The
     * superstep compute UDF is realized with a coGroup between the vertices with inbox and the
     * graph edges. The output of the compute UDF contains both the new vertex values and the new
     * messages produced. These are directed to the solution set delta and new workset,
     * respectively, with subsequent flatMaps.
     *
     * @return The operator that represents this vertex-centric graph computation.
     */
    @Override
    public DataSet<Vertex<K, VV>> createResult() {
        if (this.initialVertices == null) {
            throw new IllegalStateException("The input data set has not been set.");
        }

        // prepare the type information
        TypeInformation<K> keyType = ((TupleTypeInfo<?>) initialVertices.getType()).getTypeAt(0);
        TypeInformation<Tuple2<K, Message>> messageTypeInfo =
                new TupleTypeInfo<>(keyType, messageType);
        TypeInformation<Vertex<K, VV>> vertexType = initialVertices.getType();
        TypeInformation<Either<Vertex<K, VV>, Tuple2<K, Message>>> intermediateTypeInfo =
                new EitherTypeInfo<>(vertexType, messageTypeInfo);
        TypeInformation<Either<NullValue, Message>> nullableMsgTypeInfo =
                new EitherTypeInfo<>(TypeExtractor.getForClass(NullValue.class), messageType);
        TypeInformation<Tuple2<K, Either<NullValue, Message>>> workSetTypeInfo =
                new TupleTypeInfo<>(keyType, nullableMsgTypeInfo);

        DataSet<Tuple2<K, Either<NullValue, Message>>> initialWorkSet =
                initialVertices
                        .map(new InitializeWorkSet<K, VV, Message>())
                        .returns(workSetTypeInfo);

        final DeltaIteration<Vertex<K, VV>, Tuple2<K, Either<NullValue, Message>>> iteration =
                initialVertices.iterateDelta(initialWorkSet, this.maximumNumberOfIterations, 0);
        setUpIteration(iteration);

        // join with the current state to get vertex values
        DataSet<Tuple2<Vertex<K, VV>, Either<NullValue, Message>>> verticesWithMsgs =
                iteration
                        .getSolutionSet()
                        .join(iteration.getWorkset())
                        .where(0)
                        .equalTo(0)
                        .with(new AppendVertexState<>())
                        .returns(new TupleTypeInfo<>(vertexType, nullableMsgTypeInfo));

        VertexComputeUdf<K, VV, EV, Message> vertexUdf =
                new VertexComputeUdf<>(computeFunction, intermediateTypeInfo);

        CoGroupOperator<?, ?, Either<Vertex<K, VV>, Tuple2<K, Message>>> superstepComputation =
                verticesWithMsgs.coGroup(edgesWithValue).where("f0.f0").equalTo(0).with(vertexUdf);

        // compute the solution set delta
        DataSet<Vertex<K, VV>> solutionSetDelta =
                superstepComputation.flatMap(new ProjectNewVertexValue<>()).returns(vertexType);

        // compute the inbox of each vertex for the next superstep (new workset)
        DataSet<Tuple2<K, Either<NullValue, Message>>> allMessages =
                superstepComputation.flatMap(new ProjectMessages<>()).returns(workSetTypeInfo);

        DataSet<Tuple2<K, Either<NullValue, Message>>> newWorkSet = allMessages;

        // check if a combiner has been provided
        if (combineFunction != null) {

            MessageCombinerUdf<K, Message> combinerUdf =
                    new MessageCombinerUdf<>(combineFunction, workSetTypeInfo);

            DataSet<Tuple2<K, Either<NullValue, Message>>> combinedMessages =
                    allMessages.groupBy(0).reduceGroup(combinerUdf).setCombinable(true);

            newWorkSet = combinedMessages;
        }

        // configure the compute function
        superstepComputation = superstepComputation.name("Compute Function");
        if (this.configuration != null) {
            for (Tuple2<String, DataSet<?>> e : this.configuration.getBcastVars()) {
                superstepComputation = superstepComputation.withBroadcastSet(e.f1, e.f0);
            }
        }

        return iteration.closeWith(solutionSetDelta, newWorkSet);
    }

    /**
     * Creates a new vertex-centric iteration operator.
     *
     * @param edgesWithValue The data set containing edges.
     * @param cf The compute function
     * @param <K> The type of the vertex key (the vertex identifier).
     * @param <VV> The type of the vertex value (the state of the vertex).
     * @param <Message> The type of the message sent between vertices along the edges.
     * @param <EV> The type of the values that are associated with the edges.
     * @return An instance of the vertex-centric graph computation operator.
     */
    public static <K, VV, EV, Message> VertexCentricIteration<K, VV, EV, Message> withEdges(
            DataSet<Edge<K, EV>> edgesWithValue,
            ComputeFunction<K, VV, EV, Message> cf,
            int maximumNumberOfIterations) {

        return new VertexCentricIteration<>(cf, edgesWithValue, null, maximumNumberOfIterations);
    }

    /**
     * Creates a new vertex-centric iteration operator for graphs where the edges are associated
     * with a value (such as a weight or distance).
     *
     * @param edgesWithValue The data set containing edges.
     * @param cf The compute function.
     * @param mc The function that combines messages sent to a vertex during a superstep.
     * @param <K> The type of the vertex key (the vertex identifier).
     * @param <VV> The type of the vertex value (the state of the vertex).
     * @param <Message> The type of the message sent between vertices along the edges.
     * @param <EV> The type of the values that are associated with the edges.
     * @return An instance of the vertex-centric graph computation operator.
     */
    public static <K, VV, EV, Message> VertexCentricIteration<K, VV, EV, Message> withEdges(
            DataSet<Edge<K, EV>> edgesWithValue,
            ComputeFunction<K, VV, EV, Message> cf,
            MessageCombiner<K, Message> mc,
            int maximumNumberOfIterations) {

        return new VertexCentricIteration<>(cf, edgesWithValue, mc, maximumNumberOfIterations);
    }

    /**
     * Configures this vertex-centric iteration with the provided parameters.
     *
     * @param parameters the configuration parameters
     */
    public void configure(VertexCentricConfiguration parameters) {
        this.configuration = parameters;
    }

    /** @return the configuration parameters of this vertex-centric iteration */
    public VertexCentricConfiguration getIterationConfiguration() {
        return this.configuration;
    }

    // --------------------------------------------------------------------------------------------
    //  Wrapping UDFs
    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("serial")
    private static class InitializeWorkSet<K, VV, Message>
            extends RichMapFunction<Vertex<K, VV>, Tuple2<K, Either<NullValue, Message>>> {

        private Tuple2<K, Either<NullValue, Message>> outTuple;
        private Either<NullValue, Message> nullMessage;

        @Override
        public void open(Configuration parameters) {
            outTuple = new Tuple2<>();
            nullMessage = Either.Left(NullValue.getInstance());
            outTuple.f1 = nullMessage;
        }

        public Tuple2<K, Either<NullValue, Message>> map(Vertex<K, VV> vertex) {
            outTuple.f0 = vertex.getId();
            return outTuple;
        }
    }

    /**
     * This coGroup class wraps the user-defined compute function. The first input holds a Tuple2
     * containing the vertex state and its inbox. The second input is an iterator of the out-going
     * edges of this vertex.
     */
    @SuppressWarnings("serial")
    private static class VertexComputeUdf<K, VV, EV, Message>
            extends RichCoGroupFunction<
                    Tuple2<Vertex<K, VV>, Either<NullValue, Message>>,
                    Edge<K, EV>,
                    Either<Vertex<K, VV>, Tuple2<K, Message>>>
            implements ResultTypeQueryable<Either<Vertex<K, VV>, Tuple2<K, Message>>> {

        final ComputeFunction<K, VV, EV, Message> computeFunction;
        private transient TypeInformation<Either<Vertex<K, VV>, Tuple2<K, Message>>> resultType;

        private VertexComputeUdf(
                ComputeFunction<K, VV, EV, Message> compute,
                TypeInformation<Either<Vertex<K, VV>, Tuple2<K, Message>>> typeInfo) {

            this.computeFunction = compute;
            this.resultType = typeInfo;
        }

        @Override
        public TypeInformation<Either<Vertex<K, VV>, Tuple2<K, Message>>> getProducedType() {
            return this.resultType;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
                this.computeFunction.init(getIterationRuntimeContext());
            }
            this.computeFunction.preSuperstep();
        }

        @Override
        public void close() throws Exception {
            this.computeFunction.postSuperstep();
        }

        @Override
        public void coGroup(
                Iterable<Tuple2<Vertex<K, VV>, Either<NullValue, Message>>> messages,
                Iterable<Edge<K, EV>> edgesIterator,
                Collector<Either<Vertex<K, VV>, Tuple2<K, Message>>> out)
                throws Exception {

            final Iterator<Tuple2<Vertex<K, VV>, Either<NullValue, Message>>> vertexIter =
                    messages.iterator();

            if (vertexIter.hasNext()) {

                final Tuple2<Vertex<K, VV>, Either<NullValue, Message>> first = vertexIter.next();
                final Vertex<K, VV> vertexState = first.f0;
                final MessageIterator<Message> messageIter = new MessageIterator<>();

                if (getIterationRuntimeContext().getSuperstepNumber() == 1) {
                    // there are no messages during the 1st superstep
                } else {
                    messageIter.setFirst(first.f1.right());
                    @SuppressWarnings("unchecked")
                    Iterator<Tuple2<?, Either<NullValue, Message>>> downcastIter =
                            (Iterator<Tuple2<?, Either<NullValue, Message>>>)
                                    (Iterator<?>) vertexIter;
                    messageIter.setSource(downcastIter);
                }

                computeFunction.set(vertexState.getId(), edgesIterator.iterator(), out);
                computeFunction.compute(vertexState, messageIter);
            }
        }
    }

    @SuppressWarnings("serial")
    @ForwardedFields("f0")
    private static class MessageCombinerUdf<K, Message>
            extends RichGroupReduceFunction<
                    Tuple2<K, Either<NullValue, Message>>, Tuple2<K, Either<NullValue, Message>>>
            implements ResultTypeQueryable<Tuple2<K, Either<NullValue, Message>>>,
                    GroupCombineFunction<
                            Tuple2<K, Either<NullValue, Message>>,
                            Tuple2<K, Either<NullValue, Message>>> {

        final MessageCombiner<K, Message> combinerFunction;
        private transient TypeInformation<Tuple2<K, Either<NullValue, Message>>> resultType;

        private MessageCombinerUdf(
                MessageCombiner<K, Message> combineFunction,
                TypeInformation<Tuple2<K, Either<NullValue, Message>>> messageTypeInfo) {

            this.combinerFunction = combineFunction;
            this.resultType = messageTypeInfo;
        }

        @Override
        public TypeInformation<Tuple2<K, Either<NullValue, Message>>> getProducedType() {
            return resultType;
        }

        @Override
        public void reduce(
                Iterable<Tuple2<K, Either<NullValue, Message>>> messages,
                Collector<Tuple2<K, Either<NullValue, Message>>> out)
                throws Exception {

            final Iterator<Tuple2<K, Either<NullValue, Message>>> messageIterator =
                    messages.iterator();

            if (messageIterator.hasNext()) {

                final Tuple2<K, Either<NullValue, Message>> first = messageIterator.next();
                final K vertexID = first.f0;
                final MessageIterator<Message> messageIter = new MessageIterator<>();
                messageIter.setFirst(first.f1.right());

                @SuppressWarnings("unchecked")
                Iterator<Tuple2<?, Either<NullValue, Message>>> downcastIter =
                        (Iterator<Tuple2<?, Either<NullValue, Message>>>)
                                (Iterator<?>) messageIterator;
                messageIter.setSource(downcastIter);

                combinerFunction.set(vertexID, out);
                combinerFunction.combineMessages(messageIter);
            }
        }

        @Override
        public void combine(
                Iterable<Tuple2<K, Either<NullValue, Message>>> values,
                Collector<Tuple2<K, Either<NullValue, Message>>> out)
                throws Exception {
            this.reduce(values, out);
        }
    }

    // --------------------------------------------------------------------------------------------
    //  UTIL methods
    // --------------------------------------------------------------------------------------------

    /**
     * Helper method which sets up an iteration with the given vertex value.
     *
     * @param iteration
     */
    private void setUpIteration(DeltaIteration<?, ?> iteration) {

        // set up the iteration operator
        if (this.configuration != null) {

            iteration.name(
                    this.configuration.getName(
                            "Vertex-centric iteration (" + computeFunction + ")"));
            iteration.parallelism(this.configuration.getParallelism());
            iteration.setSolutionSetUnManaged(this.configuration.isSolutionSetUnmanagedMemory());

            // register all aggregators
            for (Map.Entry<String, Aggregator<?>> entry :
                    this.configuration.getAggregators().entrySet()) {
                iteration.registerAggregator(entry.getKey(), entry.getValue());
            }
        } else {
            // no configuration provided; set default name
            iteration.name("Vertex-centric iteration (" + computeFunction + ")");
        }
    }

    @SuppressWarnings("serial")
    @ForwardedFieldsFirst("*->f0")
    @ForwardedFieldsSecond("f1->f1")
    private static final class AppendVertexState<K, VV, Message>
            implements JoinFunction<
                    Vertex<K, VV>,
                    Tuple2<K, Either<NullValue, Message>>,
                    Tuple2<Vertex<K, VV>, Either<NullValue, Message>>> {

        private Tuple2<Vertex<K, VV>, Either<NullValue, Message>> outTuple = new Tuple2<>();

        public Tuple2<Vertex<K, VV>, Either<NullValue, Message>> join(
                Vertex<K, VV> vertex, Tuple2<K, Either<NullValue, Message>> message) {

            outTuple.f0 = vertex;
            outTuple.f1 = message.f1;
            return outTuple;
        }
    }

    @SuppressWarnings("serial")
    private static final class ProjectNewVertexValue<K, VV, Message>
            implements FlatMapFunction<Either<Vertex<K, VV>, Tuple2<K, Message>>, Vertex<K, VV>> {

        public void flatMap(
                Either<Vertex<K, VV>, Tuple2<K, Message>> value, Collector<Vertex<K, VV>> out) {

            if (value.isLeft()) {
                out.collect(value.left());
            }
        }
    }

    @SuppressWarnings("serial")
    private static final class ProjectMessages<K, VV, Message>
            implements FlatMapFunction<
                    Either<Vertex<K, VV>, Tuple2<K, Message>>,
                    Tuple2<K, Either<NullValue, Message>>> {

        private Tuple2<K, Either<NullValue, Message>> outTuple = new Tuple2<>();

        public void flatMap(
                Either<Vertex<K, VV>, Tuple2<K, Message>> value,
                Collector<Tuple2<K, Either<NullValue, Message>>> out) {

            if (value.isRight()) {
                Tuple2<K, Message> message = value.right();
                outTuple.f0 = message.f0;
                outTuple.f1 = Either.Right(message.f1);
                out.collect(outTuple);
            }
        }
    }
}
