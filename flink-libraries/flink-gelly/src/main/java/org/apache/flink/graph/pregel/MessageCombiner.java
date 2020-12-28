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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Either;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * The base class for combining messages sent during a {@link VertexCentricIteration}.
 *
 * @param <K> The type of the vertex id
 * @param <Message> The type of the message sent between vertices along the edges.
 */
public abstract class MessageCombiner<K, Message> implements Serializable {

    private static final long serialVersionUID = 1L;

    private Collector<Tuple2<K, Either<NullValue, Message>>> out;

    private Tuple2<K, Either<NullValue, Message>> outValue;

    void set(K target, Collector<Tuple2<K, Either<NullValue, Message>>> collector) {
        this.out = collector;
        this.outValue = new Tuple2<>();
        outValue.f0 = target;
    }

    /**
     * Combines messages sent from different vertices to a target vertex. Implementing this method
     * might reduce communication costs during a vertex-centric iteration.
     *
     * @param messages the input messages to combine
     * @throws Exception
     */
    public abstract void combineMessages(MessageIterator<Message> messages) throws Exception;

    /**
     * Sends the combined message to the target vertex.
     *
     * @param combinedMessage
     * @throws Exception
     */
    public final void sendCombinedMessage(Message combinedMessage) {
        outValue.f1 = Either.Right(combinedMessage);
        out.collect(outValue);
    }
}
