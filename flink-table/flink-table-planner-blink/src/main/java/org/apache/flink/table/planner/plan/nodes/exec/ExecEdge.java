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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.planner.plan.nodes.exec.serde.ExecNodeGraphJsonPlanGenerator.JsonPlanEdge;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The representation of an edge connecting two {@link ExecNode}s.
 *
 * <p>The edge's json serialization/deserialization will be delegated to {@link JsonPlanEdge}, which
 * only stores the {@link ExecNode}'s id instead of instance.
 *
 * <p>{@link JsonPlanEdge} should also be updated with this class if the fields are added/removed.
 */
public class ExecEdge {
    /** The source node of this edge. */
    private final ExecNode<?> source;
    /** The target node of this edge. */
    private final ExecNode<?> target;
    /** The {@link Shuffle} on this edge from source to target. */
    private final Shuffle shuffle;
    /** The {@link ShuffleMode} defines the data exchange mode on this edge. */
    private final ShuffleMode shuffleMode;

    public ExecEdge(
            ExecNode<?> source, ExecNode<?> target, Shuffle shuffle, ShuffleMode shuffleMode) {
        this.source = checkNotNull(source);
        this.target = checkNotNull(target);
        this.shuffle = checkNotNull(shuffle);
        this.shuffleMode = checkNotNull(shuffleMode);

        // TODO once FLINK-21224 [Remove BatchExecExchange and StreamExecExchange, and replace their
        //  functionality with ExecEdge] is finished, we should remove the following validation.
        if (shuffle.getType() != Shuffle.Type.FORWARD) {
            throw new TableException("Only FORWARD shuffle is supported now.");
        }
        if (shuffleMode != ShuffleMode.PIPELINED) {
            throw new TableException("Only PIPELINED shuffle mode is supported now.");
        }
    }

    public ExecNode<?> getSource() {
        return source;
    }

    public ExecNode<?> getTarget() {
        return target;
    }

    public Shuffle getShuffle() {
        return shuffle;
    }

    public ShuffleMode getShuffleMode() {
        return shuffleMode;
    }

    /** Returns the output {@link LogicalType} of the data passing this edge. */
    public LogicalType getOutputType() {
        return source.getOutputType();
    }

    @Override
    public String toString() {
        return "ExecEdge{"
                + "source="
                + source.getDescription()
                + ", target="
                + target.getDescription()
                + ", shuffle="
                + shuffle
                + ", shuffleMode="
                + shuffleMode
                + '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder of the {@link ExecEdge}. */
    public static class Builder {
        private ExecNode<?> source;
        private ExecNode<?> target;
        private Shuffle shuffle = FORWARD_SHUFFLE;
        private ShuffleMode shuffleMode = ShuffleMode.PIPELINED;

        public Builder source(ExecNode<?> source) {
            this.source = source;
            return this;
        }

        public Builder target(ExecNode<?> target) {
            this.target = target;
            return this;
        }

        public Builder shuffle(Shuffle shuffle) {
            this.shuffle = shuffle;
            return this;
        }

        public Builder requiredDistribution(
                InputProperty.RequiredDistribution requiredDistribution) {
            return shuffle(fromRequiredDistribution(requiredDistribution));
        }

        public Builder shuffleMode(ShuffleMode shuffleMode) {
            this.shuffleMode = shuffleMode;
            return this;
        }

        public ExecEdge build() {
            return new ExecEdge(source, target, shuffle, shuffleMode);
        }

        private Shuffle fromRequiredDistribution(
                InputProperty.RequiredDistribution requiredDistribution) {
            switch (requiredDistribution.getType()) {
                case ANY:
                    return ANY_SHUFFLE;
                case SINGLETON:
                    return SINGLETON_SHUFFLE;
                case BROADCAST:
                    return BROADCAST_SHUFFLE;
                case HASH:
                    InputProperty.HashDistribution hashDistribution =
                            (InputProperty.HashDistribution) requiredDistribution;
                    return hashShuffle(hashDistribution.getKeys());
                default:
                    throw new TableException(
                            "Unsupported RequiredDistribution type: "
                                    + requiredDistribution.getType());
            }
        }
    }

    /** The {@link Shuffle} defines how to exchange the records between {@link ExecNode}s. */
    public abstract static class Shuffle {
        private final Type type;

        protected Shuffle(Type type) {
            this.type = type;
        }

        public Type getType() {
            return type;
        }

        @Override
        public String toString() {
            return type.name();
        }

        /** Enumeration which describes the shuffle type for records when passing this edge. */
        public enum Type {
            /** Any type of shuffle is OK when passing through this edge. */
            ANY,

            /** Records are shuffled by hash when passing through this edge. */
            HASH,

            /** Full records are provided for each parallelism of the target node. */
            BROADCAST,

            /** Records are shuffled to one node, the parallelism of the target node must be 1. */
            SINGLETON,

            /** Records are shuffled in same parallelism (the shuffle behavior is function call). */
            FORWARD
        }
    }

    /** Records are shuffled by hash when passing through this edge. */
    public static class HashShuffle extends Shuffle {
        private final int[] keys;

        public HashShuffle(int[] keys) {
            super(Type.HASH);
            this.keys = checkNotNull(keys);
            checkArgument(keys.length > 0, "Hash keys must no be empty.");
        }

        public int[] getKeys() {
            return keys;
        }

        @Override
        public String toString() {
            return "HASH" + Arrays.toString(keys);
        }
    }

    /** Any type of shuffle is OK when passing through this edge. */
    public static final Shuffle ANY_SHUFFLE = new Shuffle(Shuffle.Type.ANY) {};

    /** Full records are provided for each parallelism of the target node. */
    public static final Shuffle BROADCAST_SHUFFLE = new Shuffle(Shuffle.Type.BROADCAST) {};

    /** The parallelism of the target node must be 1. */
    public static final Shuffle SINGLETON_SHUFFLE = new Shuffle(Shuffle.Type.SINGLETON) {};

    /** Records are shuffled in same parallelism (function call). */
    public static final Shuffle FORWARD_SHUFFLE = new Shuffle(Shuffle.Type.FORWARD) {};

    /**
     * Return hash {@link Shuffle}.
     *
     * @param keys hash keys
     */
    public static Shuffle hashShuffle(int[] keys) {
        return new HashShuffle(keys);
    }

    /**
     * Translates this edge into a Flink operator.
     *
     * @param planner The {@link Planner} of the translated Table.
     */
    public Transformation<?> translateToPlan(Planner planner) {
        return source.translateToPlan(planner);
    }
}
