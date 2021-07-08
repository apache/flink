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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Either;

import java.io.IOException;

import static org.apache.flink.types.Either.Left;
import static org.apache.flink.types.Either.Right;

/**
 * A {@link TypeSerializer} for the {@link Either} type of the Java class.
 *
 * @param <L> the Left value type
 * @param <R> the Right value type
 */
@Internal
public class EitherSerializer<L, R> extends TypeSerializer<Either<L, R>> {

    private static final long serialVersionUID = 1L;

    private final TypeSerializer<L> leftSerializer;

    private final TypeSerializer<R> rightSerializer;

    public EitherSerializer(TypeSerializer<L> leftSerializer, TypeSerializer<R> rightSerializer) {
        this.leftSerializer = leftSerializer;
        this.rightSerializer = rightSerializer;
    }

    // ------------------------------------------------------------------------
    //  Accessors
    // ------------------------------------------------------------------------

    public TypeSerializer<R> getRightSerializer() {
        return rightSerializer;
    }

    public TypeSerializer<L> getLeftSerializer() {
        return leftSerializer;
    }

    // ------------------------------------------------------------------------
    //  TypeSerializer methods
    // ------------------------------------------------------------------------

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<Either<L, R>> duplicate() {
        TypeSerializer<L> duplicateLeft = leftSerializer.duplicate();
        TypeSerializer<R> duplicateRight = rightSerializer.duplicate();

        if ((leftSerializer != duplicateLeft) || (rightSerializer != duplicateRight)) {
            // stateful
            return new EitherSerializer<L, R>(duplicateLeft, duplicateRight);
        } else {
            return this;
        }
    }

    @Override
    public Either<L, R> createInstance() {
        // We arbitrarily always create a Right value instance.
        return Right(rightSerializer.createInstance());
    }

    @Override
    public Either<L, R> copy(Either<L, R> from) {
        if (from.isLeft()) {
            L left = from.left();
            L copyLeft = leftSerializer.copy(left);
            return Left(copyLeft);
        } else {
            R right = from.right();
            R copyRight = rightSerializer.copy(right);
            return Right(copyRight);
        }
    }

    @Override
    public Either<L, R> copy(Either<L, R> from, Either<L, R> reuse) {
        if (from.isLeft()) {
            Left<L, R> to = Either.obtainLeft(reuse, leftSerializer);
            L left = leftSerializer.copy(from.left(), to.left());
            to.setValue(left);
            return to;
        } else {
            Right<L, R> to = Either.obtainRight(reuse, rightSerializer);
            R right = rightSerializer.copy(from.right(), to.right());
            to.setValue(right);
            return to;
        }
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(Either<L, R> record, DataOutputView target) throws IOException {
        if (record.isLeft()) {
            target.writeBoolean(true);
            leftSerializer.serialize(record.left(), target);
        } else {
            target.writeBoolean(false);
            rightSerializer.serialize(record.right(), target);
        }
    }

    @Override
    public Either<L, R> deserialize(DataInputView source) throws IOException {
        boolean isLeft = source.readBoolean();
        if (isLeft) {
            return Left(leftSerializer.deserialize(source));
        } else {
            return Right(rightSerializer.deserialize(source));
        }
    }

    @Override
    public Either<L, R> deserialize(Either<L, R> reuse, DataInputView source) throws IOException {
        boolean isLeft = source.readBoolean();
        if (isLeft) {
            Left<L, R> to = Either.obtainLeft(reuse, leftSerializer);
            L left = leftSerializer.deserialize(to.left(), source);
            to.setValue(left);
            return to;
        } else {
            Right<L, R> to = Either.obtainRight(reuse, rightSerializer);
            R right = rightSerializer.deserialize(to.right(), source);
            to.setValue(right);
            return to;
        }
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        boolean isLeft = source.readBoolean();
        target.writeBoolean(isLeft);
        if (isLeft) {
            leftSerializer.copy(source, target);
        } else {
            rightSerializer.copy(source, target);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof EitherSerializer) {
            EitherSerializer<L, R> other = (EitherSerializer<L, R>) obj;

            return leftSerializer.equals(other.leftSerializer)
                    && rightSerializer.equals(other.rightSerializer);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 17 * leftSerializer.hashCode() + rightSerializer.hashCode();
    }

    // ------------------------------------------------------------------------
    // Serializer configuration snapshotting & compatibility
    // ------------------------------------------------------------------------

    @Override
    public JavaEitherSerializerSnapshot<L, R> snapshotConfiguration() {
        return new JavaEitherSerializerSnapshot<>(this);
    }
}
