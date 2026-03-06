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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.bitmap.Bitmap;

import java.io.IOException;

/** Serializer for {@link Bitmap}. */
@Internal
public class BitmapSerializer extends TypeSerializerSingleton<Bitmap> {

    public static final BitmapSerializer INSTANCE = new BitmapSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public Bitmap createInstance() {
        return Bitmap.empty();
    }

    @Override
    public Bitmap copy(Bitmap from) {
        return Bitmap.from(from);
    }

    @Override
    public Bitmap copy(Bitmap from, Bitmap reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(Bitmap record, DataOutputView target) throws IOException {
        byte[] bytes = record.toBytes();
        target.writeInt(bytes.length);
        target.write(bytes);
    }

    @Override
    public Bitmap deserialize(DataInputView source) throws IOException {
        int length = source.readInt();
        byte[] bytes = new byte[length];
        source.read(bytes);
        return Bitmap.fromBytes(bytes);
    }

    @Override
    public Bitmap deserialize(Bitmap reuse, DataInputView source) throws IOException {
        return this.deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int length = source.readInt();
        target.writeInt(length);
        target.write(source, length);
    }

    @Override
    public TypeSerializerSnapshot<Bitmap> snapshotConfiguration() {
        return new BitmapSerializerSnapshot();
    }

    @Internal
    public static final class BitmapSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<Bitmap> {
        /** Constructor to create snapshot from serializer (writing the snapshot). */
        public BitmapSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
