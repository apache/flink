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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@Internal
public abstract class AbstractMergedChannelStateHandle<
                Info, ChannelHandle extends AbstractChannelStateHandle<Info>>
        implements StateObject {

    private static final long serialVersionUID = 1L;

    /** The original subtask index before rescaling recovery. */
    protected final int subtaskIndex;

    protected final StreamStateHandle delegate;

    protected final long size;

    protected final byte[] serializedChannelOffsets;

    private transient volatile Collection<ChannelHandle> unmergedHandles;

    protected AbstractMergedChannelStateHandle(
            int subtaskIndex,
            StreamStateHandle delegate,
            long size,
            byte[] serializedChannelOffsets) {
        this.subtaskIndex = subtaskIndex;
        this.delegate = delegate;
        this.size = size;
        this.serializedChannelOffsets = serializedChannelOffsets;
    }

    protected AbstractMergedChannelStateHandle(Collection<ChannelHandle> handles) {
        Preconditions.checkState(handles != null && handles.size() > 0);

        Iterator<ChannelHandle> iterator = handles.iterator();
        ChannelHandle handle = iterator.next();

        this.subtaskIndex = handle.getSubtaskIndex();
        this.delegate = handle.getDelegate();

        long sizeAcc = 0;

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);

        try {
            dos.writeInt(handles.size());
        } catch (IOException e) {
            throw new IllegalStateException("Can not serialize Channel offsets !", e);
        }

        sizeAcc += handle.getStateSize();
        serializeChannelOffsets(handle, dos);

        while (iterator.hasNext()) {
            handle = iterator.next();

            Preconditions.checkState(
                    this.subtaskIndex == handle.getSubtaskIndex(),
                    "All channel state handles to be converted to merged channel state handles "
                            + "must have the same subtask index !");
            Preconditions.checkState(
                    this.delegate == handle.getDelegate(),
                    "All channel state handles to be converted to merged channel state handles "
                            + "must have the same delegate StreamStateHandle !");

            sizeAcc += handle.getStateSize();
            serializeChannelOffsets(handle, dos);
        }

        this.size = sizeAcc;
        this.serializedChannelOffsets = bos.toByteArray();
        this.unmergedHandles = Collections.unmodifiableCollection(handles);
    }

    @Override
    public void discardState() throws Exception {
        delegate.discardState();
    }

    @Override
    public long getStateSize() {
        return size; // can not rely on delegate.getStateSize because it can be shared
    }

    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    public StreamStateHandle getDelegate() {
        return delegate;
    }

    public byte[] getSerializedChannelOffsets() {
        return serializedChannelOffsets;
    }

    public List<Info> getInfos() {
        ensureCacheUnmergedHandles();
        return unmergedHandles.stream().map(h -> h.getInfo()).collect(Collectors.toList());
    }

    protected abstract void writeInfo(Info info, DataOutputStream dos) throws IOException;

    protected abstract Info readInfo(DataInputStream dis) throws IOException;

    protected abstract ChannelHandle createUnmergedHandle(
            int subtaskIndex,
            StreamStateHandle delegate,
            Info info,
            long stateSize,
            List<Long> offsets);

    protected Collection<ChannelHandle> getUnmergedHandles() {
        ensureCacheUnmergedHandles();
        return unmergedHandles;
    }

    private void ensureCacheUnmergedHandles() {
        if (unmergedHandles == null) {
            synchronized (this) {
                if (unmergedHandles == null) {
                    try {
                        restoreAndCacheUnmergedHandles();
                    } catch (IOException e) {
                        throw new IllegalStateException("Corrupted serializedChannelOffsets !", e);
                    }
                }
            }
        }
    }

    private void serializeChannelOffsets(ChannelHandle handle, DataOutputStream dos) {
        try {
            writeInfo(handle.getInfo(), dos);
            dos.writeLong(handle.getStateSize());

            dos.writeInt(handle.getOffsets().size());
            for (Long offset : handle.getOffsets()) {
                dos.writeLong(offset);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Can not serialize Channel offsets !", e);
        }
    }

    private void restoreAndCacheUnmergedHandles() throws IOException {
        DataInputStream dis =
                new DataInputStream(new ByteArrayInputStream(serializedChannelOffsets));

        int remainHandle = dis.readInt();
        List<ChannelHandle> handles = new ArrayList<>(remainHandle);

        while (remainHandle-- > 0) {
            Info info = readInfo(dis);
            long stateSize = dis.readLong();

            int remainOffset = dis.readInt();
            List<Long> offsets = new ArrayList<>(remainOffset);
            while (remainOffset-- > 0) {
                offsets.add(dis.readLong());
            }

            handles.add(createUnmergedHandle(subtaskIndex, delegate, info, stateSize, offsets));
        }

        unmergedHandles = Collections.unmodifiableCollection(handles);
    }
}
