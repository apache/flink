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

package org.apache.flink.runtime.types;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.twitter.chill.IKryoRegistrar;
import com.twitter.chill.SingleRegistrar;

import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.PriorityQueue;

/*
This code is copied as is from Twitter Chill 0.7.4 because we need to user a newer chill version
but want to ensure that the serializers that are registered by default stay the same.

The only changes to the code are those that are required to make it compile and pass checkstyle
checks in our code base.
 */

class PriorityQueueSerializer extends Serializer<PriorityQueue<?>> {
    private Field compField;

    public static IKryoRegistrar registrar() {
        return new SingleRegistrar(PriorityQueue.class, new PriorityQueueSerializer());
    }

    public PriorityQueueSerializer() {
        try {
            compField = PriorityQueue.class.getDeclaredField("comparator");
            compField.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Comparator<?> getComparator(PriorityQueue<?> q) {
        try {
            return (Comparator<?>) compField.get(q);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void write(Kryo k, Output o, PriorityQueue<?> q) {
        k.writeClassAndObject(o, getComparator(q));
        o.writeInt(q.size(), true);
        for (Object a : q) {
            k.writeClassAndObject(o, a);
            o.flush();
        }
    }

    public PriorityQueue<?> read(Kryo k, Input i, Class<PriorityQueue<?>> c) {
        Comparator<Object> comp = (Comparator<Object>) k.readClassAndObject(i);
        int sz = i.readInt(true);
        // can't create with size 0:
        PriorityQueue<Object> result;
        if (sz == 0) {
            result = new PriorityQueue<Object>(1, comp);
        } else {
            result = new PriorityQueue<Object>(sz, comp);
        }
        int idx = 0;
        while (idx < sz) {
            result.add(k.readClassAndObject(i));
            idx += 1;
        }
        return result;
    }
}
