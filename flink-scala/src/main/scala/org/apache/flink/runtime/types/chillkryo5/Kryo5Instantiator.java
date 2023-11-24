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

package org.apache.flink.runtime.types.chillkryo5;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.objenesis.strategy.InstantiatorStrategy;

import java.io.Serializable;

public class Kryo5Instantiator implements Serializable {
    public Kryo newKryo() { return new Kryo(); }

    /** Use this to set a specific classloader
     */
    public Kryo5Instantiator setClassLoader(final ClassLoader cl) {
        return new Kryo5Instantiator() {
            public Kryo newKryo() {
                Kryo k = Kryo5Instantiator.this.newKryo();
                k.setClassLoader(cl);
                return k;
            }
        };
    }
    /** If true, Kryo will error if it sees a class that has not been registered
     */
    public Kryo5Instantiator setInstantiatorStrategy(final InstantiatorStrategy inst) {
        return new Kryo5Instantiator() {
            public Kryo newKryo() {
                Kryo k = Kryo5Instantiator.this.newKryo();
                k.setInstantiatorStrategy(inst);
                return k;
            }
        };
    }

    /** If true, Kryo keeps a map of all the objects it has seen.
     * this can use a ton of memory on hadoop, but save serialization costs in
     * some cases
     */
    public Kryo5Instantiator setReferences(final boolean ref) {
        return new Kryo5Instantiator() {
            public Kryo newKryo() {
                Kryo k = Kryo5Instantiator.this.newKryo();
                /**
                 * Kryo 2.17, used in storm, has this method returning void,
                 * 2.21 has it returning boolean.
                 * Try not to call the method if you don't need to.
                 */
                if(k.getReferences() != ref) { k.setReferences(ref); }
                return k;
            }
        };
    }

    /** If true, Kryo will error if it sees a class that has not been registered
     */
    public Kryo5Instantiator setRegistrationRequired(final boolean req) {
        return new Kryo5Instantiator() {
            public Kryo newKryo() {
                Kryo k = Kryo5Instantiator.this.newKryo();
                /** Try to avoid calling this method if you don't need to.
                 * We've been burned by binary compatibility with Kryo
                 */
                if(k.isRegistrationRequired() != req) { k.setRegistrationRequired(req); }
                return k;
            }
        };
    }
    /** Use Thread.currentThread().getContextClassLoader() as the ClassLoader where ther newKryo is called
     */
    public Kryo5Instantiator setThreadContextClassLoader() {
        return new Kryo5Instantiator() {
            public Kryo newKryo() {
                Kryo k = Kryo5Instantiator.this.newKryo();
                k.setClassLoader(Thread.currentThread().getContextClassLoader());
                return k;
            }
        };
    }

    public Kryo5Instantiator withRegistrar(final IKryo5Registrar r) {
        return new Kryo5Instantiator() {
            public Kryo newKryo() {
                Kryo k = Kryo5Instantiator.this.newKryo();
                r.apply(k);
                return k;
            }
        };
    }
}
