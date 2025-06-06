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
package org.apache.flink.table.api.runtime.types

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}

/*
This code is copied from the Twitter Chill project and modified to use Kryo 5.x directly.
 */

private class SymbolSerializer extends Serializer[Symbol] {
  setImmutable(true)

  override def write(k: Kryo, out: Output, obj: Symbol) { out.writeString(obj.name) }
  override def read(k: Kryo, in: Input, cls: Class[_ <: Symbol]) = Symbol(in.readString)
}
