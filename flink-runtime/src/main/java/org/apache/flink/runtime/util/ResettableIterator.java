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


package org.apache.flink.runtime.util;


import java.io.IOException;
import java.util.Iterator;


/**
 * The resettable iterator is a specialization of the iterator, allowing to reset the iterator and re-retrieve elements.
 * Whether the iterator is completely reset or only partially depends on the actual implementation.
 */
public interface ResettableIterator<E> extends Iterator<E>
{
	/**
	 * Resets the iterator.
	 */
	public void reset() throws IOException;
	
}
