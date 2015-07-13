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

package org.apache.flink.runtime.operators.testutils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.util.MutableObjectIterator;

/**
 * An iterator that returns the union of a given set of iterators.
 */
public class UnionIterator<E> implements MutableObjectIterator<E> {
	
	private MutableObjectIterator<E> currentSource;
	
	private List<MutableObjectIterator<E>> nextSources;


	public UnionIterator(MutableObjectIterator<E>... iterators) {
		this(new ArrayList<MutableObjectIterator<E>>(Arrays.asList(iterators)));
	}
	
	public UnionIterator(List<MutableObjectIterator<E>> sources) {
		this.currentSource = sources.remove(0);
		this.nextSources = sources;
	}


	@Override
	public E next(E target) throws IOException
	{
		E targetStaging = this.currentSource.next(target);
		if (targetStaging != null) {
			return targetStaging;
		} else {
			if (this.nextSources.size() > 0) {
				this.currentSource = this.nextSources.remove(0);
				return next(target);
			}
			else {
				return null;
			}
		}
	}

	@Override
	public E next() throws IOException
	{
		E targetStaging = this.currentSource.next();
		if (targetStaging != null) {
			return targetStaging;
		} else {
			if (this.nextSources.size() > 0) {
				this.currentSource = this.nextSources.remove(0);
				return next();
			}
			else {
				return null;
			}
		}
	}

}
