/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.test.util;

import java.io.IOException;
import java.util.List;

import eu.stratosphere.util.MutableObjectIterator;


/**
 * An iterator that returns the union of a given set of iterators.
 */
public class UnionIterator<E> implements MutableObjectIterator<E>
{
	private MutableObjectIterator<E> currentSource;
	
	private List<MutableObjectIterator<E>> nextSources;
	
	public UnionIterator(List<MutableObjectIterator<E>> sources)
	{
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
}