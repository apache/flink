/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;


/**
 *
 *
 * @author Stephan Ewen
 */
public class MemorySegmentListIterator implements Iterator<MemorySegment>
{
	private final ArrayList<MemorySegment> source;
	
	public MemorySegmentListIterator(ArrayList<MemorySegment> source)
	{
		this.source = source;
	}
	

	/* (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext()
	{
		return this.source.size() > 0;
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public MemorySegment next() {
		if (hasNext()) {
			MemorySegment seg = this.source.remove(this.source.size() - 1);
			seg.outputView.reset();
			return seg;
		}
		throw new NoSuchElementException();
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
