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

package eu.stratosphere.nephele.jobmanager.splitassigner.file;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.instance.AbstractInstance;

/**
 * The file input split list stores the file input splits for an input vertex that are still expected to be consumed.
 * Besides simply storing the splits, the file input split list also computes the distance all {@link AbstractInstance}
 * objects which request a input split and its nearest storage location with respect to the underlying network topology.
 * That way input splits are always given to consuming vertices in a way that data locality is preserved as well as
 * possible.
 * <p>
 * This class is not thread-safe.
 * 
 * @author warneke
 */
public final class FileInputSplitList {

	private Set<FileInputSplit> masterSet = new HashSet<FileInputSplit>();

	private Map<AbstractInstance, Queue<QueueElem>> instanceMap = new HashMap<AbstractInstance, Queue<QueueElem>>();

	private final class QueueElem implements Comparable<QueueElem> {

		final FileInputSplit inputSplit;

		final int distance;

		private QueueElem(final FileInputSplit inputSplit, final int distance) {
			this.inputSplit = inputSplit;
			this.distance = distance;
		}

		private FileInputSplit getInputSplit() {
			return this.inputSplit;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int compareTo(QueueElem o) {
			
			return (this.distance - o.distance);
		}

	}

	synchronized void addSplit(final FileInputSplit fileInputSplit) {

		this.masterSet.add(fileInputSplit);
	}

	synchronized FileInputSplit getNextInputSplit(final AbstractInstance instance) {

		final Queue<QueueElem> instanceSplitList = getInstanceSplitList(instance);

		while (true) {

			final QueueElem candidate = instanceSplitList.poll();
			if (candidate == null) {
				return null;
			}

			if (this.masterSet.remove(candidate.getInputSplit())) {
				//TODO: Remove output
				System.out.println(instance + " receives input split with distance " + candidate.distance);
				return candidate.getInputSplit();
			}

			if (this.masterSet.isEmpty()) {
				return null;
			}
		}
	}

	private Queue<QueueElem> getInstanceSplitList(final AbstractInstance instance) {

		Queue<QueueElem> instanceSplitList = this.instanceMap.get(instance);
		if (instanceSplitList == null) {

			// Create and populate instance specific split list
			instanceSplitList = new PriorityQueue<FileInputSplitList.QueueElem>();
			final Iterator<FileInputSplit> it = this.masterSet.iterator();
			while (it.hasNext()) {

				final FileInputSplit split = it.next();
				final String[] hostNames = split.getHostNames();
				if (hostNames == null) {
					instanceSplitList.add(new QueueElem(split, Integer.MAX_VALUE));

				} else {

					int minDistance = Integer.MAX_VALUE;
					for (int i = 0; i < hostNames.length; ++i) {
						final int distance = instance.getDistance(hostNames[i]);
						System.out.println("Distance between " + instance + " and " + hostNames[i] + " is " + distance);
						if (distance < minDistance) {
							minDistance = distance;
						}
					}

					instanceSplitList.add(new QueueElem(split, minDistance));
				}
			}

			this.instanceMap.put(instance, instanceSplitList);
		}

		return instanceSplitList;
	}
}
