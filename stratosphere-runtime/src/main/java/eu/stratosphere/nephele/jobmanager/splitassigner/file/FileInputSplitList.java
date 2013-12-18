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

package eu.stratosphere.nephele.jobmanager.splitassigner.file;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.core.fs.FileInputSplit;
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

	/**
	 * The logging object which is used to report information and errors.
	 */
	private static final Log LOG = LogFactory.getLog(FileInputSplitList.class);

	/**
	 * The set containing all the file input splits that still must be consumed.
	 */
	private Set<FileInputSplit> masterSet = new HashSet<FileInputSplit>();

	/**
	 * The map caching the specific file input split lists for each {@link AbstractInstance}.
	 */
	private Map<AbstractInstance, Queue<QueueElem>> instanceMap = new HashMap<AbstractInstance, Queue<QueueElem>>();

	/**
	 * This is an auxiliary class to store the minimum distance between a file input split's storage locations and an
	 * {@link AbstractInstance}.
	 * 
	 * @author warneke
	 */
	private final class QueueElem implements Comparable<QueueElem> {

		/**
		 * The file input split the distance applies to.
		 */
		final FileInputSplit inputSplit;

		/**
		 * The minimum distance between the file input split's storage locations and the instance this object has been
		 * created for.
		 */
		final int distance;

		/**
		 * Creates a new queue element.
		 * 
		 * @param inputSplit
		 *        the file input split to be stored
		 * @param distance
		 *        the minimum distance between the stored input split's storage locations and the instance this object
		 *        has been created for
		 */
		private QueueElem(final FileInputSplit inputSplit, final int distance) {
			this.inputSplit = inputSplit;
			this.distance = distance;
		}

		/**
		 * Returns the file input split stored within this object.
		 * 
		 * @return the file input split
		 */
		private FileInputSplit getInputSplit() {
			return this.inputSplit;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int compareTo(final QueueElem o) {

			return (this.distance - o.distance);
		}

	}

	/**
	 * Adds the given file input split to the set of file input splits to be consumed.
	 * 
	 * @param fileInputSplit
	 *        the file input split to be added
	 */
	synchronized void addSplit(final FileInputSplit fileInputSplit) {

		this.masterSet.add(fileInputSplit);
	}

	/**
	 * Returns the next file input split to be consumed by the given instance. The returned input split is selected in a
	 * way that the distance between the split's storage location and the requesting {@link AbstractInstance} is as
	 * short as possible.
	 * 
	 * @param instance
	 *        the instance requesting the next file input split
	 * @return the next input split to be consumed by the given instance or <code>null</code> if all input splits have
	 *         already been consumed.
	 */
	synchronized FileInputSplit getNextInputSplit(final AbstractInstance instance) {

		final Queue<QueueElem> instanceSplitList = getInstanceSplitList(instance);

		while (true) {

			final QueueElem candidate = instanceSplitList.poll();
			if (candidate == null) {
				return null;
			}

			if (this.masterSet.remove(candidate.getInputSplit())) {
				if (LOG.isInfoEnabled()) {
					if (candidate.distance == 0) {
						LOG.info(instance + " receives local file input split");
					} else {
						LOG.info(instance + " receives remote file input split (distance " + candidate.distance + ")");
					}
				}
				return candidate.getInputSplit();
			}

			if (this.masterSet.isEmpty()) {
				return null;
			}
		}
	}

	/**
	 * Returns a list of file input splits specifically ordered for the given {@link AbstractInstance}. When the list is
	 * initially created, it contains all the unconsumed file input splits at that point in time, ascendingly ordered by
	 * the minimum distance between the input splits' storage locations and the given {@link AbstractInstance}.
	 * 
	 * @param instance
	 *        the instance for which the file input split list has been computed
	 * @return the list of file input splits ordered specifically for the given instance
	 */
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
						if (LOG.isDebugEnabled()) {
							LOG.debug("Distance between " + instance + " and " + hostNames[i] + " is " + distance);
						}
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
