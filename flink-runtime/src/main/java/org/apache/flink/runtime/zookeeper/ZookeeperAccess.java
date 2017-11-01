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

package org.apache.flink.runtime.zookeeper;

import org.apache.zookeeper.KeeperException;

import java.util.ConcurrentModificationException;

/**
 * Utility class providing access to relocated zookeeper classes.
 *
 * <p>This class is necessary as flink-runtime relocates its ZooKeeper dependency.
 * Other modules may still depend on this dependency but will encounter a ClassNotFoundException
 * on access as they don't apply the relocation pattern of flink-runtime.
 */
public final class ZookeeperAccess {
	
	private ZookeeperAccess(){
	}

	/**
	 * Wraps and returns the given exception in a {@link ConcurrentModificationException} if it is a
	 * {@link org.apache.zookeeper.KeeperException.NodeExistsException}. Otherwise the
	 * given exception is returned.
	 *
	 * @param ex exception to wrap
	 * @return wrapping ConcurrentModificationException if it is a NodeExistsException, otherwise the given exception
	 */
	public static Exception wrapIfZooKeeperNodeExistsException(Exception ex) {
		if (ex instanceof KeeperException.NodeExistsException) {
			return new ConcurrentModificationException("ZooKeeper unexpectedly modified", ex);
		}
		return ex;
	}

	/**
	 * Wraps and returns the given exception in a {@link ConcurrentModificationException} if it is a
	 * {@link org.apache.zookeeper.KeeperException.NoNodeException}. Otherwise the
	 * given exception is returned.
	 *
	 * @param ex exception to wrap
	 * @return wrapping ConcurrentModificationException if it is a NoNodeException, otherwise the given exception
	 */
	public static Exception wrapIfZooKeeperNoNodeException(Exception ex) {
		if (ex instanceof KeeperException.NoNodeException) {
			return new ConcurrentModificationException("ZooKeeper unexpectedly modified", ex);
		}
		return ex;
	}
}
