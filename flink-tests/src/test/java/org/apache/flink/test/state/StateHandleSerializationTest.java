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

package org.apache.flink.test.state;

import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.StateHandle;

import org.junit.Test;

import org.reflections.Reflections;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Set;

import static org.junit.Assert.*;

public class StateHandleSerializationTest {

	/**
	 * This test validates that all subclasses of {@link StateHandle} have a proper
	 * serial version UID.
	 */
	@Test
	public void ensureStateHandlesHaveSerialVersionUID() {
		try {
			Reflections reflections = new Reflections("org.apache.flink");

			// check all state handles

			@SuppressWarnings("unchecked")
			Set<Class<?>> stateHandleImplementations = (Set<Class<?>>) (Set<?>)
					reflections.getSubTypesOf(StateHandle.class);

			for (Class<?> clazz : stateHandleImplementations) {
				validataSerialVersionUID(clazz);
			}

			// check all key/value snapshots

			@SuppressWarnings("unchecked")
			Set<Class<?>> kvStateSnapshotImplementations = (Set<Class<?>>) (Set<?>)
					reflections.getSubTypesOf(KvStateSnapshot.class);

			System.out.println(kvStateSnapshotImplementations);
			
			for (Class<?> clazz : kvStateSnapshotImplementations) {
				validataSerialVersionUID(clazz);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	private static void validataSerialVersionUID(Class<?> clazz) {
		// all non-interface types must have a serial version UID
		if (!clazz.isInterface()) {
			assertFalse("Anonymous state handle classes have problematic serialization behavior",
					clazz.isAnonymousClass());

			try {
				Field versionUidField = clazz.getDeclaredField("serialVersionUID");

				// check conditions first via "if" to prevent always constructing expensive error messages 
				if (!(Modifier.isPrivate(versionUidField.getModifiers()) &&
						Modifier.isStatic(versionUidField.getModifiers()) &&
						Modifier.isFinal(versionUidField.getModifiers())))
				{
					fail(clazz.getName() + " - serialVersionUID is not 'private static final'");
				}
			}
			catch (NoSuchFieldException e) {
				fail("State handle implementation '" + clazz.getName() + "' is missing the serialVersionUID");
			}
		}
	}
}
