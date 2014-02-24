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
package eu.stratosphere.api.common.operators.util;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import com.google.common.base.Preconditions;

/**
 * This holds an actual object containing user defined code.
 */
public class UserCodeObjectWrapper<T> implements UserCodeWrapper<T> {
	private static final long serialVersionUID = 1L;
	
	private final T userCodeObject;
	
	public UserCodeObjectWrapper(T userCodeObject) {
		Preconditions.checkNotNull(userCodeObject, "The user code object may not be null.");
		Preconditions.checkArgument(userCodeObject instanceof Serializable, "User code object is not serializable: " + userCodeObject.getClass().getName());
		
		this.userCodeObject = userCodeObject;
		
		// Remove non serializable objects from the user code object as well as from outer objects
		Object current = userCodeObject;
		try {
			while (null != current) {
				Object newCurrent = null;
				
				/**
				 * Check if the usercode class has custom serialization methods.
				 * (See http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html for details).
				 * We can not guarantee that the user handles serialization correctly in this case.
				 */
				boolean hasCustomSerialization = false;
				Method customSerializer = null;
				Method customDeserializer = null;
				try {
					customSerializer = current.getClass().getDeclaredMethod("writeObject", java.io.ObjectOutputStream.class);
					customDeserializer = current.getClass().getDeclaredMethod("readObject", java.io.ObjectInputStream.class);
				} catch (Exception e) {
					// we can ignore exceptions here.
				}
				
				if (customSerializer != null && customDeserializer != null) {
					hasCustomSerialization = true;
				}

				for (Field f : current.getClass().getDeclaredFields()) {
					f.setAccessible(true);

					if (f.getName().contains("$outer")) {
						newCurrent = f.get(current);
					}

					if (hasCustomSerialization || Modifier.isTransient(f.getModifiers()) || Modifier.isStatic(f.getModifiers())) {
						// field not relevant for serialization
						continue;
					}
					
					Object fieldContents = f.get(current);
					if (fieldContents != null &&  !(fieldContents instanceof Serializable)) {
						throw new RuntimeException("User-defined object " + userCodeObject + " (" + 
							userCodeObject.getClass().getName() + ") contains non-serializable field " +
							f.getName() + " = " + f.get(current));
					}
				}
				current = newCurrent;
			}
		}
		catch (Exception e) {
			// should never happen, since we make the fields accessible.
			// anyways, do not swallow the exception, but report it
			throw new RuntimeException("Could not access the fields of the user defined class while checking for serializability.", e);
		}
	}
	
	@Override
	public T getUserCodeObject(Class<? super T> superClass, ClassLoader cl) {
		return userCodeObject;
	}
	
	@Override
	public T getUserCodeObject() {
		return userCodeObject;
		
	}

	@Override
	public <A extends Annotation> A getUserCodeAnnotation(Class<A> annotationClass) {
		return userCodeObject.getClass().getAnnotation(annotationClass);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Class<? extends T> getUserCodeClass() {
		return (Class<? extends T>) userCodeObject.getClass();
	}
}
