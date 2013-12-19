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
import java.lang.reflect.Modifier;

import org.apache.commons.lang3.SerializationUtils;

import com.google.common.base.Preconditions;

/**
 * This holds an actual object containing user defined code.
 *
 */
public class UserCodeObjectWrapper<T> implements UserCodeWrapper<T> {
	private static final long serialVersionUID = 1L;
	
	private T userCodeObject;
	
	public UserCodeObjectWrapper(T userCodeObject) {
		Preconditions.checkArgument(userCodeObject instanceof Serializable, "User code object is not serializable: " + userCodeObject.getClass());
		this.userCodeObject = userCodeObject;
                // Remove unserializable objects from the user code object as well as from outer objects
        Object current = userCodeObject;
        try {
            while (null != current) {
                Object newCurrent = null;
                for (Field f : current.getClass().getDeclaredFields()) {
                    f.setAccessible(true);

                    if (f.getName().contains("$outer")) {
                        newCurrent = f.get(current);

                    }

                    if (!Modifier.isStatic(f.getModifiers()) && f.get(current) != null &&  !(f.get(current) instanceof Serializable)) {
                        throw new RuntimeException("User code object " +
                                userCodeObject + " contains non-serializable field " + f.getName() + " = " + f.get(current));
                    }
                }
                current = newCurrent;
            }
        } catch (IllegalAccessException e) {
            // this cannot occur since we call setAccessible(true)
            e.printStackTrace();
        }

	}
	
	@Override
	public T getUserCodeObject(Class<? super T> superClass, ClassLoader cl) {
		return getUserCodeObject();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public T getUserCodeObject() {
		// return a clone because some code retrieves this and runs configure() on it before
		// the job is actually run. This way we can always hand out a pristine copy.
		Serializable ser = (Serializable) userCodeObject;
		T cloned = (T) SerializationUtils.clone(ser);
		return cloned;
		
	}

	@Override
	public <A extends Annotation> A getUserCodeAnnotation(
			Class<A> annotationClass) {
		return userCodeObject.getClass().getAnnotation(annotationClass);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Class<? extends T> getUserCodeClass() {
		return (Class<? extends T>) userCodeObject.getClass();
	}
}
