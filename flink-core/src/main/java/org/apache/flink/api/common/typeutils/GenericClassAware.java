package org.apache.flink.api.common.typeutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * The {@link org.apache.flink.api.common.typeinfo.TypeInformation} could implement
 * this interface to tell the framework the generic class corresponding to the
 * {@link TypeInformation#getGenericParameters()}.
 */
public interface GenericClassAware {
	/**
	 * @return the generic class corresponding to the {@link TypeInformation#getGenericParameters()}
	 */
	Class<?> getGenericClass();
}
