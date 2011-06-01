package eu.stratosphere.util.dag.converter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for a {@link NodeConverter} specifying that the child elements should be appended to the result of the
 * current {@link NodeConverter}.
 * 
 * @author Arvid Heise
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface AppendChildren {
	/**
	 * The index from which all child elements should be appended to the current result. If not specified, all children
	 * will be append.
	 * 
	 * @return the index from which the child elements should be appended
	 */
	int fromIndex() default 0;
}
