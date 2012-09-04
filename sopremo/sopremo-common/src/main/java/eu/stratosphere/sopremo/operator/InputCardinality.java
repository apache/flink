package eu.stratosphere.sopremo.operator;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to specify how much inputs have to be provided min. and max.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface InputCardinality {
	int min() default 1;

	int max() default Integer.MAX_VALUE;

	int value() default -1;
}
