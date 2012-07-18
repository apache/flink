package eu.stratosphere.sopremo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to specify how much outputs have to be provided min. and max.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface OutputCardinality {
	int min() default 1;

	int max() default 1;

	int value() default 1;
}
