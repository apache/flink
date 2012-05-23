package eu.stratosphere.sopremo.expressions;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to specify hints for the optimizer
 */

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
public @interface OptimizerHints {
	public static final int UNBOUND = Integer.MAX_VALUE;

	boolean iterating() default false;

	int maxNodes() default UNBOUND;

	int minNodes() default 1;

	Scope[] scope() default { Scope.ANY };

	boolean transitive() default false;
}
