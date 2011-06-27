package eu.stratosphere.sopremo.expressions;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
public @interface OptimizerHints {
	public static final int UNBOUND = Integer.MAX_VALUE;

	boolean iterating() default false;

	boolean transitive() default false;

	Scope[] scope() default { Scope.ANY };

	int minNodes() default 1;

	int maxNodes() default UNBOUND;
}
