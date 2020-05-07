package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collections;
import java.util.List;

public class TypeExtractorContractTest {

	@Test
	public void testMaterializeTypeVariableToActualType() {

		final List<ParameterizedType> parameterizedTypes =
			TypeExtractor.buildParameterizedTypeHierarchy(MySimpleUDF.class, BaseInterface.class, true);

		final ParameterizedType normalInterfaceType = parameterizedTypes.get(parameterizedTypes.size() - 1);
		final TypeVariable firstTypeVariableOfNormalInterface = (TypeVariable) normalInterfaceType.getActualTypeArguments()[0];
		final TypeVariable secondTypeVariableOfNormalInterface = (TypeVariable) normalInterfaceType.getActualTypeArguments()[1];

		final Type materializedFirstTypeVariable = TypeExtractor.materializeTypeVariable(parameterizedTypes, firstTypeVariableOfNormalInterface);
		final ParameterizedType materializedSecondTypeVariable = (ParameterizedType) TypeExtractor.materializeTypeVariable(parameterizedTypes, secondTypeVariableOfNormalInterface);

		Assert.assertEquals(Integer.class, materializedFirstTypeVariable);
		Assert.assertEquals(String.class, materializedSecondTypeVariable.getActualTypeArguments()[0]);
		Assert.assertEquals(Integer.class, materializedSecondTypeVariable.getActualTypeArguments()[1]);
		Assert.assertEquals(Tuple2.class, materializedSecondTypeVariable.getRawType());
	}

	@Test
	public void testMaterializeTypeVariableToBottomTypeVariable() {
		final SimpleUDF myUdf = new SimpleUDF<String, Integer, Long, String>();

		final List<ParameterizedType> parameterizedTypes =
			TypeExtractor.buildParameterizedTypeHierarchy(myUdf.getClass(), BaseInterface.class, true);

		final ParameterizedType normalInterfaceType = parameterizedTypes.get(parameterizedTypes.size() - 1);

		final TypeVariable firstTypeVariableOfNormalInterface = (TypeVariable) normalInterfaceType.getActualTypeArguments()[0];
		final TypeVariable secondTypeVariableOfNormalInterface = (TypeVariable) normalInterfaceType.getActualTypeArguments()[1];

		final ParameterizedType abstractSimpleUDFType = parameterizedTypes.get(0);

		final TypeVariable firstTypeVariableOfAbstractSimpleUDF = (TypeVariable) abstractSimpleUDFType.getActualTypeArguments()[0];
		final TypeVariable secondTypeVariableOfAbstractSimpleUDF = (TypeVariable) abstractSimpleUDFType.getActualTypeArguments()[1];
		final Type materializedFirstTypeVariable =
			TypeExtractor.materializeTypeVariable(parameterizedTypes, firstTypeVariableOfNormalInterface);
		final Type materializedSecondTypeVariable =
			TypeExtractor.materializeTypeVariable(parameterizedTypes, secondTypeVariableOfNormalInterface);

		Assert.assertEquals(firstTypeVariableOfAbstractSimpleUDF, materializedFirstTypeVariable);
		Assert.assertEquals(secondTypeVariableOfAbstractSimpleUDF, materializedSecondTypeVariable);

	}

	// --------------------------------------------------------------------------------------------
	// Test build parameterized type hierarchy.
	// --------------------------------------------------------------------------------------------

	@Test
	public void testBuildParameterizedTypeHierarchyForSimpleType() {
		final List<ParameterizedType> parameterizedTypeHierarchy =
			TypeExtractor.buildParameterizedTypeHierarchy(MySimpleUDF.class, BaseInterface.class, true);
		final ParameterizedType normalInterfaceType = parameterizedTypeHierarchy.get(parameterizedTypeHierarchy.size() - 1);

		Assert.assertEquals(4, parameterizedTypeHierarchy.size());
		Assert.assertEquals(SimpleUDF.class, parameterizedTypeHierarchy.get(0).getRawType());
		Assert.assertEquals(AbstractSimpleUDF.class, parameterizedTypeHierarchy.get(1).getRawType());
		Assert.assertEquals(RichInterface.class, parameterizedTypeHierarchy.get(2).getRawType());

		Assert.assertEquals(NormalInterface.class, normalInterfaceType.getRawType());
		Assert.assertTrue(normalInterfaceType.getActualTypeArguments()[0] instanceof TypeVariable);
		Assert.assertTrue(normalInterfaceType.getActualTypeArguments()[1] instanceof TypeVariable);
	}

	@Test
	public void testBuildParameterizedTypeHierarchyForCompositeType() {
		final List<ParameterizedType> typeHierarchy =
			TypeExtractor.buildParameterizedTypeHierarchy(MyCompositeUDF.class, BaseInterface.class, true);
		final ParameterizedType normalInterfaceType = typeHierarchy.get(typeHierarchy.size() - 1);

		Assert.assertTrue(normalInterfaceType.getActualTypeArguments()[0] instanceof TypeVariable);
		Assert.assertTrue(normalInterfaceType.getActualTypeArguments()[1] instanceof TypeVariable);
	}

	@Test
	public void testBuildParameterizedTypeHierarchyOnlyFromSuperClass() {
		final List<ParameterizedType> parameterizedTypeHierarchy =
			TypeExtractor.buildParameterizedTypeHierarchy(MySimpleUDF.class, Object.class, false);

		Assert.assertEquals(2, parameterizedTypeHierarchy.size());
		Assert.assertEquals(SimpleUDF.class, parameterizedTypeHierarchy.get(0).getRawType());
		Assert.assertEquals(AbstractSimpleUDF.class, parameterizedTypeHierarchy.get(1).getRawType());
	}

	@Test
	public void testBuildParameterizedTypeHierarchyWithoutInheritance() {
		final List<ParameterizedType> parameterizedTypeHierarchy =
			TypeExtractor.buildParameterizedTypeHierarchy(MySimpleUDF.class, TypeExtractorContractTest.class, true);
		Assert.assertEquals(Collections.emptyList(), parameterizedTypeHierarchy);
	}

	// --------------------------------------------------------------------------------------------
	// Test resolve type
	// --------------------------------------------------------------------------------------------

	@Test
	public void testResolveSimpleType() {
		final List<ParameterizedType> typeHierarchy =
			TypeExtractor.buildParameterizedTypeHierarchy(MySimpleUDF.class, BaseInterface.class, true);
		final ParameterizedType normalInterfaceType = typeHierarchy.get(typeHierarchy.size() - 1);

		final ParameterizedType resolvedNormalInterfaceType =
			(ParameterizedType) TypeExtractor.resolveTypeFromTypeHierarchy(normalInterfaceType, typeHierarchy, true);

		Assert.assertEquals(Integer.class, resolvedNormalInterfaceType.getActualTypeArguments()[0]);

		final ParameterizedType secondResolvedType = (ParameterizedType) resolvedNormalInterfaceType.getActualTypeArguments()[1];

		Assert.assertEquals(String.class, secondResolvedType.getActualTypeArguments()[0]);
		Assert.assertEquals(Integer.class, secondResolvedType.getActualTypeArguments()[1]);
	}

	@Test
	public void testResolveCompositeType() {
		final List<ParameterizedType> typeHierarchy =
			TypeExtractor.buildParameterizedTypeHierarchy(MyCompositeUDF.class, BaseInterface.class, true);
		final ParameterizedType normalInterfaceType = typeHierarchy.get(typeHierarchy.size() - 1);
		final ParameterizedType resolvedNormalInterfaceType =
			(ParameterizedType) TypeExtractor.resolveTypeFromTypeHierarchy(normalInterfaceType, typeHierarchy, true);

		Assert.assertEquals(String.class, resolvedNormalInterfaceType.getActualTypeArguments()[0]);

		final ParameterizedType resolvedTuple2Type = (ParameterizedType) resolvedNormalInterfaceType.getActualTypeArguments()[1];

		Assert.assertEquals(Tuple2.class, resolvedTuple2Type.getRawType());
		Assert.assertEquals(Integer.class, resolvedTuple2Type.getActualTypeArguments()[0]);
		Assert.assertEquals(Boolean.class, resolvedTuple2Type.getActualTypeArguments()[1]);
	}

	@Test
	public void testResolveGenericArrayType() {
		final List<ParameterizedType> typeHierarchy =
			TypeExtractor.buildParameterizedTypeHierarchy(MyGenericArrayUDF.class, BaseInterface.class, true);

		final ParameterizedType normalInterfaceType = typeHierarchy.get(typeHierarchy.size() - 1);

		final ParameterizedType resolvedNormalInterfaceType =
			(ParameterizedType) TypeExtractor.resolveTypeFromTypeHierarchy(normalInterfaceType, typeHierarchy, true);

		final GenericArrayType secondResolvedType = (GenericArrayType) resolvedNormalInterfaceType.getActualTypeArguments()[1];

		Assert.assertEquals(String.class, resolvedNormalInterfaceType.getActualTypeArguments()[0]);
		Assert.assertEquals(Integer.class, secondResolvedType.getGenericComponentType());
	}

	@Test
	public void testDoesNotResolveGenericArrayType() {
		final List<ParameterizedType> typeHierarchy =
			TypeExtractor.buildParameterizedTypeHierarchy(MyGenericArrayUDF.class, BaseInterface.class, true);

		final ParameterizedType normalInterfaceType = typeHierarchy.get(typeHierarchy.size() - 1);

		final ParameterizedType resolvedNormalInterfaceType =
			(ParameterizedType) TypeExtractor.resolveTypeFromTypeHierarchy(normalInterfaceType, typeHierarchy, false);

		final GenericArrayType genericArrayType = (GenericArrayType) resolvedNormalInterfaceType.getActualTypeArguments()[1];
		Assert.assertEquals(String.class, resolvedNormalInterfaceType.getActualTypeArguments()[0]);
		Assert.assertTrue(genericArrayType.getGenericComponentType() instanceof TypeVariable);

	}
	// --------------------------------------------------------------------------------------------
	// Basic interfaces.
	// --------------------------------------------------------------------------------------------

	interface BaseInterface {
	}

	interface NormalInterface<X, Y> extends BaseInterface{
		Y foo(X x);
	}

	interface RichInterface<X, Y, Z> extends NormalInterface<X, Y> {
		void open(X x, Y y, Z z);
	}

	// --------------------------------------------------------------------------------------------
	// Generic type does not have composite type.
	// --------------------------------------------------------------------------------------------

	abstract class AbstractSimpleUDF<X, Y, Z> implements RichInterface<X, Y, Z> {

		@Override
		public void open(X x, Y y, Z z) {
		}

		@Override
		public Y foo(X x) {
			return null;
		}

		public abstract void bar();
	}

	class SimpleUDF<X, Y, Z, K> extends AbstractSimpleUDF<X, Y, Z> {

		K k;

		@Override
		public void bar() {
		}
	}

	class MySimpleUDF extends SimpleUDF<Integer, Tuple2<String, Integer>, Tuple2<Boolean, Double>, Long> {
	}

	// --------------------------------------------------------------------------------------------
	// Generic type has composite type.
	// --------------------------------------------------------------------------------------------

	interface CompositeUDF<X, Y, Z> extends RichInterface<X, Tuple2<Y, Z>, X> {

	}

	abstract class AbstractCompositeUDF<X> implements CompositeUDF<String, X, Boolean> {

		@Override
		public Tuple2<X, Boolean> foo(String s) {
			return null;
		}

		@Override
		public void open(String s, Tuple2<X, Boolean> xBooleanTuple2, String s2) {

		}

		public abstract void bar();

	}

	class MyCompositeUDF extends AbstractCompositeUDF<Integer> {
		@Override
		public void bar() {

		}
	}

	// --------------------------------------------------------------------------------------------
	// Generic type has generic array.
	// --------------------------------------------------------------------------------------------

	interface GenericArrayUDF<X, Y, Z> extends RichInterface<X, Y[], Z> {

	}

	abstract class AbstractGenericArrayUDF<X> implements GenericArrayUDF<String, X, Boolean> {

		@Override
		public X[] foo(String s) {
			return null;
		}

		@Override
		public void open(String s, X[] xes, Boolean aBoolean) {

		}

		public abstract void bar();

	}

	class MyGenericArrayUDF extends AbstractGenericArrayUDF<Integer> {

		@Override
		public void bar() {

		}
	}
}
