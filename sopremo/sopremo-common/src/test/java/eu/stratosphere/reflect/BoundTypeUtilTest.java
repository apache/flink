package eu.stratosphere.reflect;

import static org.junit.Assert.assertArrayEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import org.junit.Test;

import eu.stratosphere.util.reflect.BoundType;
import eu.stratosphere.util.reflect.BoundTypeUtil;

/**
 * Tests the {@link BoundTypeUtil} class, especially {@link BoundTypeUtil#getStaticBoundTypes(Class)}
 * 
 * @author Arvid Heise
 */
public class BoundTypeUtilTest {
	/**
	 * 
	 */
	@Test
	public void testBoundOrder() {
		final Class<?>[] klasses = { String.class, Integer.class };
		assertArrayEquals(BoundType.arrayOf(klasses), BoundTypeUtil.getStaticBoundTypes(BoundMap.class));
	}

	/**
	 * 
	 */
	@Test
	public void testCompletedBounds() {
		final Class<?>[] klasses = { Integer.class, String.class };
		assertArrayEquals(BoundType.arrayOf(klasses), BoundTypeUtil.getStaticBoundTypes(CompletedBoundMap.class));
	}

	/**
	 * 
	 */
	@Test
	public void testEmptyBounds() {
		// no type parameters
		assertArrayEquals(new Class[0], BoundTypeUtil.getStaticBoundTypes(BoundTypeUtilTest.class));

		// not statically bound
		assertArrayEquals(new Class[0], BoundTypeUtil.getStaticBoundTypes(ArrayList.class));
	}

	/**
	 * 
	 */
	@Test
	public void testMultipleNestingBounds() {
		final BoundType stringArrayListLinkedList = BoundType.of(LinkedList.class,
			BoundType.of(ArrayList.class, BoundType.of(String.class)));
		final BoundType integerArrayListHashMap = BoundType.of(HashMap.class,
			array(BoundType.of(ArrayList.class, BoundType.of(Integer.class)),
				BoundType.of(NestedList.class)));
		assertArrayEquals(array(stringArrayListLinkedList, integerArrayListHashMap),
			BoundTypeUtil.getStaticBoundTypes(NestedMap.class));
	}

	/**
	 * 
	 */
	@Test
	public void testNestedBounds() {
		final BoundType stringArrayList = BoundType.of(ArrayList.class, BoundType.of(String.class));
		assertArrayEquals(bindToArray(LinkedList.class, stringArrayList),
			BoundTypeUtil.getStaticBoundTypes(NestedList.class));
	}

	/**
	 * 
	 */
	@Test
	public void testPartialBounds() {
		final Class<?>[] klasses = { Integer.class };
		assertArrayEquals(BoundType.arrayOf(klasses), BoundTypeUtil.getStaticBoundTypes(PartialBoundMap.class));
	}

	/**
	 * 
	 */
	@Test
	public void testSimpleBounds() {
		final Class<?>[] klasses = { String.class };
		assertArrayEquals(BoundType.arrayOf(klasses), BoundTypeUtil.getStaticBoundTypes(BoundList.class));
	}

	private static <T> T[] array(final T... ts) {
		return ts;
	}

	private static BoundType[] bindToArray(final Class<?> klass, final BoundType... types) {
		return new BoundType[] { BoundType.of(klass, types) };
	}

	@SuppressWarnings("serial")
	private static class BoundList extends ArrayList<String> {
		// no reimplementation since only the static binding of types is tested
	}

	@SuppressWarnings("serial")
	private static class BoundMap extends HashMap<String, Integer> {
		// no reimplementation since only the static binding of types is tested
	}

	@SuppressWarnings("serial")
	private static class CompletedBoundMap extends PartialBoundMap<String> {
		// no reimplementation since only the static binding of types is tested
	}

	@SuppressWarnings("serial")
	private static class NestedList extends ArrayList<LinkedList<ArrayList<String>>> {
		// no reimplementation since only the static binding of types is tested
	}

	@SuppressWarnings("serial")
	private static class NestedMap extends
			HashMap<LinkedList<ArrayList<String>>, HashMap<ArrayList<Integer>, NestedList>> {
		// no reimplementation since only the static binding of types is tested
	}

	@SuppressWarnings("serial")
	private static class PartialBoundMap<T> extends HashMap<T, Integer> {
		// no reimplementation since only the static binding of types is tested
	}

}
