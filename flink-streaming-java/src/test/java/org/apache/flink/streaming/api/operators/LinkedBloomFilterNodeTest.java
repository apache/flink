//package org.apache.flink.streaming.api.operators;
//
//import org.apache.flink.core.memory.DataInputViewStreamWrapper;
//import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
//
//import org.junit.Assert;
//import org.junit.Test;
//
//import java.io.ByteArrayInputStream;
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.util.concurrent.TimeUnit;
//
///**
// * {@link ElasticBloomFilterNode} unit tests.
// */
//public class LinkedBloomFilterNodeTest {
//
//	@Test
//	public void basicTest() throws InterruptedException {
//
//		ElasticBloomFilterNode node = new ElasticBloomFilterNode(100, 0.01, 1000);
//
//		Assert.assertEquals(Long.MAX_VALUE, node.getDeleteTS());
//
//		for (int i = 0; i < 100; ++i) {
//			node.add(String.valueOf(i).getBytes());
//			Assert.assertTrue(node.contains(String.valueOf(i).getBytes()));
//		}
//		Assert.assertTrue(node.isFull());
//		TimeUnit.MILLISECONDS.sleep(1000);
//		Assert.assertNotEquals(Long.MAX_VALUE, node.getDeleteTS());
//	}
//
//	@Test
//	public void testSnapshotAndRestore() throws InterruptedException, IOException {
//
//		ElasticBloomFilterNode node1 = new ElasticBloomFilterNode(100, 0.01, 1000);
//		for (int i = 0; i < 100; ++i) {
//			node1.add(String.valueOf(i).getBytes());
//			Assert.assertTrue(node1.contains(String.valueOf(i).getBytes()));
//		}
//
//		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//		DataOutputViewStreamWrapper outputViewStreamWrapper = new DataOutputViewStreamWrapper(outputStream);
//
//		node1.snapshot(outputViewStreamWrapper);
//
//		byte[] outputBytes = outputStream.toByteArray();
//
//		ElasticBloomFilterNode node2 = new ElasticBloomFilterNode();
//		node2.restore(new DataInputViewStreamWrapper(new ByteArrayInputStream(outputBytes)));
//
//		Assert.assertEquals(node1.getCapacity(), node2.getCapacity());
//		Assert.assertEquals(String.valueOf(node1.getFpp()), String.valueOf(node2.getFpp()));
//		Assert.assertEquals(node1.getSize(), node2.getSize());
//		Assert.assertEquals(node1.getBloomFilter(), node2.getBloomFilter());
//	}
//}
