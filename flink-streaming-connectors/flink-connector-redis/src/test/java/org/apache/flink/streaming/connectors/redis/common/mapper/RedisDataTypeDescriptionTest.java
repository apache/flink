package org.apache.flink.streaming.connectors.redis.common.mapper;

import org.apache.flink.streaming.connectors.redis.RedisSinkTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RedisDataTypeDescriptionTest {

	@Test(expected=IllegalArgumentException.class)
	public void shouldThrowExceptionIfAdditionalKeyIsNotGivenForHashDataType(){
		RedisSinkTest.RedisDataMapper redisDataMapper = new RedisSinkTest
			.RedisDataMapper(RedisDataType.HASH);
		redisDataMapper.getDataTypeDescription();
	}

	@Test
	public void shouldReturnNullForAdditionalDataType(){
		RedisSinkTest.RedisDataMapper redisDataMapper = new RedisSinkTest
			.RedisDataMapper(RedisDataType.LIST);
		RedisDataTypeDescription redisDataTypeDescription = redisDataMapper.getDataTypeDescription();
		assertEquals(RedisDataType.LIST, redisDataTypeDescription.getDataType());
		assertNull(redisDataTypeDescription.getAdditionalKey());
	}
}
