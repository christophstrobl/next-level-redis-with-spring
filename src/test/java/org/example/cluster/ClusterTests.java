/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example.cluster;

import static org.hamcrest.core.IsCollectionContaining.*;
import static org.hamcrest.core.IsNot.*;
import static org.junit.Assert.*;
import static org.springframework.data.redis.connection.RedisClusterNode.*;

import java.util.Collection;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Christoph Strobl
 */
@RunWith(SpringRunner.class)
@SpringBootTest(properties = "spring.redis.cluster.nodes=127.0.0.1:30001")
public class ClusterTests {

	@Autowired RedisConnectionFactory connectionFactory;

	@SpringBootApplication
	static class Config {}

	RedisClusterConnection connection;
	StringRedisConnection stringConnection;

	@Before
	public void setUp() {

		connection = connectionFactory.getClusterConnection();
		connection.flushAll();

		stringConnection = new DefaultStringRedisConnection(connection);
	}

	@After
	public void after() {
		connection.close();
	}

	/**
	 * Collect all keys that are available throughout the cluster. <br />
	 * {@literal key-1} is mapped to {@literal [229]} at {@literal 127.0.0.1:30001}. <br />
	 * {@literal key-2} is mapped to {@literal [12422]} at {@literal 127.0.0.1:30003}. <br />
	 */
	@Test
	public void collectAllKeysInCluster() {

		stringConnection.set("key-1", "foo");
		stringConnection.set("key-2", "bar");

		Collection<String> keys = stringConnection.keys("*");

		assertThat(keys, hasItems("key-1", "key-2"));
	}

	/**
	 * Collect all keys from a single node.
	 */
	@Test
	public void readAllKeysFromSingleClusterNode() {

		stringConnection.set("key-1", "foo");
		stringConnection.set("key-2", "bar");
		stringConnection.set("2-key", "bar");

		Collection<String> keys = connection
				.keys(newRedisClusterNode().listeningAt("127.0.0.1", 30003).build(), "*".getBytes()).stream()
				.map((bytes) -> new String(bytes)).collect(Collectors.toList());

		assertThat(keys, hasItems("key-2", "2-key"));
		assertThat(keys, not(hasItems("key-1")));
	}

	/**
	 * Execute commands that do not match a single slot.<br />
	 * {@literal key-1} is mapped to {@literal [229]} at {@literal 127.0.0.1:30001}. <br />
	 * {@literal key-2} is mapped to {@literal [12422]} at {@literal 127.0.0.1:30003}. <br />
	 */
	@Test
	public void executeCrossSlotCommands() {

		stringConnection.set("key-1", "foo");
		stringConnection.set("key-2", "bar");

		assertThat(stringConnection.mGet("key-1", "key-2"), hasItems("foo", "bar"));
	}

}
