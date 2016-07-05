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
package org.example.ohm;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.core.IsNot.*;
import static org.junit.Assert.*;

import java.util.Map;
import java.util.stream.Collectors;

import org.example.types.Address;
import org.example.types.Gender;
import org.example.types.Person;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.hash.BeanUtilsHashMapper;
import org.springframework.data.redis.hash.DecoratingStringHashMapper;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.data.redis.hash.Jackson2HashMapper;
import org.springframework.data.redis.hash.ObjectHashMapper;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Christoph Strobl
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class ObjectHashMappingTests {

	@SpringBootApplication
	static class Config {

		/**
		 * The {@link HashMapper} to use. There's a {@link Jackson2HashMapper}, {@link BeanUtilsHashMapper},
		 * {@link DecoratingStringHashMapper} and the {@link ObjectHashMapper} we use in this sample.
		 *
		 * @return a new {@link ObjectHashMapper}.
		 */
		@Bean
		ObjectHashMapper mapper() {
			return new ObjectHashMapper();
		}
	}

	@Autowired RedisConnectionFactory connectionFactory;
	@Autowired HashMapper<Object, byte[], byte[]> mapper;

	RedisConnection connection;
	StringRedisConnection stringConnection;

	@Before
	public void setUp() {

		connection = connectionFactory.getConnection();
		connection.flushAll();

		stringConnection = new DefaultStringRedisConnection(connection);
	}

	@After
	public void after() {
		connection.close();
	}

	/**
	 * Map a complex object to a hash, store and retrieve it.
	 */
	@Test
	public void mapToFromHash() {

		Address winterfell = new Address();
		winterfell.setCity("winterfell");
		winterfell.setCountry("the north");

		Person jonSnow = new Person("jon", "snow", Gender.MALE);
		jonSnow.setId("123");
		jonSnow.setAddress(winterfell);

		Map<String, String> rawHash = mapper.toHash(jonSnow).entrySet().stream()
				.collect(Collectors.toMap(e -> new String(e.getKey()), e -> new String(e.getValue())));

		System.out.println(rawHash);

		stringConnection.hMSet("person:123", rawHash);

		Person fromHash = (Person) mapper.fromHash(stringConnection.hGetAll("person:123".getBytes()));

		assertThat(fromHash, is(equalTo(jonSnow)));
	}

	/**
	 * Update properties of a complex object within the hash and retrieve it.
	 */
	@Test
	public void manipulateHashAndReadItBack() {

		Address winterfell = new Address();
		winterfell.setCity("winterfell");
		winterfell.setCountry("the north");

		Person jonSnow = new Person("jon", "snow", Gender.MALE);
		jonSnow.setId("123");
		jonSnow.setAddress(winterfell);

		stringConnection.hMSet("person:123", mapper.toHash(jonSnow).entrySet().stream()
				.collect(Collectors.toMap(e -> new String(e.getKey()), e -> new String(e.getValue()))));

		stringConnection.hSet("person:123", "lastname", "doe");

		Person fromHash = (Person) mapper.fromHash(connection.hGetAll("person:123".getBytes()));

		System.out.println(fromHash);

		assertThat(fromHash, is(not(equalTo(jonSnow))));
	}
}
