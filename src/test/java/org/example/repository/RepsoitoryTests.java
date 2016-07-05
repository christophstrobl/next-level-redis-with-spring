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
package org.example.repository;

import static org.hamcrest.collection.IsCollectionWithSize.*;
import static org.hamcrest.collection.IsEmptyCollection.*;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.*;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.hamcrest.core.IsNot.*;
import static org.junit.Assert.*;
import static org.springframework.data.redis.core.PartialUpdate.*;

import java.util.Arrays;
import java.util.List;

import org.example.types.Address;
import org.example.types.Gender;
import org.example.types.Person;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisKeyExpiredEvent;
import org.springframework.data.redis.core.RedisKeyValueAdapter.EnableKeyspaceEvents;
import org.springframework.data.redis.core.RedisKeyValueTemplate;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Christoph Strobl
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class RepsoitoryTests<K, V> {

	@SpringBootApplication
	@EnableRedisRepositories(considerNestedRepositories = true, enableKeyspaceEvents = EnableKeyspaceEvents.ON_STARTUP)
	static class Config {

		/**
		 * An {@link ApplicationListener} that captures {@link RedisKeyExpiredEvent}s and just prints the value to the
		 * console.
		 *
		 * @return
		 */
		@Bean
		ApplicationListener<RedisKeyExpiredEvent<Person>> eventListener() {
			return event -> {
				System.out.println(String.format("Received expire event for key=%s with value %s.",
						new String(event.getSource()), event.getValue()));
			};
		}
	}

	@Autowired PersonRepository repo;
	@Autowired RedisKeyValueTemplate template;
	@Autowired RedisConnectionFactory connectionFactory;

	/*
	 * Set of test users
	 */
	Person eddard = new Person("eddard", "stark", Gender.MALE);
	Person robb = new Person("robb", "stark", Gender.MALE);
	Person sansa = new Person("sansa", "stark", Gender.FEMALE);
	Person arya = new Person("arya", "stark", Gender.FEMALE);
	Person bran = new Person("bran", "stark", Gender.MALE);
	Person rickon = new Person("rickon", "stark", Gender.MALE);
	Person jon = new Person("jon", "snow", Gender.MALE);

	@Before
	public void setUp() {

		RedisConnection connection = connectionFactory.getConnection();
		connection.flushAll();
		connection.close();
	}

	/**
	 * Find entity by a single {@link Indexed} property value.
	 */
	@Test
	public void findBySingleProperty() {

		flushTestUsers();

		List<Person> starks = repo.findByLastname(eddard.getLastname());

		assertThat(starks, containsInAnyOrder(eddard, robb, sansa, arya, bran, rickon));
		assertThat(starks, not(hasItem(jon)));
	}

	/**
	 * Find entities by multiple {@link Indexed} properties using {@literal AND}.
	 */
	@Test
	public void findByMultiplePropertiesUsingAnd() {

		flushTestUsers();

		List<Person> aryaStark = repo.findByFirstnameAndLastname(arya.getFirstname(), arya.getLastname());

		assertThat(aryaStark, hasItem(arya));
		assertThat(aryaStark, not(hasItems(eddard, robb, sansa, bran, rickon, jon)));
	}

	/**
	 * Find entities by multiple {@link Indexed} properties using {@literal OR}.
	 */
	@Test
	public void findByMultiplePropertiesUsingOr() {

		flushTestUsers();

		List<Person> aryaAndJon = repo.findByFirstnameOrLastname(arya.getFirstname(), jon.getLastname());

		assertThat(aryaAndJon, containsInAnyOrder(arya, jon));
		assertThat(aryaAndJon, not(hasItems(eddard, robb, sansa, bran, rickon)));
	}

	/**
	 * Find entity by a single {@link Indexed} property on an embedded entity.
	 */
	@Test
	public void findByEmbeddedProperty() {

		Address winterfell = new Address();
		winterfell.setCountry("the north");
		winterfell.setCity("winterfell");

		eddard.setAddress(winterfell);

		flushTestUsers();

		List<Person> eddardStark = repo.findByAddress_City(winterfell.getCity());

		assertThat(eddardStark, hasItem(eddard));
		assertThat(eddardStark, not(hasItems(robb, sansa, arya, bran, rickon, jon)));
	}

	/**
	 * Partially update an entity by just setting the firstname. Have indexes updated accordingly.
	 */
	@Test
	public void updateEntity() {

		jon.setFirstname("john");

		flushTestUsers();

		assertThat(repo.findOne(jon.getId()), is(jon));

		template.update(newPartialUpdate(jon.getId(), Person.class).set("firstname", "jon"));

		assertThat(repo.findByFirstnameAndLastname("jon", "snow"), hasSize(1));
		assertThat(repo.findByFirstnameAndLastname("john", "snow"), is(empty()));
	}

	/**
	 * Set expiration of an entity to 1 second and verify the expiration listener is receives the event correctly.
	 */
	@Test
	public void reveiveExpirationEvent() throws InterruptedException {

		jon.setTtl(1L);

		flushTestUsers();

		Thread.sleep(2500);

		assertThat(repo.findByFirstnameAndLastname("jon", "snow"), is(empty()));
	}

	/**
	 * Store references to other entites without embedding all data. <br />
	 * Print out the hash structure within Redis.
	 */
	@Test
	public void useReferencesToStoreDataToOtherObjects() {

		flushTestUsers();

		eddard.setChildren(Arrays.asList(jon, robb, sansa, arya, bran, rickon));

		repo.save(eddard);

		Person laoded = repo.findOne(eddard.getId());
		assertThat(laoded.getChildren(), hasItems(jon, robb, sansa, arya, bran, rickon));

		/*
		 * Deceased:
		 *
		 * - Robb was killed by Roose Bolton during the Red Wedding.
		 * - Jon was stabbed by brothers or the Night's Watch.
		 */
		repo.delete(Arrays.asList(robb, jon));

		laoded = repo.findOne(eddard.getId());
		assertThat(laoded.getChildren(), hasItems(sansa, arya, bran, rickon));
		assertThat(laoded.getChildren(), not(hasItems(robb, jon)));
	}

	private void flushTestUsers() {
		repo.save(Arrays.asList(eddard, robb, sansa, arya, bran, rickon, jon));
	}

	/**
	 * Simple {@link CrudRepository} to be picked up by the Redis repository support.
	 *
	 * @author Christoph Strobl
	 */
	static interface PersonRepository extends CrudRepository<Person, String> {

		List<Person> findByLastname(String lastname);

		List<Person> findByFirstnameAndLastname(String firstname, String lastname);

		List<Person> findByFirstnameOrLastname(String firstname, String lastname);

		List<Person> findByAddress_City(String city);
	}
}
