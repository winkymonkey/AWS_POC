package org.example.aws.bb_elasticache;

import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Service;


@Service
public class RedisService {
	
	@Autowired
	private StringRedisTemplate stringRedisTemplate;
	
	
	
	/* **************************************************************************************************** */
	/**
	 * All Key Operations
	 */
	public void doAllKeyOperations() {
		//get all keys
		Set<String> allKeys = stringRedisTemplate.keys("*");
		allKeys.forEach(key -> System.out.println(key));
		
		//get all keys in specific pattern
		Set<String> allKeysInPattern = stringRedisTemplate.keys("myPattern*");
		allKeysInPattern.forEach(key -> System.out.println(key));
		
		//Know the type of a key
		DataType dataType = stringRedisTemplate.type("myKey");
		System.out.println(dataType);
		
		//....
	}
	
	
	
	/* **************************************************************************************************** */
	/**
	 * All String Operations
	 */
	public void doAllStringOperations() {
		ValueOperations<String, String> stringOps = stringRedisTemplate.opsForValue();
		
		//set a single key
		stringOps.set("myKey", "myVal");
		
		//Get value of a single key
		stringOps.get("myKey");
		
		//...
	}
	
	
	
	/* **************************************************************************************************** */
	/**
	 * All Hash Operations
	 */
	public void doAllHashOperations() {
		HashOperations<String, String, String> hashOps = stringRedisTemplate.opsForHash();
		
		//set a field-value for a key
		hashOps.put("Test:key", "myField", "myValue");
		
		//get a field-value for a key
		Map<String, String> entries = hashOps.entries("Test:key");
		entries.forEach((field,val) -> System.out.println("["+field+"]&&["+val+"]"));
		
		//.....
	}
	
	
	
	/* **************************************************************************************************** */
	/**
	 * All List Operations
	 */
	public void doAllListOperations() {
		ListOperations<String, String> listOps = stringRedisTemplate.opsForList();
		
		//Push a value (at head)
		listOps.leftPush("myKey", "val");
		
		//Push a value (at tail)
		listOps.rightPush("myKey2", "val2");
		
		//...
	}
	
}
