package org.example.aws.aa_dynamodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;


@Service
public class DynamoService {
	
	@Autowired
	private DynamoClient dynamoClient;
	
	private static final String tableName = "Movies";
	private static final int YEAR_VAL = 2015;
	private static final String TITLE_VAL = "Inception";
	
	/* ***************************************
	{
		year: 2015,				//Hash Key
		title: "Inception",		//Range Key
		info: {
			plot: "Nothing happens at all.",
			rating: 9
		},
		category: ["drama", "thriller"],
		duration: 160,
		director: "Christopher Nolan",
		heroes: ["hero1", "hero2"]
	}
	*************************************** */
	
	
	
	/* **************************************************************************************************** */
	/* 										 			CREATE
	/* **************************************************************************************************** */
	/**
	 * CREATE an Item
	 */
	public void createItem() {
		Map<String, Object> infoMap = new HashMap<String, Object>();
		infoMap.put("plot", "Inception");
        infoMap.put("rating", 9);
        List<String> categoryList = new ArrayList<>();
        categoryList.add("drama");
        categoryList.add("thriller");
		
        Item item = new Item()
					.withPrimaryKey("year", YEAR_VAL, "title", TITLE_VAL)
					.withMap("info", infoMap)
					.withList("category", categoryList)
					.withLong("duration", 160)
					.withString("director", "Christopher Nolan");
		
        Table table = dynamoClient.getDynamoDB().getTable(tableName);
        PutItemOutcome outcome = table.putItem(item);
		System.out.println(outcome.getPutItemResult());
	}
	
	
	
	
	/* **************************************************************************************************** */
	/* 										 		GET, QUERY
	/* **************************************************************************************************** */
	/**
	 * GET one Item
	 */
	public void getItem() {
		GetItemSpec getItemSpec = new GetItemSpec()
									.withPrimaryKey("year", YEAR_VAL, "title", TITLE_VAL);
		
		Table table = dynamoClient.getDynamoDB().getTable(tableName);
		Item item = table.getItem(getItemSpec);
		System.out.println(item.toJSONPretty());
	}
	
	
	/**
	 * QUERY the table for list of Items
	 */
	public void queryTable() {
        QuerySpec querySpec = new QuerySpec()
        						.withKeyConditionExpression("year = :yyyy")
        						.withValueMap(new ValueMap()
        											.withInt(":yyyy", 1985));
        
        Table table = dynamoClient.getDynamoDB().getTable(tableName);
        ItemCollection<QueryOutcome> itemCollection = table.query(querySpec);
        itemCollection.forEach(item -> System.out.println(item.toJSONPretty()));
	}
	
	
	/**
	 * Query GSI named "directorIndex"
	 * Hash Key--director
	 * Range Key--duration
	 */
	public void queryIndex() {
		QuerySpec querySpec = new QuerySpec()
								.withKeyConditionExpression("duration = :v_duration and begins_with(director, :v_director)")
								.withValueMap(new ValueMap()
												.withNumber(":v_duration", 160)
												.withString(":v_director", "Chris"));
		
		Table table = dynamoClient.getDynamoDB().getTable(tableName);
		ItemCollection<QueryOutcome> itemCollection = table.getIndex("CreateDateIndex").query(querySpec);
		itemCollection.forEach(item -> System.out.println(item.toJSONPretty()));
	}
	
	
	/**
	 * KeyConditionExpression
	 *   https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
	 *  
	 * FilterExpression
	 *   https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
	 */
	
	
	
	/* **************************************************************************************************** */
	/* 										 		UPDATE
	/* **************************************************************************************************** */
	/**
	 * Update an Item
	 * Use 'withUpdateExpression()'
	 */
	public void updateItem() {
		UpdateItemSpec updateItemSpec = new UpdateItemSpec()
										.withPrimaryKey("year", YEAR_VAL, "title", TITLE_VAL)
										.withUpdateExpression("set info.plot=:v_plot, info.rating=:v_rating, info.categories=:v_categories")
										.withValueMap(new ValueMap()
														.withString(":v_plot", "Everything happens all at once.")
														.withNumber(":v_rating", 8.5)
														.withList(":v_categories", Arrays.asList("A+", "A-", "B-")))
										.withReturnValues(ReturnValue.UPDATED_NEW);
		
		Table table = dynamoClient.getDynamoDB().getTable(tableName);
		UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
		System.out.println(outcome.getUpdateItemResult());
	}
	
	
	/**
	 * UpdateExpressions
	 * https://docs.aws.amazon.com/en_pv/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html
	 */
	
	
	/**
	 * Update an Item's atomic counter
	 */
	public void updateItem_atomicCounter() {
		UpdateItemSpec updateItemSpec = new UpdateItemSpec()
										.withPrimaryKey("year", YEAR_VAL, "title", TITLE_VAL)
										.withUpdateExpression("set info.rating = info.rating + :v_counter")
										.withValueMap(new ValueMap().withNumber(":v_counter", 1))
										.withReturnValues(ReturnValue.UPDATED_NEW);
		
		Table table = dynamoClient.getDynamoDB().getTable(tableName);
		UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
		System.out.println(outcome.getUpdateItemResult());
	}
	
	
	/**
	 * Update an Item conditionally
	 * Use 'withConditionExpression()'
	 */
	public void updateItem_conditionExpression() {
		UpdateItemSpec updateItemSpec = new UpdateItemSpec()
										.withPrimaryKey("year", YEAR_VAL, "title", TITLE_VAL)
										.withUpdateExpression("remove info.actors[0]")
										.withConditionExpression("size(info.actors) > :num")
										.withValueMap(new ValueMap()
														.withNumber(":num", 2))
										.withReturnValues(ReturnValue.UPDATED_NEW);
		
		Table table = dynamoClient.getDynamoDB().getTable(tableName);
		UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
		System.out.println(outcome.getUpdateItemResult());
	}
	
	
	/**
	 * When updating multiple fields, the use of 'withUpdateExpression()' is suitable.
	 * Rather use 'addAttributeUpdate()' method.
	 */
	public void updateItem_multipleFieldUpdate() {
		Map<String, String> attributes = new HashMap<>();
		attributes.put("key1", "value1");
		attributes.put("key2", "value2");
		attributes.put("key3", "value3");
		
		UpdateItemSpec updateItemSpec = new UpdateItemSpec()
										.withPrimaryKey("year", YEAR_VAL, "title", TITLE_VAL)
										.withReturnValues(ReturnValue.UPDATED_NEW);
		
		for(Map.Entry<String, String> attribute : attributes.entrySet()) {
			updateItemSpec.addAttributeUpdate(new AttributeUpdate(attribute.getKey()).put(attribute.getValue()));
		}
		
		Table table = dynamoClient.getDynamoDB().getTable(tableName);
		UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
		System.out.println(outcome.getUpdateItemResult());
	}
	
	
	
	/* **************************************************************************************************** */
	/* 										 		DELETE
	/* **************************************************************************************************** */
	/**
	 * Delete an Item
	 */
	public void deleteItem() {
		DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
										.withPrimaryKey("year", YEAR_VAL, "title", TITLE_VAL)
										.withConditionExpression("info.rating <= :val")
										.withValueMap(new ValueMap()
														.withNumber(":val", 8.5));
		
		Table table = dynamoClient.getDynamoDB().getTable(tableName);
		DeleteItemOutcome outcome = table.deleteItem(deleteItemSpec);
		System.out.println(outcome.getDeleteItemResult());
	}
	
}
