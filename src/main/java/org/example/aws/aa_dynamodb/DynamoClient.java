package org.example.aws.aa_dynamodb;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;


@Component
public class DynamoClient {
	
	private DynamoDB dynamoDB;
	
	public DynamoDB getDynamoDB() {
		return dynamoDB;
	}


	@PostConstruct
	public void init() {
		AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.fromName("us-west-2")).build();
		dynamoDB = new DynamoDB(amazonDynamoDB);
	}
	
	
	
	
	/*
	private static final String EXTERNAL_ID = "externalId";
	private static final String STS_ASSUME_ROLE = "StsAssumeRole";
	private static final String ASSUME_ROLE_ARN = "arn:aws:iam::111188884444:role/this-is-assume-role";
	
	// Create dynamoDb object with assumeRole
	// Requires 'aws-java-sdk-sts' dependency for STS 
	@PostConstruct
	public void init() {
		STSAssumeRoleSessionCredentialsProvider provider = new STSAssumeRoleSessionCredentialsProvider.Builder(ASSUME_ROLE_ARN, STS_ASSUME_ROLE)
																.withRoleSessionDurationSeconds(1 * 60 * 60)
																.withExternalId(EXTERNAL_ID)
																.build();
		AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.fromName("us-west-2")).withCredentials(provider).build();
		dynamoDB = new DynamoDB(amazonDynamoDB);
	}
	*/
	
}
