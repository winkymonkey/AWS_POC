package org.example.aws.dd_sqs_consumerwithcamel;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;


@Component
public class SqsCamelClient {
	
	private AmazonSQS amazonSQS;
	
	public AmazonSQS getAmazonSQS() {
		return amazonSQS;
	}


	@PostConstruct
	public void init() {
		amazonSQS = AmazonSQSClientBuilder.standard().withRegion(Regions.fromName("us-west-2")).build();
	}
	
}
