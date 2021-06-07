package org.example.aws.cc_sqs;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;


@Component
public class SqsClient {
	
	private AmazonSQS amazonSQS;
	
	public AmazonSQS getAmazonSQS() {
		return amazonSQS;
	}


	@PostConstruct
	public void init() {
		amazonSQS = AmazonSQSClientBuilder.standard().withRegion(Regions.fromName("us-west-2")).build();
	}
	
}
