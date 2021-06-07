package org.example.aws.ee_sqs_camelwithjms;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;


@Component
public class SqsJmsClient {
	
	private AmazonSQS amazonSQS;
	private SQSConnectionFactory sqsConnectionFactory;
	
	public AmazonSQS getAmazonSQS() {
		return amazonSQS;
	}
	public SQSConnectionFactory getSqsConnectionFactory() {
		return sqsConnectionFactory;
	}


	@PostConstruct
	public void init() {
		amazonSQS = AmazonSQSClientBuilder.standard().withRegion(Regions.fromName("us-west-2")).build();
		sqsConnectionFactory = new SQSConnectionFactory(new ProviderConfiguration(), amazonSQS);
	}
	
}
