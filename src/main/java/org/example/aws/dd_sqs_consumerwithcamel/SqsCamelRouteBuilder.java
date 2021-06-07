package org.example.aws.dd_sqs_consumerwithcamel;

import java.util.Set;

import org.apache.camel.ExchangePattern;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public class SqsCamelRouteBuilder extends RouteBuilder {
	
	@Autowired
	private SqsQueueNameManager sqsQueueNameManager;
	
	@Autowired
	private SqsCamelPredicate sqsCamelPredicate;
	
	
    @Override
    public void configure() throws Exception {
    	/*
    	 * Whenever any custom exception occurs,
    	 * You can choose whether to retry the same operation few more times or not
    	 * If you choose to retry mention the below
    	 * 	  onException(CustomException.class)
    	 * 		.redeliveryPolicy(new RedeliveryPolicy())
    	 * 		.maximumRedeliveries(INT)
    	 * 		.redeliveryDelay(LONG)
    	 * 		.maximumRedeliveryDelay(LONG)
    	 * 		.backOffMultiplier(DOUBLE);
    	 */
    	
    	Set<String> queueNames = sqsQueueNameManager.getAllQueueNames();
    	
    	for(String queueName : queueNames) {
    		String routeUri = "aws-sqs://" + queueName + "?amazonSQSClient=#sqsClient&concurrentConsumers=1&maxMessagesPerPoll=10";
    		//If you want to enable Redrive Policy,
    		//then create SqsRedrivePolicy class with "maxReceiveCount" and "deadLetterTargetArn"
    		//then convert the object in JSON
    		//then append the JSON at the end of the Route URI
    		
    		from(routeUri).choice()
    			.when(sqsCamelPredicate).to(ExchangePattern.InOnly, "bean:sqsCamelDataProcessor?method=execute")
    			.otherwise().to("bean:sqsCamelDataProcessor?method=predicateFails")
    			.end();
    	}
    }
    
}
