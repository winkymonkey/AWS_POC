package org.example.aws.ff_sns;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.SetSubscriptionAttributesRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;


@Service
public class SnsService {
	
	@Autowired
	private SnsClient snsClient;
	
	
	
	/* **************************************************************************************************** */
	/**
	 * Create a Topic
	 */
	public void createTopic() {
		final CreateTopicRequest createTopicRequest = new CreateTopicRequest("MyTopic");
		final CreateTopicResult createTopicResult = snsClient.getAmazonSNS().createTopic(createTopicRequest);

		System.out.println("TopicArn:" + createTopicResult.getTopicArn());
		System.out.println("CreateTopicRequest: " + snsClient.getAmazonSNS().getCachedResponseMetadata(createTopicRequest));
	}
	
	
	
	/* **************************************************************************************************** */
	/**
	 * Subscribe endpoint to a Topic
	 */
	public void subscribeEndpointToTopic() {
		String topicArn = "arn:aws:sns:us-east-2:123456789012:MyTopic";

		final SubscribeRequest subscribeRequest = new SubscribeRequest(topicArn, "email", "name@example.com");
		snsClient.getAmazonSNS().subscribe(subscribeRequest);

		System.out.println("SubscribeRequest: " + snsClient.getAmazonSNS().getCachedResponseMetadata(subscribeRequest));
		System.out.println("To confirm the subscription, check your email.");
	}
	
	
	
	/* **************************************************************************************************** */
	/**
	 * Publish message to Topic
	 */
	public void publishMessage() {
		String topicArn = "arn:aws:sns:us-east-2:123456789012:MyTopic";

		final String msg = "If you receive this message, publishing a message to an Amazon SNS topic works.";
		final PublishRequest publishRequest = new PublishRequest(topicArn, msg);
		final PublishResult publishResult = snsClient.getAmazonSNS().publish(publishRequest);

		String messageId = publishResult.getMessageId();
		System.out.println("MessageId: " + messageId);
	}
	
	
	/**
	 * Publish message (with attributes) to Topic
	 */
	public void publishMessage_withAttributes() {
		String topicArn = "arn:aws:sns:us-east-2:123456789012:MyTopic";
		String messageBody = "This is my new message";
		
		final SNSMessageAttributes messageAttributes = new SNSMessageAttributes(messageBody);
		messageAttributes.addAttribute("store", "example_corp");														// "String" attribute
		messageAttributes.addAttribute("event", "order_placed");														// "String" attribute
		messageAttributes.addAttribute("customer_interests", Arrays.asList(new String[] {"soccer","rugby","hockey"}));	// "String.Array" attribute
		messageAttributes.addAttribute("price_usd", 1000);																// "Numeric" attribute
		messageAttributes.addAttribute("encrypted", Arrays.asList(new Boolean[] {false}));								// "Boolean" attribute for filtering using subscription filter policies

		String messageId = messageAttributes.publish(snsClient.getAmazonSNS(), topicArn);
		System.out.println("MessageId: " + messageId);
	}
	
	
	
	/* **************************************************************************************************** */
	/**
	 * Set the specified SQS queue as a dead-letter-queue of the specified SNS subscription.
	 */
	public void configureDlq_forSubscription() {
		String subscriptionArn = "arn:aws:sns:us-east-2:123456789012:MyEndpoint:1234a567-bc89-012d-3e45-6fg7h890123i";

		SetSubscriptionAttributesRequest request = new SetSubscriptionAttributesRequest()
													.withSubscriptionArn(subscriptionArn)
													.withAttributeName("RedrivePolicy")
													.withAttributeValue("{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-2:123456789012:MyDeadLetterQueue\"}");
		snsClient.getAmazonSNS().setSubscriptionAttributes(request);
	}
	
}
