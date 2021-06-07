package org.example.aws.cc_sqs;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


@Service
public class SqsService {

	@Autowired
	private SqsClient sqsClient;
	
	
	
	/* **************************************************************************************************** */
	/**
	 * Create a Standard queue
	 */
	public void createQueue_StandardQueue() {
		String queueName = "TestQueue";
		
		CreateQueueRequest request = new CreateQueueRequest(queueName);
		String queueUrl = sqsClient.getAmazonSQS().createQueue(request).getQueueUrl();
		System.out.println(queueUrl);
	}
	
	
	/**
	 * Create a FIFO queue
	 */
	public void createQueue_FifoQueue() {
		String queueName = "TestQueue.fifo";
		Map<String, String> attributes = new HashMap<String, String>();
		attributes.put("FifoQueue", "true");					//A FIFO queue must have the FifoQueue attribute set to True
		attributes.put("ContentBasedDeduplication", "true");	//If the user doesn't provide a MessageDeduplicationId, generate a MessageDeduplicationId based on the content.
		
		CreateQueueRequest request = new CreateQueueRequest(queueName);
		String queueUrl = sqsClient.getAmazonSQS().createQueue(request).getQueueUrl();
		System.out.println(queueUrl);
	}
	
	
	/**
	 * Create a FIFO queue
	 * With SSE enabled (using AWS CMK)
	 */
	public void createQueue_FifoQueue_SSEenabled_AwsCmk() {
		String queueName = "TestQueue.fifo";
		Map<String, String> attributes = new HashMap<String, String>();
		attributes.put("FifoQueue", "true");
		attributes.put("ContentBasedDeduplication", "true");
		attributes.put("KmsMasterKeyId", "arn:aws:kms:us-east-2:123456789012:alias/aws/sqs");	//Enable SSE by specifying the alias ARN of the AWS CMK 
		attributes.put("KmsDataKeyReusePeriodSeconds", "60");									//(OPTIONAL) Specify the length of time, in seconds, for which SQS can reuse the CMK before calling AWS KMS again
		
		CreateQueueRequest request = new CreateQueueRequest(queueName);
		String queueUrl = sqsClient.getAmazonSQS().createQueue(request).getQueueUrl();
		System.out.println(queueUrl);
	}
	
	
	/**
	 * Create a FIFO queue
	 * With SSE enabled (using Custom CMK)
	 */
	public void createQueue_FifoQueue_SSEenabled_CustomCmk() {
		String queueName = "TestQueue.fifo";
		Map<String, String> attributes = new HashMap<String, String>();
		attributes.put("FifoQueue", "true");
		attributes.put("ContentBasedDeduplication", "true");
		attributes.put("KmsMasterKeyId", "arn:aws:kms:us-east-2:123456789012:alias/MyAlias");	//Enable SSE by specifying the alias ARN of the AWS CMK 
		attributes.put("KmsDataKeyReusePeriodSeconds", "864000");								//(OPTIONAL) Specify the length of time, in seconds, for which SQS can reuse the CMK before calling AWS KMS again
		
		CreateQueueRequest request = new CreateQueueRequest(queueName);
		String queueUrl = sqsClient.getAmazonSQS().createQueue(request).getQueueUrl();
		System.out.println(queueUrl);
	}
	
	
	
	
	/* **************************************************************************************************** */
	/**
	 * List all queues
	 */
	public void listAllQueues() {
		for (String queueUrl : sqsClient.getAmazonSQS().listQueues().getQueueUrls()) {
			System.out.println(queueUrl);
		}
	}
	
	
	
	
	/* **************************************************************************************************** */
	/**
	 * Send message in a Standard Queue
	 */
	public void sendMessage_StandardQueue() {
		String queueUrl = "https://sqs.us-west-2.amazonaws.com/<ACCOUNT>/<QUEUE_NAME>";
		String messageTest = "This is my message text.";

		SendMessageRequest request = new SendMessageRequest(queueUrl, messageTest);
		SendMessageResult sendMessageResult = sqsClient.getAmazonSQS().sendMessage(request);
		System.out.println(sendMessageResult.getMessageId());
	}
	
	
	/**
	 * Send message in a FIFO Queue
	 */
	public void sendMessage_FifoQueue() {
		String queueUrl = "https://sqs.us-west-2.amazonaws.com/<ACCOUNT>/<QUEUE_NAME>.fifo";
		String messageTest = "This is my message text.";

		SendMessageRequest request = new SendMessageRequest(queueUrl, messageTest)
													.withMessageGroupId("myGroupId");
		SendMessageResult sendMessageResult = sqsClient.getAmazonSQS().sendMessage(request);
		System.out.println(sendMessageResult.getMessageId());
		System.out.println(sendMessageResult.getSequenceNumber());
	}
	
	
	/**
	 * Send message(with message attributes) in a Standard Queue
	 */
	public void sendMessageWithAttributes_StandardQueue() {
		String queueUrl = "https://sqs.us-west-2.amazonaws.com/<ACCOUNT>/<QUEUE_NAME>";
		String messageTest = "This is my message text.";
		
		Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
		messageAttributes.put("Name", new MessageAttributeValue().withDataType("String").withStringValue("Jane")); 									// Attribute Type--String
		messageAttributes.put("AccurateWeight", new MessageAttributeValue().withDataType("Number").withStringValue("230.000000000000000001"));		// Attribute Type--Number
		messageAttributes.put("ByteArray", new MessageAttributeValue().withDataType("Binary").withBinaryValue(ByteBuffer.wrap(new byte[10]))); 		// Attribute Type--Binary
		messageAttributes.put("EmployeeId", new MessageAttributeValue().withDataType("String.EmployeeId").withStringValue("ABC123456")); 					// Attribute Type--String(custom)
		messageAttributes.put("AccountId", new MessageAttributeValue().withDataType("Number.AccountId").withStringValue("000123456"));						// Attribute Type--Number(custom)
		messageAttributes.put("ApplicationIcon", new MessageAttributeValue().withDataType("Binary.JPEG").withBinaryValue(ByteBuffer.wrap(new byte[10])));	// Attribute Type--Binary(custom)

		SendMessageRequest request = new SendMessageRequest(queueUrl, messageTest)
													.withMessageAttributes(messageAttributes);
		SendMessageResult sendMessageResult = sqsClient.getAmazonSQS().sendMessage(request);
		System.out.println(sendMessageResult.getMessageId());
	}
	
	
	/**
	 * Send message(with message attributes) in a FIFO Queue
	 */
	public void sendMessageWithAttributes_FifoQueue() {
		String queueUrl = "https://sqs.us-west-2.amazonaws.com/<ACCOUNT>/<QUEUE_NAME>.fifo";
		String messageTest = "This is my message text.";
		
		Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
		messageAttributes.put("Name", new MessageAttributeValue().withDataType("String").withStringValue("Jane")); 									// Attribute Type--String
		messageAttributes.put("AccurateWeight", new MessageAttributeValue().withDataType("Number").withStringValue("230.000000000000000001"));		// Attribute Type--Number
		messageAttributes.put("ByteArray", new MessageAttributeValue().withDataType("Binary").withBinaryValue(ByteBuffer.wrap(new byte[10]))); 		// Attribute Type--Binary
		messageAttributes.put("EmployeeId", new MessageAttributeValue().withDataType("String.EmployeeId").withStringValue("ABC123456")); 					// Attribute Type--String(custom)
		messageAttributes.put("AccountId", new MessageAttributeValue().withDataType("Number.AccountId").withStringValue("000123456"));						// Attribute Type--Number(custom)
		messageAttributes.put("ApplicationIcon", new MessageAttributeValue().withDataType("Binary.JPEG").withBinaryValue(ByteBuffer.wrap(new byte[10])));	// Attribute Type--Binary(custom)
		
		SendMessageRequest request = new SendMessageRequest(queueUrl, messageTest)
													.withMessageAttributes(messageAttributes)
													.withMessageGroupId("myGroupId");
		SendMessageResult sendMessageResult = sqsClient.getAmazonSQS().sendMessage(request);
		System.out.println(sendMessageResult.getMessageId());
		System.out.println(sendMessageResult.getSequenceNumber());
	}
	
	
	
	
	/* **************************************************************************************************** */
	/**
	 * Receive message from a queue
	 * In case of FIFO queue, you can mention it like 
	 * => new ReceiveMessageRequest(queueUrl).withReceiveRequestAttemptId("1");
	 */
	public void receiveMessage() {
		String queueUrl = "https://sqs.us-west-2.amazonaws.com/<ACCOUNT>/<QUEUE_NAME>.fifo";
		
		List<Message> messageList = sqsClient.getAmazonSQS().receiveMessage(new ReceiveMessageRequest(queueUrl))
															.getMessages();
		for (final Message message : messageList) {
			System.out.println("Message");
			System.out.println("  MessageId:     " + message.getMessageId());
			System.out.println("  ReceiptHandle: " + message.getReceiptHandle());
			System.out.println("  MD5OfBody:     " + message.getMD5OfBody());
			System.out.println("  Body:          " + message.getBody());
			for (final Entry<String, String> entry : message.getAttributes().entrySet()) {
				System.out.println("Attribute");
				System.out.println("  Name:  " + entry.getKey());
				System.out.println("  Value: " + entry.getValue());
			}
		}
	}
	
	
	
	
	/* **************************************************************************************************** */
	/**
	 * Delete a message from a queue
	 * Or Delete all messages from a queue
	 */
	public void deleteMessage() {
		String queueUrl = "https://sqs.us-west-2.amazonaws.com/<ACCOUNT>/<QUEUE_NAME>.fifo";
		List<Message> messageList = new ArrayList<>();			// List of message received from SQS
		
		String messageReceiptHandle = messageList.get(0).getReceiptHandle();
		sqsClient.getAmazonSQS().deleteMessage(new DeleteMessageRequest(queueUrl, messageReceiptHandle));
	}
	
	
	
	
	/* **************************************************************************************************** */
	/**
	 * Delete a queue
	 */
	public void deleteQueue() {
		String queueUrl = "https://sqs.us-west-2.amazonaws.com/<ACCOUNT>/<QUEUE_NAME>.fifo";
		sqsClient.getAmazonSQS().deleteQueue(new DeleteQueueRequest(queueUrl));
	}
	
	
	
	
	/* **************************************************************************************************** */
	/**
	 * Create Dead Letter Queue & map it with a Source Queue
	 * @throws JsonProcessingException
	 */
	public void configureDLQ() throws JsonProcessingException {
		String sourceQueueUrl = "https://sqs.us-west-2.amazonaws.com/<ACCOUNT>/<SOURCE_QUEUE_NAME>.fifo";
		String deadLetterQueueUrl = "https://sqs.us-west-2.amazonaws.com/<ACCOUNT>/<DLQ_QUEUE_NAME>.fifo";
		
		
		// Get the ARN of the DLQ
		GetQueueAttributesRequest requestGetAttributes = new GetQueueAttributesRequest(deadLetterQueueUrl)
															.withAttributeNames(QueueAttributeName.QueueArn.toString());
		String deadLetterQueueArn = sqsClient.getAmazonSQS().getQueueAttributes(requestGetAttributes)
															.getAttributes()
															.get(QueueAttributeName.QueueArn.toString());
		
		
		// Set the DLQ for the source queue using the RedrivePolicy
		SqsRedrivePolicy redrivePolicy = new SqsRedrivePolicy("5", deadLetterQueueArn);
		SetQueueAttributesRequest requestSetAttributes = new SetQueueAttributesRequest()
															.withQueueUrl(sourceQueueUrl)
															.addAttributesEntry(QueueAttributeName.RedrivePolicy.toString(), new ObjectMapper().writeValueAsString(redrivePolicy));
		sqsClient.getAmazonSQS().setQueueAttributes(requestSetAttributes);
	}
	
	
	
	
	/* **************************************************************************************************** */
	/**
	 * Configure visibility timeout for a queue 
	 */
	public void configureVisibilityTimeout() {
		String queueUrl = "https://sqs.us-west-2.amazonaws.com/<ACCOUNT>/<QUEUE_NAME>.fifo";
		
		// Set the visibility timeout for the queue
		SetQueueAttributesRequest requestSetAttributes = new SetQueueAttributesRequest()
															.withQueueUrl(queueUrl)
															.addAttributesEntry(QueueAttributeName.VisibilityTimeout.toString(), "60");
		sqsClient.getAmazonSQS().setQueueAttributes(requestSetAttributes);
	}
	
	
	/**
	 * Configure visibility timeout for a specific message in a queue
	 */
	public void configureVisibilityTimeout_forOneMessage() {
		String queueUrl = "https://sqs.us-west-2.amazonaws.com/<ACCOUNT>/<QUEUE_NAME>.fifo";
		String receiptHandle = sqsClient.getAmazonSQS().receiveMessage(queueUrl).getMessages().get(0).getReceiptHandle();
		sqsClient.getAmazonSQS().changeMessageVisibility(queueUrl, receiptHandle, 60);
	}
	
	
	/**
	 * Configure visibility timeout for multiple messages in a queue
	 */
	public void configureVisibilityTimeout_forMultipleMessages() {
		String queueUrl = "https://sqs.us-west-2.amazonaws.com/<ACCOUNT>/<QUEUE_NAME>.fifo";
		List<ChangeMessageVisibilityBatchRequestEntry> entries = new ArrayList<ChangeMessageVisibilityBatchRequestEntry>();
		
		// Add the first message to the ArrayList with a visibility timeout value.
		String receiptHandleForMsg1 = sqsClient.getAmazonSQS().receiveMessage(queueUrl).getMessages().get(0).getReceiptHandle();
		entries.add(new ChangeMessageVisibilityBatchRequestEntry("uniqueMessageId123", receiptHandleForMsg1).withVisibilityTimeout(60));
		
		// Add the second message to the ArrayList with a different timeout value.
		String receiptHandleForMsg2 = sqsClient.getAmazonSQS().receiveMessage(queueUrl).getMessages().get(0).getReceiptHandle();
		entries.add(new ChangeMessageVisibilityBatchRequestEntry("uniqueMessageId456", receiptHandleForMsg2).withVisibilityTimeout(120));
		
		
		sqsClient.getAmazonSQS().changeMessageVisibilityBatch(queueUrl, entries);
	}
	
}

