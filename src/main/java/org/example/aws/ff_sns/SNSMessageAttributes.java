package org.example.aws.ff_sns;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;


public class SNSMessageAttributes {

	private String message;
	private Map<String, MessageAttributeValue> messageAttributes;

	public SNSMessageAttributes(final String message) {
		this.message = message;
		messageAttributes = new HashMap<>();
	}

	public String getMessage() {
		return message;
	}
	public void setMessage(final String message) {
		this.message = message;
	}
	
	
	/**
	 * Add String attributes
	 * @param attributeName
	 * @param attributeValue
	 */
	public void addAttribute(final String attributeName, final String attributeValue) {
		final MessageAttributeValue messageAttributeValue = new MessageAttributeValue()
															.withDataType("String")
															.withStringValue(attributeValue);
		messageAttributes.put(attributeName, messageAttributeValue);
	}

	/**
	 * Add String Array attributes
	 * @param attributeName
	 * @param attributeValues
	 */
	public void addAttribute(final String attributeName, final List<?> attributeValues) {
		String valuesString, delimiter = ", ", prefix = "[", suffix = "]";
		if (attributeValues.get(0).getClass() == String.class) {
			delimiter = "\", \"";
			prefix = "[\"";
			suffix = "\"]";
		}
		valuesString = attributeValues.stream().map(Object::toString).collect(Collectors.joining(delimiter, prefix, suffix));
		final MessageAttributeValue messageAttributeValue = new MessageAttributeValue()
															.withDataType("String.Array")
															.withStringValue(valuesString);
		messageAttributes.put(attributeName, messageAttributeValue);
	}

	/**
	 * Add Number attributes
	 * @param attributeName
	 * @param attributeValue
	 */
	public void addAttribute(final String attributeName, final Number attributeValue) {
		final MessageAttributeValue messageAttributeValue = new MessageAttributeValue()
															.withDataType("Number")
															.withStringValue(attributeValue.toString());
		messageAttributes.put(attributeName, messageAttributeValue);
	}

	
	/**
	 * Publish the message
	 * @param snsClient
	 * @param topicArn
	 * @return
	 */
	public String publish(final AmazonSNS snsClient, final String topicArn) {
		final PublishRequest request = new PublishRequest(topicArn, message).withMessageAttributes(messageAttributes);
		final PublishResult result = snsClient.publish(request);
		return result.getMessageId();
	}
	
}
