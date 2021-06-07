package org.example.aws.ee_sqs_camelwithjms;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.springframework.stereotype.Component;


@Component
public class SqsJmsListener implements MessageListener {
	
	@Override
	public void onMessage(Message message) {
		try {
			message.acknowledge();
			handleMessage(message);
		}
		catch (JMSException e) {
			e.printStackTrace();
		}
	}
	
	
	private void handleMessage(Message message) {
		try {
			String jmsMessageId = message.getJMSMessageID();
			TextMessage textMessage = (TextMessage) message;
			System.out.println(textMessage.getText());
			System.out.println(jmsMessageId);
		}
		catch(JMSException e) {
			e.printStackTrace();
		}
	}
	
}
