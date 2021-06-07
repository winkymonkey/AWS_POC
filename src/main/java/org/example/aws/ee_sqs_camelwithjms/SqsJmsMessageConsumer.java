package org.example.aws.ee_sqs_camelwithjms;

import java.util.Set;

import javax.annotation.PostConstruct;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;


@Component
public class SqsJmsMessageConsumer {
	
	@Autowired
	private SqsJmsClient sqsJmsClient;
	
	@Autowired
	private SqsQueueNameManager sqsQueueNameManager;
	
	@Autowired
	private SqsJmsListener sqsJmsListener;
	
	
	@PostConstruct
	public void init() throws JMSException {
		SQSConnectionFactory connectionFactory = sqsJmsClient.getSqsConnectionFactory();
		SQSConnection connection = connectionFactory.createConnection();
		
		Set<String> queueNameSet = sqsQueueNameManager.getAllQueueNames();
		for(String queueName : queueNameSet) {
			Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));
			consumer.setMessageListener(sqsJmsListener);
		}
	}
	
}
