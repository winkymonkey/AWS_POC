package org.example.aws.ee_sqs_camelwithjms;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.services.sqs.model.ListQueuesRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;


@Component
public class SqsQueueNameManager {
	
	private static final Map<String, String> queueNameUrlMap = new ConcurrentHashMap<>();
	
	@Autowired
	private SqsJmsClient sqsJmsClient;
	
	
	@PostConstruct
	public void init() {
		ListQueuesRequest request = new ListQueuesRequest();
    	request.setQueueNamePrefix("myQueueNamePrefix-");
    	ListQueuesResult result = sqsJmsClient.getAmazonSQS().listQueues(request);
    	List<String> queueUrlList = result.getQueueUrls();
    	
    	queueUrlList.forEach(queueUrl -> {
    		String queueName = queueUrl.substring(queueUrl.lastIndexOf("/")+1);
			queueNameUrlMap.put(queueName, queueUrl);
    	});
	}
	
	public Set<String> getAllQueueNames() {
		return queueNameUrlMap.keySet();
	}
	
}
