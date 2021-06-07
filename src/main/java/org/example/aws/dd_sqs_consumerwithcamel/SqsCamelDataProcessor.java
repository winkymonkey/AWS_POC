package org.example.aws.dd_sqs_consumerwithcamel;

import org.springframework.stereotype.Component;


@Component
public class SqsCamelDataProcessor {
	
	public void execute(String message) {
		//receive incoming message only if the message is intended for this consumer
		//process it
	}
	
	public void predicateFails(String message) {
		//receive incoming message only if the message is NOT intended for this consumer
		//log this error
	}
}
