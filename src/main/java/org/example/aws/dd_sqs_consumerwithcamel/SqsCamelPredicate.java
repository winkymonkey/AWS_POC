package org.example.aws.dd_sqs_consumerwithcamel;

import org.apache.camel.Exchange;
import org.apache.camel.Predicate;


public class SqsCamelPredicate implements Predicate {

	@Override
	public boolean matches(Exchange exchange) {
		if(exchange == null || exchange.getIn() == null) {
			throw new RuntimeException("Exchange message is null. Cannot process the message.");
		}
		else {
			String msgBody = exchange.getIn().getBody().toString();
			System.out.println(msgBody);
			//validate if the msgBody is intended for this consumer or not
			return true;
		}
	}
}
