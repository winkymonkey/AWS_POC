package org.example.aws.dd_sqs_consumerwithcamel;

import org.apache.camel.impl.SimpleRegistry;
import org.apache.camel.spring.SpringCamelContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class CamelContextConfigurer {
	
	@Autowired
	private SqsCamelRouteBuilder sqsCamelRouteBuilder;
	
	@Autowired
	private SqsCamelClient sqsCamelClient;
	
	@Bean("camel-context")
	public SpringCamelContext camelContext(ApplicationContext applicationContext) throws Exception {
		SpringCamelContext camelContext = new SpringCamelContext(applicationContext);
		SimpleRegistry registry = new SimpleRegistry();
		registry.put("sqsCamelClient", sqsCamelClient.getAmazonSQS());
		camelContext.addRoutes(sqsCamelRouteBuilder);
		return camelContext;
	}
	
}
