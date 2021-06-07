package org.example.aws.ff_sns;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;


@Component
public class SnsClient {
	
	private AmazonSNS amazonSNS;
	
	public AmazonSNS getAmazonSNS() {
		return amazonSNS;
	}
	
	
	@PostConstruct
	public void init() {
		amazonSNS = AmazonSNSClientBuilder.standard().withRegion(Regions.fromName("us-west-2")).build();
	}
	
}
