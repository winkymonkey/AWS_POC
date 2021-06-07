package org.example.aws.gg_s3;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;


@Component
public class S3Client {
	
	private AmazonS3 amazonS3;
	private TransferManager transfermanager;
	
	public AmazonS3 getAmazonS3() {
		return amazonS3;
	}
	public TransferManager getTransfermanager() {
		return transfermanager;
	}


	@PostConstruct
	public void init() {
		amazonS3 = AmazonS3ClientBuilder.standard().withRegion(Regions.fromName("us-west-2")).build();
		transfermanager = TransferManagerBuilder.standard().withS3Client(amazonS3).build();
	}
	
}
