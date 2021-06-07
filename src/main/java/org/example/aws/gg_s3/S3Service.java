package org.example.aws.gg_s3;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetBucketAccelerateConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketLocationRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Upload;


@Service
public class S3Service {
	
	private static final String BUCKET_NAME = "bucketName";
	private static final String KEY_NAME = "myKey";
	
	@Autowired
	private S3Client s3Client;
	
	
	
	/* **************************************************************************************************** */
	/* 										 		LIST OBJECTS
	/* **************************************************************************************************** */
	/**
	 * List all Objects
	 */
	public void listObjects() {
		ObjectListing objects = s3Client.getAmazonS3().listObjects(BUCKET_NAME);
		System.out.println("No. of Objects = " + objects.getObjectSummaries().size());
	}
	
	
	/**
	 * List Objects in batch
	 */
	public void listObjects_inBatch() {
		ListObjectsRequest listRequest = new ListObjectsRequest().withBucketName(BUCKET_NAME).withMaxKeys(2);
		ObjectListing objects = s3Client.getAmazonS3().listObjects(listRequest);
		while (true) {
            List<S3ObjectSummary> summaries = objects.getObjectSummaries();
            for (S3ObjectSummary summary : summaries) {
                System.out.println("Object Key="+summary.getKey()+" is retrieved with size="+summary.getSize());
            }
            
            if (objects.isTruncated())
                objects = s3Client.getAmazonS3().listNextBatchOfObjects(objects);
            else
                break;            
        }
	}
	
	
	
	
	/* **************************************************************************************************** */
	/* 										 		GET OBJECT
	/* **************************************************************************************************** */
	public void getObject() {
		S3Object s3Object = s3Client.getAmazonS3().getObject(new GetObjectRequest(BUCKET_NAME, KEY_NAME));
		System.out.println("Content-Type: " + s3Object.getObjectMetadata().getContentType());
		System.out.println("Content: " + s3Object.getObjectContent());
	}
	
	
	
	
	/* **************************************************************************************************** */
	/* 										 		UPLOAD
	/* **************************************************************************************************** */
	/**
	 * // Upload a text string as a new object.
	 */
	public void upload() {
		s3Client.getAmazonS3().putObject(BUCKET_NAME, KEY_NAME, "content");
        System.out.println("Object is uploaded with transfer acceleration.");
	}
	
	
	/**
	 * Upload a file as a new object with ContentType and title specified.
	 */
	public void uploadFile() {
		PutObjectRequest request = new PutObjectRequest(BUCKET_NAME, KEY_NAME, new File("*** Path to file to upload ***"));
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("plain/text");
        metadata.addUserMetadata("title", "someTitle");
        request.setMetadata(metadata);
        s3Client.getAmazonS3().putObject(request);
	}
	
	
	/**
	 * Multipart upload
	 */
	public void multipartUpload() throws AmazonServiceException, AmazonClientException, InterruptedException {
		Upload upload = s3Client.getTransfermanager().upload(BUCKET_NAME, KEY_NAME, new File("*** Path to file to upload ***"));
		System.out.println("Object upload started");
		upload.waitForCompletion();
		System.out.println("Object upload complete");
	}
	
	
	/**
	 * Multipart upload
	 * Receive notifications when bytes are transferred
	 */
	public void multipartUploadWithListener() throws AmazonServiceException, AmazonClientException, InterruptedException {
		PutObjectRequest request = new PutObjectRequest(BUCKET_NAME, KEY_NAME, new File("*** Path to file to upload ***"));
		request.setGeneralProgressListener(new ProgressListener() {
            public void progressChanged(ProgressEvent progressEvent) {
                System.out.println("Transferred bytes: " + progressEvent.getBytesTransferred());
            }
        });
		Upload upload = s3Client.getTransfermanager().upload(request);
		System.out.println("Object upload started");
		upload.waitForCompletion();
		System.out.println("Object upload complete");
	}
	
	
	/**
	 * Generate a pre-signed URL
	 * Upload using the pre-signed URL
	 */
	public void generatePresignedUrl() throws IOException {
		System.out.println("Seting the pre-signed URL to expire after one hour");
		Date expiration = new Date();
		long expTimeMillis = expiration.getTime();
		expTimeMillis += 1000 * 60 * 60;
		expiration.setTime(expTimeMillis);
		
		System.out.println("Generating pre-signed URL");
		GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(BUCKET_NAME, KEY_NAME)
													                .withMethod(HttpMethod.PUT).withExpiration(expiration);
		URL url = s3Client.getAmazonS3().generatePresignedUrl(request);
		uploadUsingPresignedUrl(url);
	}
	
	private void uploadUsingPresignedUrl(URL url) throws IOException {
		System.out.println("Creating the connection and use it to upload the new object using the pre-signed URL");
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("PUT");
        OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());
        out.write("*** This text uploaded as an object via presigned URL ***");
        out.close();
        connection.getResponseCode();
        System.out.println("HTTP response code: " + connection.getResponseCode());
        
        System.out.println("Checking to make sure that the object was uploaded successfully");
        S3Object object = s3Client.getAmazonS3().getObject(BUCKET_NAME, KEY_NAME);
        System.out.println("Object " + object.getKey() + " created in bucket " + object.getBucketName());
	}
	
	
	
	
	/* **************************************************************************************************** */
	/* 										 		DELETE OBJECT
	/* **************************************************************************************************** */
	public void deleteObject() {
		s3Client.getAmazonS3().deleteObject(new DeleteObjectRequest(BUCKET_NAME, KEY_NAME));
	}
	
	
	
	
	/* **************************************************************************************************** */
	/* 										 		MISCELLANEOUS
	/* **************************************************************************************************** */
	public void checkBucketLocation() {
		String bucketLocation = s3Client.getAmazonS3().getBucketLocation(new GetBucketLocationRequest(BUCKET_NAME));
        System.out.println("Bucket location: " + bucketLocation);
	}
	
	
	public void checkIfTransferAccelerationEnabled() {
		String accelerateStatus = s3Client.getAmazonS3().getBucketAccelerateConfiguration(new GetBucketAccelerateConfigurationRequest(BUCKET_NAME)).getStatus();
        System.out.println("Bucket accelerate status: " + accelerateStatus);
	}
	
	
	/**
	 * Restore an object archived to S3 Glacier
	 */
	public void restoreObjects() {
		System.out.println("Creating & submitting a request to restore an object from Glacier for 2 days.");
		RestoreObjectRequest requestRestore = new RestoreObjectRequest(BUCKET_NAME, KEY_NAME, 2);
        s3Client.getAmazonS3().restoreObjectV2(requestRestore);
        
        ObjectMetadata response = s3Client.getAmazonS3().getObjectMetadata(BUCKET_NAME, KEY_NAME);
        Boolean restoreFlag = response.getOngoingRestore();
        System.out.format("Restoration status="+ (restoreFlag ? "in progress":"not in progress (finished or failed)"));
	}
	
}
