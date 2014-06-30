package com.rtla.helper;

import java.nio.ByteBuffer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceInUseException;

/**
 * This file is a helper class for sending data to Kinesis stream.
 * 
 * @author Jason CUI
 */
public class AWSKinesisHelper {

	private static AWSKinesisHelper instance = null;

	private static AmazonKinesisClient kinesisClient = null;
	private static String kinesisStreamName = null;
	private static int streamSize = 0;

	private AWSKinesisHelper() {
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location (~/.aws/credentials), and is in valid format.",
					e);
		}
		kinesisClient = new AmazonKinesisClient(credentials);
	}

	/**
	 * Singleton instance for AWSKinesisHelper.
	 * 
	 * @return
	 */
	public static AWSKinesisHelper getInstance() {
		if (instance == null)
			instance = new AWSKinesisHelper();

		return instance;
	}

	/**
	 * Prepare the Stream with inputs.
	 * 
	 * @param streamName
	 * @param streamSize
	 */
	public void prepareStream(String streamName, int streamSize)
			throws Exception {
		if (instance == null)
			throw new Exception("AWSKinesisHelper not initialized!");
		else {
			CreateStreamRequest createStreamRequest = new CreateStreamRequest();
			createStreamRequest.setStreamName(streamName);
			createStreamRequest.setShardCount(streamSize);

			try {
				kinesisClient.createStream(createStreamRequest);
			} catch (ResourceInUseException e) {
				// already in use
				System.out.println("already in use");
			}

			waitForStreamToBecomeAvailable(streamName);
		}
	}

	/**
	 * Send data to the Stream.
	 * 
	 * @param strData
	 */
	public void sendData(String strData) {
		PutRecordRequest putRecordRequest = new PutRecordRequest();
		putRecordRequest.setStreamName(kinesisStreamName);
		putRecordRequest.setData(ByteBuffer.wrap(strData.getBytes()));
		putRecordRequest.setPartitionKey(String.format("partitionKey"));
		PutRecordResult putRecordResult = kinesisClient
				.putRecord(putRecordRequest);
		System.out.println("Successfully putRecord, partition key: "
				+ putRecordRequest.getPartitionKey() + ", ShardID: "
				+ putRecordResult.getShardId());
	}

	/**
	 * Delete the Stream on Kinesis.
	 * 
	 * @param streamName
	 */
	public void cleanUp(String streamName) {
		DeleteStreamRequest deleteStreamRequest = new DeleteStreamRequest();
		deleteStreamRequest.setStreamName(streamName);

		kinesisClient.deleteStream(deleteStreamRequest);
	}

	/**
	 * Wait for the stream to became available.
	 * 
	 * @param myStreamName
	 */
	private void waitForStreamToBecomeAvailable(String myStreamName) {

		System.out.println("Waiting for " + myStreamName
				+ " to become ACTIVE...");

		long startTime = System.currentTimeMillis();
		long endTime = startTime + (10 * 60 * 1000);
		while (System.currentTimeMillis() < endTime) {
			try {
				Thread.sleep(1000 * 20);
			} catch (InterruptedException e) {
				// Ignore interruption (doesn't impact stream creation)
			}
			try {
				DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
				describeStreamRequest.setStreamName(myStreamName);
				// ask for no more than 10 shards at a time -- this is an
				// optional parameter
				describeStreamRequest.setLimit(10);
				DescribeStreamResult describeStreamResponse = kinesisClient
						.describeStream(describeStreamRequest);

				String streamStatus = describeStreamResponse
						.getStreamDescription().getStreamStatus();
				System.out.println("  - current state: " + streamStatus);
				if (streamStatus.equals("ACTIVE")) {
					kinesisStreamName = myStreamName;
					return;
				}
			} catch (AmazonServiceException ase) {
				if (ase.getErrorCode().equalsIgnoreCase(
						"ResourceNotFoundException") == false) {
					throw ase;
				}
				throw new RuntimeException("Stream " + myStreamName
						+ " never went active");
			}
		}
	}

}
