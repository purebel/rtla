package com.rtla.sample;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceInUseException;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.rtla.helper.AWSKinesisHelper;

public class RTLASample {
	
	private static final int EVENT_MOVEMENT = 10001;
	private static final int EVENT_PRESENCE = 10002;
	
	//Records
	private static final HashMap<String, String> records = new HashMap<String, String>();

	static AmazonKinesisClient kinesisClient = null;
	static AWSKinesisHelper helper = null;
	private static final Log LOG = LogFactory.getLog(AmazonKinesisClient.class);
	
	//To print the JSON string of all records
//	private static StringBuffer sb = new StringBuffer();

	private static void init() throws Exception {
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
		LOG.info("Credentials:" + credentials.getAWSAccessKeyId() + "-"
				+ credentials.getAWSSecretKey());
		kinesisClient = new AmazonKinesisClient(credentials);
	}

	public static void main(String[] args) throws Exception {
		records.put("movement", "movement_test.csv");
		records.put("presence", "presence_test.csv");
//		 helper = AWSKinesisHelper.getInstance();
//		 helper.prepareStream("RTLA_JASON", 1);
		/*
		 * Test data for (int j = 0; j < 50000; j++) {
		 * helper.sendData("RTLA_JASON + " + j); }
		 */
		// helper.cleanUp();
		parseRecordFromFile(EVENT_MOVEMENT, records.get("movement"));
		parseRecordFromFile(EVENT_PRESENCE, records.get("presence"));
		
//		System.out.println(sb.toString());
	}

	public static void parseRecordFromFile(int eventType, String filename) {
		
		try {
			File fileCSV = new File(filename);
			BufferedReader br = new BufferedReader(new FileReader(fileCSV));

			String currentLine = "";
			JSONObject eventObj; 
			while ((currentLine = br.readLine()) != null) {
				eventObj = new JSONObject();
				LOG.info(currentLine);
				
				String[] lineTokens = currentLine.split(",");
				eventObj.put("entity", lineTokens[2]);//
				eventObj.put("deviceId", lineTokens[1]);//MAC 
				eventObj.put("locationMapHierarchy", lineTokens[7]);//Location
				JSONObject coorObj = new JSONObject();
				coorObj.put("x", Float.parseFloat(lineTokens[5]));
				coorObj.put("y", Float.parseFloat(lineTokens[6]));
				coorObj.put("unit", lineTokens[4]);
				eventObj.put("locationCoordinate", coorObj);
				switch(eventType) {
				case EVENT_MOVEMENT:
					eventObj.put("type", "movement");
					eventObj.put("subscriptionName", lineTokens[9]);//
					eventObj.put("moveDistanceInFt", Float.parseFloat(lineTokens[8]));//move distance
					eventObj.put("referenceMarkerName", lineTokens[3]);//
					eventObj.put("timestamp", lineTokens[11]);//ts
					break;
				case EVENT_PRESENCE:
					eventObj.put("type", "presence");
					eventObj.put("subscriptionName", lineTokens[8]);//
					eventObj.put("timestamp", lineTokens[10]);//ts
					break;
				}
				//proObj.put("itemNo", lineTokens[0]);
//				helper.sendData(eventObj.toString());
				LOG.info("Send to Kinesis:\n" + eventObj.toString(4));
//				sb.append(eventObj.toString(4)).append("\n");
			}
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch( JSONException e){
			e.printStackTrace();
		}

	}

	public static void main1(String[] args) throws Exception {
		init();
		LOG.info("Kinesis Client created succesfully!");

		final String rtlaStreamName = "RTLA_JASON";
		final Integer rtlaStreamSize = 1;

		// Create Stream
		CreateStreamRequest createStreamRequest = new CreateStreamRequest();
		createStreamRequest.setStreamName(rtlaStreamName);
		createStreamRequest.setShardCount(rtlaStreamSize);
		try {
			kinesisClient.createStream(createStreamRequest);
		} catch (ResourceInUseException e) {
			LOG.warn("The Stream already existed!");
		}

		LOG.info("Creating Stream : " + rtlaStreamName);
		waitForStreamToBecomeAvailable(rtlaStreamName);

		// List all Streams associated with the account
		ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
		listStreamsRequest.setLimit(10);
		ListStreamsResult listStreamsResult = kinesisClient
				.listStreams(listStreamsRequest);
		List<String> streamNames = listStreamsResult.getStreamNames();
		while (listStreamsResult.isHasMoreStreams()) {
			if (streamNames.size() > 0) {
				listStreamsRequest.setExclusiveStartStreamName(streamNames
						.get(streamNames.size() - 1));
			}
			listStreamsResult = kinesisClient.listStreams(listStreamsRequest);
			streamNames.addAll(listStreamsResult.getStreamNames());
		}
		LOG.info("Printing list of Streams:");

		if (!streamNames.isEmpty()) {
			for (int i = 0; i < streamNames.size(); i++) {
				System.out.println(streamNames.get(i));
			}
		}

		LOG.info("Putting records in to stream:" + rtlaStreamName);
		// Write 5 records to Stream "RTLocation"
		for (int j = 0; j < 50000; j++) {
			PutRecordRequest putRecordRequest = new PutRecordRequest();
			putRecordRequest.setStreamName(rtlaStreamName);
			putRecordRequest.setData(ByteBuffer.wrap(String.format(
					"MAC-%s,corX-%d,corY-%d", "MAC" + j, new Integer(j),
					new Integer(j)).getBytes()));
			putRecordRequest.setPartitionKey(String
					.format("partitionKey-%d", j));
			PutRecordResult putRecordResult = kinesisClient
					.putRecord(putRecordRequest);
			LOG.info("Successfully putRecord, partition key: "
					+ putRecordRequest.getPartitionKey() + ", ShardID: "
					+ putRecordResult.getShardId());
		}

		LOG.info("Deleting the Stream");
		DeleteStreamRequest deleteStreamRequest = new DeleteStreamRequest();
		deleteStreamRequest.setStreamName(rtlaStreamName);

		kinesisClient.deleteStream(deleteStreamRequest);
		LOG.warn("Stream is now being deleted: " + rtlaStreamName);
	}

	private static void waitForStreamToBecomeAvailable(String myStreamName) {

		LOG.info("Waiting for " + myStreamName + " to become ACTIVE ...");

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
				LOG.info("    - current state: " + streamStatus);
				if (streamStatus.equals("ACTIVE")) {
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
