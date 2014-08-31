package com.rtla.kinesis;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

public class RTLARecordProcessor implements IRecordProcessor {
	private static final Log LOG = LogFactory
			.getLog(RTLARecordProcessor.class);
	private String kinesisShardId;

	// Backoff and retry settings
	private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
	private static final int NUM_RETRIES = 10;

	// Checkpoint about once a minute
	private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
	private long nextCheckpointTimeInMillis;
	
	static String DBTableName = "MSE_JASON";//"EventMovement";
	
	protected static AmazonDynamoDBClient client;
	private static int coorxStep=1;
  	private static int cooryStep=1;

	private final CharsetDecoder decoder = Charset.forName("UTF-8")
			.newDecoder();

	/**
	 * Constructor.
	 */
	public RTLARecordProcessor() {
		super();
	}

	@Override
	public void initialize(String shardId) {
		LOG.info("Initializing record processor for shard: " + shardId);
		this.kinesisShardId = shardId;
		
		try {
			initDynamoDB();
			LOG.info("Initializing DynamoDB Okay.");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.error("Initializating DynamoDB FAILED");
		}
	}
	
	private static void initDynamoDB() throws Exception {
		AWSCredentials credentials = null;	
		credentials = new ProfileCredentialsProvider().getCredentials();
		client = new AmazonDynamoDBClient(credentials);
	}

	@Override
	public void processRecords(List<Record> records,
			IRecordProcessorCheckpointer checkpointer) {
		LOG.info("Processing " + records.size() + " records from "
				+ kinesisShardId);

		// Process records and perform all exception handling.
		processRecordsWithRetries(records);

		// Checkpoint once every checkpoint interval.
		if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
			checkpoint(checkpointer);
			nextCheckpointTimeInMillis = System.currentTimeMillis()
					+ CHECKPOINT_INTERVAL_MILLIS;
		}
	}

	/**
	 * Process records performing retries as needed. Skip "poison pill" records.
	 * 
	 * @param records
	 */
	private void processRecordsWithRetries(List<Record> records) {
		for (Record record : records) {
			boolean processedSuccessfully = false;
			String data = null;
			for (int i = 0; i < NUM_RETRIES; i++) {
				try {
					// For this app, we interpret the payload as UTF-8 chars.
					data = decoder.decode(record.getData()).toString();
					LOG.info(record.getSequenceNumber() + ", "
							+ record.getPartitionKey() + ", " + data);
					uploadItemsToDynamo(DBTableName, data);
					//
					// Logic to process record goes here.
					//
					processedSuccessfully = true;
					break;
				} catch (CharacterCodingException e) {
					LOG.error("Malformed data: " + data, e);
					break;
				} catch (Throwable t) {
					LOG.warn("Caught throwable while processing record "
							+ record, t);
				}

				// backoff if we encounter an exception.
				try {
					Thread.sleep(BACKOFF_TIME_IN_MILLIS);
				} catch (InterruptedException e) {
					LOG.debug("Interrupted sleep", e);
				}
			}

			if (!processedSuccessfully) {
				LOG.error("Couldn't process record " + record
						+ ". Skipping the record.");
			}
		}
	}

	@Override
	public void shutdown(IRecordProcessorCheckpointer checkpointer,
			ShutdownReason reason) {
		LOG.info("Shutting down record processor for shard: " + kinesisShardId);
		// Important to checkpoint after reaching end of shard, so we can start
		// processing data from child shards.
		if (reason == ShutdownReason.TERMINATE) {
			checkpoint(checkpointer);
		}
	}

	/**
	 * Checkpoint with retries.
	 * 
	 * @param checkpointer
	 */
	private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
		LOG.info("Checkpointing shard " + kinesisShardId);
		for (int i = 0; i < NUM_RETRIES; i++) {
			try {
				checkpointer.checkpoint();
				break;
			} catch (ShutdownException se) {
				// Ignore checkpoint if the processor instance has been shutdown
				// (fail over).
				LOG.info("Caught shutdown exception, skipping checkpoint.", se);
				break;
			} catch (ThrottlingException e) {
				// Backoff and re-attempt checkpoint upon transient failures
				if (i >= (NUM_RETRIES - 1)) {
					LOG.error("Checkpoint failed after " + (i + 1)
							+ "attempts.", e);
					break;
				} else {
					LOG.info("Transient issue when checkpointing - attempt "
							+ (i + 1) + " of " + NUM_RETRIES, e);
				}
			} catch (InvalidStateException e) {
				// This indicates an issue with the DynamoDB table (check for
				// table, provisioned IOPS).
				LOG.error(
						"Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.",
						e);
				break;
			}
			try {
				Thread.sleep(BACKOFF_TIME_IN_MILLIS);
			} catch (InterruptedException e) {
				LOG.debug("Interrupted sleep", e);
			}
		}
	}
	
	private static void uploadItemsToDynamo(String tableName, String data) {
        
        try {
          	LOG.info("UploadItem Dynamo DB:" + tableName + " with Item:" + data);
          	
          	JSONObject eventObj = JSONObject.fromObject(data);
          	LOG.info("JSONObject with data:" + 
          			eventObj.getString("deviceId") + "\n" + 
          			eventObj.getString("entity") + "\n" + 
          			eventObj.getString("unit") + "\n" +
          			eventObj.getString("coor_x") + "\n" +
          			eventObj.getString("coor_y") + "\n" +
          			eventObj.getString("locationMapHierarchy") + "\n" +
          			eventObj.getString("referenceMarkerName") + "\n" +
          			eventObj.getString("subscriptionName") + "\n" +
          			eventObj.getString("type") + "\n" +
          			eventObj.getString("timestamp") + "\n");
          	
          	int intCoorX = (int)Float.parseFloat(eventObj.getString("coor_x"));
          	int intCoorY = (int)Float.parseFloat(eventObj.getString("coor_y"));
          	String districtStr = intCoorX + "-" + intCoorY;
          	LOG.info("districtStr:" + districtStr);
          	Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
          	item.put("mac", new AttributeValue().withS(eventObj.getString("deviceId")));
          	item.put("ts", new AttributeValue().withS(eventObj.getString("timestamp")));
          	item.put("coorx", new AttributeValue().withS(eventObj.getString("coor_x")));
          	item.put("coory", new AttributeValue().withS(eventObj.getString("coor_y")));
          	item.put("movement", new AttributeValue().withS(eventObj.getString("moveDistanceInFt")));
          	item.put("district", new AttributeValue().withS(districtStr));
          	PutItemRequest itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            PutItemResult itemResult = client.putItem(itemRequest);
            item.clear();
                
        }   catch (AmazonServiceException ase) {
        	LOG.error("Failed to create item in " + tableName);
        	ase.printStackTrace();
        } 

    }

}
