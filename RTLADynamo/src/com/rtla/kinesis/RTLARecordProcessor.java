package com.rtla.kinesis;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

import freemarker.core.ParseException;

public class RTLARecordProcessor implements IRecordProcessor {
	private static final Log LOG = LogFactory.getLog(RTLARecordProcessor.class);
	private String kinesisShardId;

	// Backoff and retry settings
	private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
	private static final int NUM_RETRIES = 10;

	// Checkpoint about once a minute
	private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
	private long nextCheckpointTimeInMillis;

	static String DBTableMSE = "demo_raw";// "EventMovement";
	static String DBTableStat = "demo_stats";//"RTLA_STATS_DAILY_JASON";// Daily Statistic
	static String DBTableHM = "demo_heatmap";//"QH_heatmap2";

	protected static AmazonDynamoDBClient client;
	private static int coorxStep = 1;
	private static int cooryStep = 1;

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
					// ===============================================
					// MSE raw data
					uploadItemsToDynamo(DBTableMSE, data);

					// Daily statistics
					uploadItemForStat(DBTableStat, data);

					// HeatMap
					uploadItemsForHeatMap(DBTableHM, data);
					// ===============================================

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

	private static void uploadItemForStat(String tableName, String data) {

		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		try {
			LOG.info("UploadItem Dynamo DB:" + tableName + " with Item:" + data);

			JSONObject eventObj = JSONObject.fromObject(data);

			Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
			LOG.info(eventObj.toString(4));
			JSONObject locationObj = eventObj
					.getJSONObject("locationCoordinate");
			String eventMac = eventObj.getString("deviceId");
			String eventDate = eventObj.getString("timestamp").substring(0, 10);
			String eventType = eventObj.getString("type");
			item.put("date", new AttributeValue().withS(eventObj.getString(
					"timestamp").substring(0, 10)));
			item.put("mac",
					new AttributeValue().withS(eventObj.getString("deviceId")));
			item.put("entity",
					new AttributeValue().withS(eventObj.getString("entity")));
			if (eventType.equals("movement")) {
				// DO Nothing
				// item.put("enter", new
				// AttributeValue().withS(eventObj.getString("timestamp")));
				// item.put("exit", new
				// AttributeValue().withS(eventObj.getString("timestamp")));
			} else if (eventType.equals("presence")) {
				Condition hashKeyCondition = new Condition()
						.withComparisonOperator(ComparisonOperator.EQ)
						.withAttributeValueList(
								new AttributeValue().withS(eventDate));
				Condition rangeKeyCondition = new Condition()
						.withComparisonOperator(ComparisonOperator.EQ)
						.withAttributeValueList(
								new AttributeValue().withS(eventMac));
				QueryRequest queryRequest = new QueryRequest(tableName);
				queryRequest.withIndexName("date-mac-index")
						.addKeyConditionsEntry("date", hashKeyCondition)
						.addKeyConditionsEntry("mac", rangeKeyCondition);
				QueryResult queryResult = client.query(queryRequest);
				if (queryResult.getCount() == 0) {
					// First appearance
					LOG.info("First appearance of :" + eventMac);
					item.put("enter", new AttributeValue().withS(eventObj
							.getString("timestamp")));
					item.put("exit", new AttributeValue().withS(eventObj
							.getString("timestamp")));
					item.put("presence", new AttributeValue()
							.withS(new Integer(0).toString()));
				} else if (queryResult.getCount() == 1) {
					// Non-First, update fields
					LOG.info("Non-First appearance of :" + eventMac);
					for (Map<String, AttributeValue> queryItem : queryResult
							.getItems()) {
						AttributeValue enterDate = null;
						AttributeValue exitDate = null;
						for (Map.Entry<String, AttributeValue> queryItem2 : queryItem
								.entrySet()) {
							String attributeName = queryItem2.getKey();
							AttributeValue value = queryItem2.getValue();

								if (attributeName.equals("enter")) {
									enterDate = value;

								} else if (attributeName.equals("exit")) {
									exitDate = value;
								}

						}
						if((enterDate == null) && (exitDate == null))
						{
							LOG.info("First time for presence, not first for movment:" + eventMac);
							//First time for presence event, not movement
							item.put("enter", new AttributeValue().withS(eventObj
									.getString("timestamp")));
							item.put("exit", new AttributeValue().withS(eventObj
									.getString("timestamp")));
							item.put("presence", new AttributeValue()
									.withS(new Integer(0).toString()));
						}
						else if((enterDate != null) && (exitDate != null)){
							LOG.info("Non-First time for presence, not first for movment:" + eventMac);
							Date curDate = null;
							try {
								curDate = df.parse(eventObj.getString("timestamp"));
							} catch (java.text.ParseException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							long presence = 0;
							try {
								presence = (curDate.getTime() - df.parse(enterDate.getS()).getTime()) / 1000;
							} catch (java.text.ParseException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							item.put("enter", enterDate);
							item.put("exit", new AttributeValue().withS(eventObj
									.getString("timestamp")));
							item.put("presence", new AttributeValue()
									.withS(new Integer((int) presence).toString()));
						}
						
					}
				}

			}

			PutItemRequest itemRequest = new PutItemRequest().withTableName(
					tableName).withItem(item);
			PutItemResult itemResult = client.putItem(itemRequest);
			LOG.info(itemResult.getConsumedCapacity());
			item.clear();

		} catch (AmazonServiceException ase) {
			LOG.error("Failed to create item in " + tableName);
			ase.printStackTrace();
		}

	}

	private static void uploadItemsToDynamo(String tableName, String data) {

		try {
			LOG.info("UploadItem Dynamo DB:" + tableName + " with Item:" + data);

			JSONObject eventObj = JSONObject.fromObject(data);

			JSONObject locationObj = eventObj
					.getJSONObject("locationCoordinate");
			String type = eventObj.getString("type");
			String CoorX = (locationObj.getString("x"));
			String CoorY = (locationObj.getString("y"));
			int intCoorX = (int) Float.parseFloat(locationObj.getString("x"));
			int intCoorY = (int) Float.parseFloat(locationObj.getString("y"));
			String unit = locationObj.getString("unit");

			LOG.info("JSONObject with data:" + eventObj.getString("deviceId")
					+ "\n" + eventObj.getString("entity") + "\n"
					+ unit
					+ "\n"
					+ intCoorX
					+ "\n"
					+ intCoorY
					+ "\n"
					+
					// eventObj.getString("unit") + "\n" +
					// eventObj.getString("coor_x") + "\n" +
					// eventObj.getString("coor_y") + "\n" +
					eventObj.getString("locationMapHierarchy")
					+ "\n"
					+
					// eventObj.getString("referenceMarkerName") + "\n" +
					eventObj.getString("subscriptionName") + "\n"
					+ eventObj.getString("type") + "\n"
					+ eventObj.getString("timestamp") + "\n");

			String districtStr = intCoorX / coorxStep + "-" + intCoorY
					/ cooryStep;
			LOG.info("districtStr:" + districtStr);
			Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
			item.put("mac",
					new AttributeValue().withS(eventObj.getString("deviceId")));
			item.put("ts",
					new AttributeValue().withS(eventObj.getString("timestamp")));
			item.put("coorx", new AttributeValue().withS(/*
														 * eventObj.getString(
														 * "coor_x")
														 */CoorX));
			item.put("coory", new AttributeValue().withS(/*
														 * eventObj.getString(
														 * "coor_y")
														 */CoorY));
			if (type.compareTo("movement") == 0) {
				item.put("movement", new AttributeValue().withS(eventObj
						.getString("moveDistanceInFt")));
			}
			item.put("district", new AttributeValue().withS(districtStr));
			PutItemRequest itemRequest = new PutItemRequest().withTableName(
					tableName).withItem(item);
			PutItemResult itemResult = client.putItem(itemRequest);
			item.clear();

		} catch (AmazonServiceException ase) {
			LOG.error("Failed to create item in " + tableName);
			ase.printStackTrace();
		}

	}

	private static void uploadItemsForHeatMap(String tableName, String data) {

		try {
			LOG.info("UploadItem HeatMap DB:" + tableName + " with Item:"
					+ data);

			JSONObject eventObj = JSONObject.fromObject(data);
			JSONObject locationObj = eventObj
					.getJSONObject("locationCoordinate");

			int intCoorX = (int) Float.parseFloat(locationObj.getString("x"));
			int intCoorY = (int) Float.parseFloat(locationObj.getString("y"));
			String unit = locationObj.getString("unit");

			LOG.info("JSONObject with data:" + eventObj.getString("deviceId")
					+ "\n" + eventObj.getString("entity") + "\n" + unit + "\n"
					+ intCoorX + "\n" + intCoorY + "\n"
					+ eventObj.getString("locationMapHierarchy") + "\n"
					+ eventObj.getString("subscriptionName") + "\n"
					+ eventObj.getString("type") + "\n"
					+ eventObj.getString("timestamp") + "\n");

			String districtStr = intCoorX / coorxStep + "-" + intCoorY
					/ cooryStep;
			LOG.info("districtStr:" + districtStr);
			String[] tsArr = eventObj.getString("timestamp").split(" ");
			String curDate = tsArr[0];
			String tsCode = curDate;
			LOG.info("tsCode:" + tsCode);

			// get stats from table
			int cnt = getStats(tableName, districtStr, tsCode);
			System.out.println("getStats return" + "cnt=" + cnt);
			cnt++;
			System.out.println("increase by 1, and " + "cnt=" + cnt);
			String strCnt = Integer.toString(cnt);

			Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
			item.put("district", new AttributeValue().withS(districtStr));
			item.put("ts", new AttributeValue().withS(tsCode));
			item.put("cnt", new AttributeValue().withS(strCnt));

			PutItemRequest itemRequest = new PutItemRequest().withTableName(
					tableName).withItem(item);
			PutItemResult itemResult = client.putItem(itemRequest);
			item.clear();

		} catch (AmazonServiceException ase) {
			LOG.error("Failed to create item in " + tableName);
			ase.printStackTrace();
		}

	}

	private static int getStats(String tableName, String district, String tsCode) {
		int stats = 0;

		Condition hashKeyCondition = new Condition().withComparisonOperator(
				ComparisonOperator.EQ).withAttributeValueList(
				new AttributeValue().withS(/* district */tsCode));

		Condition rangeKeyCondition = new Condition().withComparisonOperator(
				ComparisonOperator.EQ).withAttributeValueList(
				new AttributeValue().withS(/* tsCode */district));

		Map<String, Condition> keyConditions = new HashMap<String, Condition>();
		keyConditions.put("ts", hashKeyCondition);
		keyConditions.put("district", rangeKeyCondition);

		QueryRequest queryRequest = new QueryRequest().withTableName(tableName)
				.withKeyConditions(keyConditions).withAttributesToGet("cnt");

		QueryResult result = client.query(queryRequest);

		// Check the response.
		for (Map<String, AttributeValue> item : result.getItems()) {
			printItem(item);
		}
		for (Map<String, AttributeValue> item : result.getItems()) {
			for (Map.Entry<String, AttributeValue> item2 : item.entrySet()) {
				String attributeName = item2.getKey();
				AttributeValue value = item2.getValue();
				String attr = "cnt";
				System.out.println("getStats::" + "attributeName="
						+ attributeName + ", cmp="
						+ attributeName.compareTo(attr));
				if (attributeName.compareTo(attr) == 0) {// TBD
					String strStats = (value.getS() == null ? "0" : value
							.getS());
					stats = Integer.parseInt(strStats);
					System.out.println("getStats::" + "cnt=" + stats);
					break;
				}

			}
		}
		return stats;

	}

	private static void printItem(Map<String, AttributeValue> attributeList) {
		for (Map.Entry<String, AttributeValue> item : attributeList.entrySet()) {
			String attributeName = item.getKey();
			AttributeValue value = item.getValue();
			System.out.println(attributeName
					+ " "
					+ (value.getS() == null ? "" : "S=[" + value.getS() + "]")
					+ (value.getN() == null ? "" : "N=[" + value.getN() + "]")
					+ (value.getB() == null ? "" : "B=[" + value.getB() + "]")
					+ (value.getSS() == null ? "" : "SS=[" + value.getSS()
							+ "]")
					+ (value.getNS() == null ? "" : "NS=[" + value.getNS()
							+ "]")
					+ (value.getBS() == null ? "" : "BS=[" + value.getBS()
							+ "] \n"));
		}
	}

}
