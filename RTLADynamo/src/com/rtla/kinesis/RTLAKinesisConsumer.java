package com.rtla.kinesis;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Date;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONObject.*;
import net.sf.ezmorph.*;
import org.apache.commons.lang.*;
import org.apache.commons.beanutils.*;
import org.apache.commons.collections.*;
import com.alibaba.fastjson.*;



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
//import com.amazonaws.services.kinesis.AmazonKinesisClient;
//import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
//import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.rtla.kinesis.AWSKinesisHelper;

//import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
//import com.amazonaws.services.dynamodb.*;
import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;


public class RTLAKinesisConsumer {
//	private static AWSKinesisHelper instance = null;
	private static AmazonKinesisClient kinesisClient = null;
	protected static AmazonDynamoDBClient client;
	static AWSKinesisHelper Helper = AWSKinesisHelper.getInstance();
	static AWSCredentials credentials = null;	
	static List<Record> records;
	static Record rec;
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		String DBTableName = "MSE";//"EventMovement";
    	credentials = new ProfileCredentialsProvider().getCredentials();
    	kinesisClient = new AmazonKinesisClient(credentials);//Helper.setupKinesisClient();

    	//-------TEST Start --------//
/*  
    	String jsStr="{\"locationCoordinate\": {"
    		+ "\"type\":\"locationCoordinate\","
    		+"\"properties\":{\"x\":{\"type\":\"70.24668\"},"
    		+ "    		      \"y\":{\"type\":\"85.13588\"},"
    		+ "    		       \"unit\":{\"type\":\"string\",\"enum\":\"FEET\"}}}}";

    	  JSONObject dataJson = JSONObject.fromObject(jsStr);
    	   
    	JSONObject resp=dataJson.getJSONObject("locationCoordinate");
    	JSONObject data=resp.getJSONObject("properties");
    	JSONObject coorx=data.getJSONObject("x");
    	String coorxVal =coorx.getString("type");

    	JSONObject coory=data.getJSONObject("y");
    	String cooryVal =coory.getString("type");

        	System.out.println("JSON to JAVA array.coorx="+coorxVal);
        	System.out.println("JSON to JAVA array.coory="+cooryVal);        		
*/
    	//-------TEST END --------//
    	

        createClient();
		System.out.println("Done client create" );
        try {

        	getRecords();
    		System.out.println("Done getRecords" );
 //           uploadItems(DBTableName);
 //   		System.out.println("Done uploadItems" );

        } catch (AmazonServiceException ase) {
            System.err.println("Data load script failed.");
        }	
	}//main

        private static void createClient() throws Exception {

//new PropertiesCredentials(
    				//RTLAKinesisConsumer.class.getResourceAsStream("AwsCredentials.properties"));

            client = new AmazonDynamoDBClient(credentials);
        }//createClient
        
        
        private static void getRecords() {
        	String StreamName="RTLA";
    		// Retrieve the Shards from a Stream
    		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
    		describeStreamRequest.setStreamName(StreamName);
    		DescribeStreamResult describeStreamResult;
    		List<Shard> shards = new ArrayList<>();
    		String lastShardId = null;

    		System.out.println("Enterning uploadItems" );   		
    		do {
        		System.out.println("handling shard" );   			
    		    describeStreamRequest.setExclusiveStartShardId(lastShardId);
        		System.out.println("get lastShardId" );  
        		describeStreamResult = kinesisClient.describeStream(describeStreamRequest);
        		System.out.println("get describeStreamResult" );  
        		shards.addAll(describeStreamResult.getStreamDescription().getShards());
        		System.out.println("add shard" );   
    		    if (describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0) {
    		        lastShardId = shards.get(shards.size() - 1).getShardId();
    		    } else {
    		    	lastShardId = null;
    		    }
    		} while (lastShardId != null);

    		System.out.println("Done shards derive" );
    		// Get Data from the Shards in a Stream
    		// Hard-coded to use only 1 shard
    		String shardIterator;
    		GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
    		getShardIteratorRequest.setStreamName(StreamName);
    		getShardIteratorRequest.setShardId(shards.get(0).getShardId());
    		getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");

    		GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);
    		shardIterator = getShardIteratorResult.getShardIterator();

    		// Continuously read data records from shard.

    		while (true) {
    			// Create new GetRecordsRequest with existing shardIterator.
    			// Set maximum records to return to 1000.
    			GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
    			getRecordsRequest.setShardIterator(shardIterator);
    			getRecordsRequest.setLimit(1000);

    			GetRecordsResult result = kinesisClient.getRecords(getRecordsRequest);

    			// Put result into record list. Result may be empty.
    			records = result.getRecords();

    			// Print records
    			for (int i=0; i<records.size(); i++) {
    				rec=records.get(i);
    				ByteBuffer byteBuffer = rec.getData();
                	String recStr=new String(byteBuffer.array());
    				System.out.println(String.format("Seq No: %s", rec.getSequenceNumber()));
    				System.out.println("recStr="+recStr);
    				uploadItems("MSE", recStr);
    				System.out.println("Done 1 item put");
//    				records.remove(i);
    			}
    			records.clear();

    			try {
    				Thread.sleep(1000);
    			} catch (InterruptedException exception) {
    				throw new RuntimeException(exception);
    			}

    			shardIterator = result.getNextShardIterator();
    		}
        	
        }


    
        private static void uploadItems(String tableName, String recStr) {
            
            try {
              	System.out.println("Entering uploadItems");  
              	// Add books.

//            	String recArray[]=recStr.split("\\+");
//            	String jsStr=recArray[1];
              	String jsStr=recStr;
           
            	
              	System.out.println("Rec to JSON String:"+jsStr);            	

            	  JSONObject dataJson = JSONObject.fromObject(jsStr);
            	  
            	  String type=dataJson.getString("type");
                	
            	   JSONObject propertiesJ=dataJson.getJSONObject("properties");
 //           	   JSONObject subscriptionNameJ=propertiesJ.getJSONObject("subscriptionName");
//            	   String subscriptionName=subscriptionNameJ.getString("type");

               	   JSONObject deviceIdJ=propertiesJ.getJSONObject("deviceId");
            	   String deviceId=deviceIdJ.getString("type");            	   
            	   
     	   
            	JSONObject locationJ=propertiesJ.getJSONObject("locationCoordinate");
            	JSONObject locationPropJ=locationJ.getJSONObject("properties");
            	JSONObject coorx=locationPropJ.getJSONObject("x");
            	String coorxVal =coorx.getString("type");
            	JSONObject coory=locationPropJ.getJSONObject("y");
            	String cooryVal =coory.getString("type");

//              	System.out.println("JSON to JAVA 3");
                	
             	   JSONObject moveDistanceInFtJ=propertiesJ.getJSONObject("moveDistanceInFt");
             	   String moveDistance=moveDistanceInFtJ.getString("type");      
             	   
             	   JSONObject timestampJ=propertiesJ.getJSONObject("timestamp");
             	   String timestamp=timestampJ.getString("type");   
             	   
                 	System.out.println("JSON to JAVA array.type="+type);
                 	System.out.println("JSON to JAVA array.mac="+deviceId);
                  	System.out.println("JSON to JAVA array.coorx="+coorxVal);
                  	System.out.println("JSON to JAVA array.coory="+cooryVal);  
                  	System.out.println("JSON to JAVA array.moveDist="+moveDistance);
                  	System.out.println("JSON to JAVA array.timestamp="+timestamp);              
            	
                Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
                item.put("mac", new AttributeValue().withS(deviceId));
                item.put("ts", new AttributeValue().withS(timestamp));


                
                PutItemRequest itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
                client.putItem(itemRequest);
                item.clear();
                //for test
                /*
                item.put("mac", new AttributeValue().withS(deviceId));
                String tmp_ts="0";
                item.put("ts", new AttributeValue().withS(tmp_ts));
                
                client.putItem(itemRequest);
                item.clear();
                */

                    
            }   catch (AmazonServiceException ase) {
                System.err.println("Failed to create item in " + tableName + " " + ase);
            } 

        }		
		
		
		
		/*
		
//		AmazonKinesisClient kinesisClient = new Helper.setupKinesisClient();
		AWSCredentials credentials = null;
		kinesisClient = new AmazonKinesisClient(credentials);
		dbclient = new AmazonDynamoDBClient(credentials);
		// Retrieve the Shards from a Stream
		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		DescribeStreamRequest.setStreamName(Helper.properties().getProperty("RTLocation"));
		DescribeStreamResult describeStreamResult;
		List<Shard> shards = new ArrayList<>();
		String lastShardId = null;

		do {
		    describeStreamRequest.setExclusiveStartShardId(lastShardId);
		    describeStreamResult = kinesisClient.describeStream(describeStreamRequest);
		    shards.addAll(describeStreamResult.getStreamDescription().getShards());
		    if (shards.size() > 0) {
		        lastShardId = shards.get(shards.size() - 1).getShardId();
		    }
		} while (describeStreamResult.getStreamDescription().getHasMoreShards());

		// Get Data from the Shards in a Stream
		// Hard-coded to use only 1 shard
		String shardIterator;
		GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
		getShardIteratorRequest.setStreamName(Helper.properties().getProperty("kinesisStreamName"));
		getShardIteratorRequest.setShardId(shards.get(0).getShardId());
		getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");

		GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);
		shardIterator = getShardIteratorResult.getShardIterator();

		// Continuously read data records from shard.
		List<Record> records;
		while (true) {
			// Create new GetRecordsRequest with existing shardIterator.
			// Set maximum records to return to 1000.
			GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
			getRecordsRequest.setShardIterator(shardIterator);
			getRecordsRequest.setLimit(1000);

			GetRecordsResult result = kinesisClient.getRecords(getRecordsRequest);

			// Put result into record list. Result may be empty.
			records = result.getRecords();

			// Print records
			for (Record record : records) {
				ByteBuffer byteBuffer = record.getData();

///≤Â»Î dynamodb
			try{
			String tableName = "MSE";
			Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
			item.put("mac", new AttributeValue().withN("mac address"));
			item.put("ts", new AttributeValue().withS("timestamp"));
			item.put("px", new AttributeValue().withS("x postion"));
			item.put("py", new AttributeValue().withS("y postion"));
			PutItemRequest putItemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            		PutItemResult result = dbclient.putItem(putItemRequest);
			}catch (AmazonServiceException ase) {
            			System.err.println("Create items failed.");
			}
				
////
				System.out.println(String.format("Seq No: %s - %s", record.getSequenceNumber(),
						new String(byteBuffer.array())));
			}

			try {
				Thread.sleep(1000);
			} catch (InterruptedException exception) {
				throw new RuntimeException(exception);
			}

			shardIterator = result.getNextShardIterator();
		}
		
     }*/
}
