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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;


public class Heatmap {
//	private static AWSKinesisHelper instance = null;
	private static AmazonKinesisClient kinesisClient = null;
	protected static AmazonDynamoDBClient client;
	static AWSKinesisHelper Helper = AWSKinesisHelper.getInstance();
	static AWSCredentials credentials = null;	
	static List<Record> records;
	static Record rec;
  	static int coorxStep=1;
  	static int cooryStep=1;
	static String DBTableName = "heatMap";
	static int firstRec=0;
	static String date="";
	static int heatMapStep=10;
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub

    	credentials = new ProfileCredentialsProvider().getCredentials();
    	kinesisClient = new AmazonKinesisClient(credentials);


        createClient();
		System.out.println("Done client create" );
        try {

        	getRecords();
    		System.out.println("Done getRecords" );

        } catch (AmazonServiceException ase) {
            System.err.println("Data load script failed.");
        }	
	}//main

        private static void createClient() throws Exception {
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
    				uploadItems(DBTableName, recStr);
    				System.out.println("Done 1 item put");
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

              	String jsStr=recStr;
           
            	
              	System.out.println("Rec to JSON String:"+jsStr);            	

            	JSONObject dataJson = JSONObject.fromObject(jsStr);
            	  
                	
            	JSONObject propertiesJ=dataJson.getJSONObject("properties");     	   
            	JSONObject locationJ=propertiesJ.getJSONObject("locationCoordinate");
            	JSONObject locationPropJ=locationJ.getJSONObject("properties");
            	JSONObject coorx=locationPropJ.getJSONObject("x");
            	String coorxVal =coorx.getString("type");
            	JSONObject coory=locationPropJ.getJSONObject("y");
            	String cooryVal =coory.getString("type");
     
             	   
             	   JSONObject timestampJ=propertiesJ.getJSONObject("timestamp");
             	   String timestamp=timestampJ.getString("type");   
             	   

                  	System.out.println("JSON to JAVA array.coorx="+coorxVal);
                  	System.out.println("JSON to JAVA array.coory="+cooryVal);  
                  	System.out.println("JSON to JAVA array.timestamp="+timestamp);  
                  	
                    

                  	float coorxFloat=Float.parseFloat(coorxVal);
                  	int coorxInt=(int)coorxFloat;
                  	String districtX=Integer.toString(coorxInt/coorxStep);
                  	System.out.println("JSON to JAVA array.districtx="+districtX); 
                  	
                  	float cooryFloat=Float.parseFloat(cooryVal);
                  	int cooryInt=(int)cooryFloat;
                  	String districtY=Integer.toString(cooryInt/cooryStep);  
                  	System.out.println("JSON to JAVA array.districty="+districtY); 
                  	
                  	String districtStr=districtX+"-"+districtY;
                  	System.out.println("JSON to JAVA array.district="+districtStr);
                  	
                  	String[] tsArr=timestamp.split(" ");
                  	String curDate=tsArr[0];
        			String[] time=tsArr[1].split(":");
        			String strHour = time[0];
        			String strMin = time[1];
        			String[] secArr=time[2].split("\\.");
        			String strSec = secArr[0];
        			System.out.println("time:"+curDate  + strHour +strMin +strSec);
        			int min=Integer.parseInt(strMin);

        			int minCode=min/heatMapStep;
        			String strMinCode=Integer.toString(minCode);


                  	System.out.println("Derive current date:"+curDate);
                  	System.out.println("Derive hour:"+strHour + ", min:" +min);
                  	System.out.println("Derive minCode:"+minCode + ", strMinCode" + strMinCode);
                  	
/*                  	if(firstRec==0) {
                  		date=curDate;
                  		firstRec=1;
                      	System.out.println("First record, init date="+date);                  		
                  	}
 */                 	
        			String tsCode=curDate+" "+strHour+":"+strMinCode;
       
                  	//get stats from table
                  	int cnt=getStats(tableName, districtStr, tsCode);
                	System.out.println("getStats return" + "cnt=" + cnt);
                  	cnt++;
                	System.out.println("increase by 1, and " + "cnt=" + cnt);
                  	String strCnt=Integer.toString(cnt);
                  	
                  	
                Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
                item.put("district", new AttributeValue().withS(districtStr));
                item.put("ts", new AttributeValue().withS(tsCode));
                item.put("cnt", new AttributeValue().withS(strCnt));


                
                PutItemRequest itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
                client.putItem(itemRequest);
                item.clear();

                    
            }   catch (AmazonServiceException ase) {
                System.err.println("Failed to create item in " + tableName + " " + ase);
            } 
        }
            private static int getStats(String tableName,String district, String tsCode) {
            	int stats=0;
            	
            	Condition hashKeyCondition = new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withS(district));
            	
            	Condition rangeKeyCondition = new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withS(tsCode));
            	
            Map<String, Condition> keyConditions = new HashMap<String, Condition>();
            keyConditions.put("district", hashKeyCondition);
            keyConditions.put("ts", rangeKeyCondition);
            

            QueryRequest queryRequest = new QueryRequest()
                .withTableName(tableName)
                .withKeyConditions(keyConditions)
                .withAttributesToGet("cnt");
               
            	
                
                QueryResult result = client.query(queryRequest);

                // Check the response.
                for (Map<String, AttributeValue> item : result.getItems()) {
                    printItem(item);
                }           
                for (Map<String, AttributeValue> item : result.getItems()) {
                    for(Map.Entry<String, AttributeValue> item2 : item.entrySet()) {
                        String attributeName = item2.getKey();
                        AttributeValue value = item2.getValue();
                        String attr="cnt";
                    	System.out.println("getStats::" + "attributeName=" + attributeName + ", cmp="+attributeName.compareTo(attr));                        
                        if(attributeName.compareTo(attr)==0) {//TBD
                        	String strStats=(value.getS()==null?"0":value.getS());
                        	stats=Integer.parseInt(strStats);
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
                    System.out.println(attributeName + " "
                            + (value.getS() == null ? "" : "S=[" + value.getS() + "]")
                            + (value.getN() == null ? "" : "N=[" + value.getN() + "]")
                            + (value.getB() == null ? "" : "B=[" + value.getB() + "]")
                            + (value.getSS() == null ? "" : "SS=[" + value.getSS() + "]")
                            + (value.getNS() == null ? "" : "NS=[" + value.getNS() + "]")
                            + (value.getBS() == null ? "" : "BS=[" + value.getBS() + "] \n"));
                }
            }
            

        		
		
		
}
