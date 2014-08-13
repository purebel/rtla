package com.rtla.kinesis;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import com.rtla.kinesis.AWSKinesisHelper;

public class Record {
	String ts;
	List<String> coorx;
	List<String> coory;
	List<String> mac;
	Integer tsDelta;
	
	List<String> entityEnum;
	List<String> locationMapHier;
	List<String> coorUnit;
	List<String> moveDistanceInFt;
	List<String> refMarkerName;
	List<String> subscripName;
	

	// tsDelta is the sec to wait before sending next record.
	public Record(List<String> coorx, List<String> coory, List<String> mac, Integer tsDelta, String ts, 
			List<String> entityEnum, List<String> locationMapHier, List<String> coorUnit, List<String> moveDistanceInFt,
			List<String> refMarkerName, List<String> subscripName) {

		this.coorx = ((List) ((ArrayList) coorx).clone());//coorx;
		this.coory = ((List) ((ArrayList) coory).clone());//coory;
		this.mac=((List) ((ArrayList) mac).clone());
		this.tsDelta = tsDelta;
		this.ts=ts;
		
		this.entityEnum=((List) ((ArrayList) entityEnum).clone());
		this.locationMapHier=((List) ((ArrayList) locationMapHier).clone());
		this.coorUnit=((List) ((ArrayList) coorUnit).clone());
		this.moveDistanceInFt=((List) ((ArrayList) moveDistanceInFt).clone());
		this.refMarkerName=((List) ((ArrayList) refMarkerName).clone());
		this.subscripName=((List) ((ArrayList) subscripName).clone());
		
	}

	public static void main(String[] args) {
		//Add by Jason
		AWSKinesisHelper helper = AWSKinesisHelper.getInstance();

		try {
		
		     helper.prepareStream("RTLA", 1);
		} catch (Exception e1) {
		     // TODO Auto-generated catch block
		     e1.printStackTrace();
		}

		// TODO Auto-generated method stub
		List<Record> record_q = new ArrayList<Record>();
		try {
//			FileInputStream fstream = new FileInputStream("event_movement_archive.csv");
			FileInputStream fstream = new FileInputStream("event_movement_archive copy.csv");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));

			String strLine;
			String strTS;
			String[] coor;


			List<String> coorx_q = new ArrayList<String>();
			List<String> coory_q = new ArrayList<String>();
			List<String> mac_q = new ArrayList<String>();
			
			List<String> entityEnum_q=new ArrayList<String>();
			List<String> locationMapHier_q = new ArrayList<String>();
			List<String> coorUnit_q = new ArrayList<String>();
			List<String> moveDistanceInFt_q = new ArrayList<String>();
			List<String> refMarkerName_q = new ArrayList<String>();
			List<String> subscripName_q = new ArrayList<String>();
			
			boolean firstLine = true;
			Integer lastHour = 0;
			Integer lastMin = 0;
			Integer lastSec = 0;
			Integer secDelta = 0;
			Integer hour = 0;
			Integer min = 0;
			Integer sec = 0;


			String currentLine=br.readLine();
			String nextLine=br.readLine();
			Integer currentLineIsEmpty=1;
			Integer nextLineIsEmpty=1;
			Integer wcnt;
			Integer lineCnt=0;
			
			for(wcnt=0; wcnt<currentLine.length(); wcnt++) {
				if(Character.isDigit(currentLine.charAt(wcnt))) {
					currentLineIsEmpty=0;
				}					
			}
			for(wcnt=0; wcnt<nextLine.length(); wcnt++) {
				if(Character.isDigit(nextLine.charAt(wcnt))) {
					nextLineIsEmpty=0;
				}					
			}
			System.out.println(":lineCnt=" + lineCnt);
			System.out.println(":currentLineIsEmpty=" + currentLineIsEmpty);
			System.out.println(":nextLineIsEmpty=" + nextLineIsEmpty);			
			while (currentLineIsEmpty==0) {

				lineCnt++;
				coor = currentLine.split(",");
				
				// get Timestamp
				strTS = coor[11];
				System.out.println("----Current TS:" + strTS);
				String[] ts = strTS.split(":");
				String strHour = ts[0];
				String minSec = ts[1];
				String[] min_sec = minSec.split("\\.");
				String strMin = min_sec[0];
				String strSec = min_sec[1];

				hour = Integer.valueOf(strHour);
				min = Integer.valueOf(strMin);
				sec = Integer.valueOf(strSec);
				System.out.println("Coordx is 5:" + coor[5]);
				System.out.println("Coordy is 5:" + coor[6]);
				if (firstLine == true) {
					coorx_q.add(coor[5]);
					coory_q.add(coor[6]);
					mac_q.add(coor[1]);

					entityEnum_q.add(coor[2]);
					refMarkerName_q.add(coor[3]);
					coorUnit_q.add(coor[4]);
					locationMapHier_q.add(coor[7]);
					moveDistanceInFt_q.add(coor[8]);
					subscripName_q.add(coor[10]);

					firstLine = false;
					System.out.println("First line Coordx is :" + coor[5]);
					System.out.println("First line Coordy is :" + coor[6]);
					System.out.println("First line mac is :" + coor[1]);
					System.out.println("coorx_q.size=" + coorx_q.size());
					System.out.println("coorx_q last item=" + coorx_q.get(coorx_q.size()-1));

					Record record = new Record(coorx_q, coory_q, mac_q, 0, strTS, 
							                   entityEnum_q, locationMapHier_q, coorUnit_q, 
							                   moveDistanceInFt_q, refMarkerName_q, subscripName_q);
					record_q.add(record);
					coorx_q.clear();
					coory_q.clear();
					mac_q.clear();
					entityEnum_q.clear();
					refMarkerName_q.clear();
					coorUnit_q.clear();
					locationMapHier_q.clear();
					moveDistanceInFt_q.clear();
					subscripName_q.clear();
					lastHour = hour;
					lastMin = min;
					lastSec = sec;
					System.out.println("After push, record_q.size=" + record_q.size());	
					// push to the same q
/*				} else if ((hour == lastHour) && (min == lastMin)
						&& (sec == lastSec)) {
					coorx_q.add(coor[5]);
					coory_q.add(coor[6]);
					mac_q.add(coor[1]);
					System.out.println("Push Coordx is :" + coor[5]);
					System.out.println("Push Coordy is :" + coor[6]);
					// push the last record and create new record.
*/				} else {
	                
					secDelta = (hour*3600+min*60+sec)-(lastHour*3600+lastMin*60+lastSec);
					coorx_q.add(coor[5]);
					coory_q.add(coor[6]);
					mac_q.add(coor[1]);
					entityEnum_q.add(coor[2]);
					refMarkerName_q.add(coor[3]);
					coorUnit_q.add(coor[4]);
					locationMapHier_q.add(coor[7]);
					moveDistanceInFt_q.add(coor[8]);
					subscripName_q.add(coor[10]);
					
					Record record = new Record(coorx_q, coory_q, mac_q, secDelta, strTS, 
			                                   entityEnum_q, locationMapHier_q, coorUnit_q, 
			                                   moveDistanceInFt_q, refMarkerName_q, subscripName_q);
					record_q.add(record);

					coorx_q.clear();
					coory_q.clear();
					mac_q.clear();
					entityEnum_q.clear();
					refMarkerName_q.clear();
					coorUnit_q.clear();
					locationMapHier_q.clear();
					moveDistanceInFt_q.clear();
					subscripName_q.clear();
					
					lastHour = hour;
					lastMin = min;
					lastSec = sec;

					System.out.println("New Push Coordx is :" + coor[5]);
					System.out.println("New Push Coordy is :" + coor[6]);
					System.out.println("New Push tsDelta is :" + secDelta);
					System.out.println("After push, record_q.size=" + record_q.size());	
				}
/*
				if(nextLineIsEmpty==1) {
					System.out.println("!!Last line");
					secDelta = 0;
				    Record record = new Record(coorx_q, coory_q, mac_q, secDelta, strTS);
				    record_q.add(record);
				    System.out.println("Coordx is 2:" + coor[5]);
				    System.out.println("Coordy is 2:" + coor[6]);
				    System.out.println("record_q.size:" + record_q.size());
				}
*/				

				currentLine=nextLine;
				nextLine=br.readLine();
				System.out.println("currentLine:" + currentLine);
				System.out.println("nextLine:" + nextLine);
				currentLineIsEmpty=nextLineIsEmpty;//1;
				nextLineIsEmpty=1;
				wcnt=0;		
				if(nextLine == null) {
					
					nextLineIsEmpty=1;
				}else{
				for(wcnt=0; wcnt<nextLine.length(); wcnt++) {
					if(Character.isDigit(nextLine.charAt(wcnt))) {
						nextLineIsEmpty=0;
					}					
				}
				}
				/*
				for(wcnt=0; wcnt<currentLine.length(); wcnt++) {
					if(Character.isDigit(currentLine.charAt(wcnt))) {
						currentLineIsEmpty=0;
					}					
				}
				*/
				System.out.println(":lineCnt=" + lineCnt);
				System.out.println(":currentLineIsEmpty=" + currentLineIsEmpty);
				System.out.println(":nextLineIsEmpty=" + nextLineIsEmpty);

			}

		    
			System.out.println("----Finish file handling---");


			in.close();
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}

		// HTTP client
		try {

			
			System.out.println("-----Start HTTP handling--------");
			System.out.println("record_q.size=" + record_q.size());
			Integer i=0;

			String data="";
			StringBuffer databuf;
			  
		      
/*
			URL url = new URL("http://54.86.238.101:8989");
//			URL url = new URL("http://127.0.0.1:8989");//for local test
	*/
			while(i<record_q.size()) {
				/*
			}
		      HttpURLConnection hc = (HttpURLConnection) url.openConnection();
			  hc.setDoOutput(true);


			  OutputStreamWriter out=new OutputStreamWriter(hc.getOutputStream());
*/				
			      
			  Record item=record_q.get(i);
			  Integer waitTime=item.tsDelta*1000;
			  System.out.println("Get item " + i);
			  System.out.println("Get item tsDelta:" + item.tsDelta);
			  System.out.println("Get item waitTime:" + waitTime);
			  
			  data="";
			  databuf=new StringBuffer();
			  
		      System.out.println("Get record item");
		      databuf.append("{");
			  for(Integer j=0; j<item.coorx.size(); j++) {
				  
				  String type="movement-event";
				  String subscripName="";
				  String entityEnum="";
				  String locationMapHier="";
				  String coorUnit="";
				  String moveDistanceInFt="";
				  String refMarkerName="";
				  
				  String coorx="";
				  String coory="";
				  String mac="";
				  String ts="";
				  
				  /*
			      mac=URLEncoder.encode("mac", "UTF-8") + "=" + URLEncoder.encode(item.mac.get(j), "UTF-8");
				  coorx=URLEncoder.encode("coorx", "UTF-8") + "=" + URLEncoder.encode(item.coorx.get(j), "UTF-8");
			      coory=URLEncoder.encode("coory", "UTF-8") + "=" + URLEncoder.encode(item.coory.get(j), "UTF-8");

			      
			      databuf.append(mac);
			      databuf.append(",");
			      databuf.append(coorx);
			      databuf.append(",");
			      databuf.append(coory);
			      databuf.append(",");	
			      */
				  mac=item.mac.get(j);
				  coorx=item.coorx.get(j);
				  coory=item.coory.get(j);
				  ts=item.ts;
				  
				  entityEnum=item.entityEnum.get(j);
				  refMarkerName=item.refMarkerName.get(j);
				  coorUnit=item.coorUnit.get(j);
				  locationMapHier=item.locationMapHier.get(j);
				  moveDistanceInFt=item.moveDistanceInFt.get(j);
				  subscripName=item.subscripName.get(j);
				  
				  
				  databuf.append("\"type\":\"");
				  databuf.append(type);
				  databuf.append("\",");
				  
				  databuf.append("\"properties\":{");
				      //subscriptionName
				      databuf.append("\"subscriptionName\":{");
				        databuf.append("\"type\":\"");
				        databuf.append(subscripName);
				      databuf.append("\"},");
				      //entity
				      databuf.append("\"entity\":{");
				        databuf.append("\"type\":\"");
				        databuf.append("string");
				        databuf.append("\",");
				        databuf.append("\"enum\":\"");
				        databuf.append(entityEnum);
				      databuf.append("\"},");		
				      //deviceId=MAC
				      databuf.append("\"deviceId\":{");
				        databuf.append("\"type\":\"");
				        databuf.append(mac);
				      databuf.append("\"},");
				      //locationMapHier
				      databuf.append("\"locationMapHierarchy\":{");
				        databuf.append("\"type\":\"");
				        databuf.append(locationMapHier);
				      databuf.append("\"},");
				      //location coor
				      databuf.append("\"locationCoordinate\":{");
				        databuf.append("\"type\":\"");
				        databuf.append("locationCoordinate");
				        databuf.append("\",");
				        databuf.append("\"properties\":{");
				          databuf.append("\"x\":{");
					        databuf.append("\"type\":\"");				          
				            databuf.append(coorx);
				          databuf.append("\"},");
				          databuf.append("\"y\":{");
					        databuf.append("\"type\":\"");
					        databuf.append(coory);
				          databuf.append("\"},");				          
				          databuf.append("\"unit\":{");
					        databuf.append("\"type\":\"");
					        databuf.append("string");
					        databuf.append("\",");
					        databuf.append("\"enum\":\"");					        
					        databuf.append(coorUnit);
				          databuf.append("\"}");
				        databuf.append("}"); //properties			        
				      databuf.append("},");	//locationCoordinate
				      //moveDistanceInFt
					  databuf.append("\"moveDistanceInFt\":{");
					    databuf.append("\"type\":\"");
					    databuf.append(moveDistanceInFt);
					  databuf.append("\"},");
					  //refMarkerName
					  databuf.append("\"referenceMarkerName\":{");
					    databuf.append("\"type\":\"");
					    databuf.append(refMarkerName);
					  databuf.append("\"},");		
					  
					  //ts
					  databuf.append("\"timestamp\":{");
					    databuf.append("\"type\":\"");
					    databuf.append(ts);
					  databuf.append("\"}");							  
				  //end of properties  
				  databuf.append("}");				    

/*				  
				  databuf.append("\"MAC\":\"");
				  databuf.append(mac);
				  databuf.append("\",");
				  databuf.append("\"X\":\"");
				  databuf.append(coorx);;
				  databuf.append("\",");				  
				  databuf.append("\"Y\":\""); 
				  databuf.append(coory);
				  databuf.append("\",");	
				  databuf.append("\"Timestamp\":\""); 
				  databuf.append(ts);
				  databuf.append("\"");				  
*/
			  }		
			  databuf.append("}");
			  data=databuf.toString();
			  
		      System.out.println("formed data:" + data);
		      
				//Add by jason
				helper.sendData(data);
				
/*			  out.write( data );//new
			  out.flush(); //new
			  
			  
			  BufferedReader reader=new BufferedReader(new InputStreamReader(hc.getInputStream()));//new

		      StringBuilder sb=new StringBuilder();

		      String line=null;
			  while((line=reader.readLine()) != null) {
				  
				  sb.append(line+"\n");
			  }

			  String test=sb.toString();
//		      System.out.println("output stream:" + test);
//		      System.out.println("-----Finish out.write--------" + waitTime);
		     
		      System.out.println(new Date());
		      Thread.sleep(waitTime);//waitTime);

//		      System.out.println("-----Done wait--------");
		      System.out.println(new Date());
			  
			  i++;
			  out.close();
			  */
		      i++;
			}
			
//			helper.sendData("RTLocation");

		      System.out.println("-----Finish while--------");
		      
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}

	}
}
