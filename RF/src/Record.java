import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;
import org.apache.http.protocol.HTTP;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.impl.client.*;
import org.apache.http.impl.*;

import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Record {
	String ts;
	List<String> coorx;
	List<String> coory;
	List<String> mac;
	Integer tsDelta;
	

	// tsDelta is the sec to wait before sending next record.
	public Record(List<String> coorx, List<String> coory, List<String> mac, Integer tsDelta, String ts) {

		this.coorx = ((List) ((ArrayList) coorx).clone());//coorx;
		this.coory = ((List) ((ArrayList) coory).clone());//coory;
		this.mac=((List) ((ArrayList) mac).clone());
		this.tsDelta = tsDelta;
		this.ts=ts;
	}

	public static void main(String[] args) {
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

					firstLine = false;
					System.out.println("First line Coordx is :" + coor[5]);
					System.out.println("First line Coordy is :" + coor[6]);
					System.out.println("First line mac is :" + coor[1]);					System.out.println("coorx_q.size=" + coorx_q.size());
					System.out.println("coorx_q last item=" + coorx_q.get(coorx_q.size()-1));
					Record record = new Record(coorx_q, coory_q, mac_q, 0, strTS);
					record_q.add(record);
					coorx_q.clear();
					coory_q.clear();
					mac_q.clear();
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
					Record record = new Record(coorx_q, coory_q, mac_q, secDelta, strTS);
					record_q.add(record);

					coorx_q.clear();
					coory_q.clear();
					mac_q.clear();
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
			  
		      

			URL url = new URL("http://54.86.238.101:8989");
//			URL url = new URL("http://127.0.0.1:8080");//for local test
	
			while(i<record_q.size()) {
		      HttpURLConnection hc = (HttpURLConnection) url.openConnection();
			  hc.setDoOutput(true);


			  OutputStreamWriter out=new OutputStreamWriter(hc.getOutputStream());
				
			      
			  Record item=record_q.get(i);
			  Integer waitTime=item.tsDelta*1000;
			  System.out.println("Get item " + i);
			  System.out.println("Get item tsDelta:" + item.tsDelta);
			  System.out.println("Get item waitTime:" + waitTime);
			  
			  data="";
			  databuf=new StringBuffer();
			  
		      System.out.println("Get record item");
			  for(Integer j=0; j<item.coorx.size(); j++) {
				  String coorx="";
				  String coory="";
				  String mac="";
			      mac=URLEncoder.encode("mac", "UTF-8") + "=" + URLEncoder.encode(item.mac.get(j), "UTF-8");
				  coorx=URLEncoder.encode("coorx", "UTF-8") + "=" + URLEncoder.encode(item.coorx.get(j), "UTF-8");
			      coory=URLEncoder.encode("coory", "UTF-8") + "=" + URLEncoder.encode(item.coory.get(j), "UTF-8");

			      
			      databuf.append(mac);
			      databuf.append(",");
			      databuf.append(coorx);
			      databuf.append(",");
			      databuf.append(coory);
			      databuf.append(",");			      

			  }		
			  data=databuf.toString();
		      System.out.println("formed data:" + data);

			  out.write( data );//new
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
			}
			
			
		      System.out.println("-----Finish while--------");
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}

	}
}
