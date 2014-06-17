import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

public class TestHCPost {

	private static HttpPost postForm(String url, Map<String, String> params) {
		HttpPost httpPost = new HttpPost(url);
		List<NameValuePair> nvps = new ArrayList<NameValuePair>();

		Set<String> keySet = params.keySet();
		for (String key : keySet) {
			nvps.add(new BasicNameValuePair(key, params.get(key)));
		}

		try {
			httpPost.setEntity(new UrlEncodedFormEntity(nvps, HTTP.UTF_8));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		return httpPost;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DefaultHttpClient httpclient = new DefaultHttpClient();
		String params[][] = {{"first", "sdfdsfdsfds"}, {"second", "sdfdsfdsfds"}, {"third", "sdfdsfdsfds"}};
		String charset = "";

		for (int i = 0; i < params.length; i++) {
			Map<String, String> payload = new HashMap<String, String>();
			payload.put(params[i][0], params[i][1]);
			HttpPost post = postForm("http://10.200.3.201:8080", payload);
			HttpResponse httpResponse = null;
			try {
				httpResponse = httpclient.execute(post);
				HttpEntity entity = httpResponse.getEntity();
				charset = EntityUtils.getContentCharSet(entity);
				EntityUtils.consume(entity);
				
			} catch (ClientProtocolException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Response status from the server "
					+ httpResponse.getStatusLine() + " " + charset);
		}
		httpclient.getConnectionManager().shutdown();

	}
}
