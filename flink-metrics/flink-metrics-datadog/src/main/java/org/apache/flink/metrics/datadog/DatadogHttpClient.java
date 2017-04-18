package org.apache.flink.metrics.datadog;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.RequestBody;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 *
 * */
public class DatadogHttpClient{
	private static final String SERIES_URL_FORMAT = "https://app.datadoghq.com/api/v1/series?api_key=%s";
	private static final String VALIDATE_URL_FORMAT = "https://app.datadoghq.com/api/v1/validate?api_key=%s";
	private static final MediaType MEDIA_TYPE = MediaType.parse("application/json; charset=utf-8");
	private static final int TIMEOUT = 5;

	private final String seriesUrl;
	private final String validateUrl;
	private final OkHttpClient client;
	private final String apiKey;

	public DatadogHttpClient(String dgApiKey) {
		if(dgApiKey == null || dgApiKey.isEmpty()) {
			throw new IllegalArgumentException(
				"Invalid API key:" + dgApiKey);
		}
		apiKey = dgApiKey;
		client = new OkHttpClient.Builder()
			.connectTimeout(TIMEOUT, TimeUnit.SECONDS)
			.writeTimeout(TIMEOUT, TimeUnit.SECONDS)
			.readTimeout(TIMEOUT, TimeUnit.SECONDS)
			.build();

		seriesUrl = String.format(SERIES_URL_FORMAT, apiKey);
		validateUrl = String.format(VALIDATE_URL_FORMAT, apiKey);
		validateApiKey();
	}

	private void validateApiKey() {
		Request r = new Request.Builder().url(validateUrl).get().build();

		try {
			Response response = client.newCall(r).execute();
			if(!response.isSuccessful()) {
				throw new IllegalArgumentException(
					String.format("API key: %s is invalid", apiKey));
			}
		} catch(IOException e) {
			throw new IllegalStateException("Failed contacting Datadog to validate API key");
		}
	}

	public void syncPost(String postBody) throws IOException {
		Request request = new Request.Builder()
			.url(seriesUrl)
			.post(RequestBody.create(MEDIA_TYPE, postBody))
			.build();

		client.newCall(request).execute().close();
	}
}
