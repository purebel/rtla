package com.rtla.kinesis;

import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

public class RTLAKinesisApplication {
    private static final String DEFAULT_APP_NAME = "RTLAKinesisApplication";
    private static final String DEFAULT_STREAM_NAME = "RTLA_demo";

    private static final String DEFAULT_KINESIS_ENDPOINT = "https://kinesis.us-east-1.amazonaws.com";

    // Initial position in the stream when the application starts up for the first time.
    // Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)
    private static final InitialPositionInStream DEFAULT_INITIAL_POSITION = InitialPositionInStream.TRIM_HORIZON;

    private static String applicationName = DEFAULT_APP_NAME;
    private static String streamName = DEFAULT_STREAM_NAME;
    private static String kinesisEndpoint = DEFAULT_KINESIS_ENDPOINT;
    private static InitialPositionInStream initialPositionInStream = DEFAULT_INITIAL_POSITION;

    private static KinesisClientLibConfiguration kinesisClientLibConfiguration;

    private static final Log LOG = LogFactory.getLog(RTLAKinesisApplication.class);
    
	private RTLAKinesisApplication() {
		super();
	}
	
	/**
	 * 
	 * @param args Property file with configuration overrides
	 * @throws IOException Thrown if can't read properties
	 */
	public static void main(String[] args) throws IOException {
		
		/*
		String propertiesFile = null;

        if (args.length > 1) {
            System.err.println("Usage: java " + RTLAKinesisApplication.class.getName() + " <propertiesFile>");
            System.exit(1);
        } else if (args.length == 1) {
            propertiesFile = args[0];
        }
		 */
        //configure(propertiesFile);
		
		configure();
       
        
        System.out.println("Starting " + applicationName);
        LOG.info("Running " + applicationName + " to process stream " + streamName);


        IRecordProcessorFactory recordProcessorFactory = new RTLARecordProcessorFactory();
         Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);

        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            LOG.error("Caught throwable while processing data.", t);
            exitCode = 1;
        }
        System.exit(exitCode);
	}
	
	//private static void configure(String propertiesFile) throws IOException {
	private static void configure() throws IOException {

        // ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl" , "60");

        String workerId = /*InetAddress.getLocalHost().getCanonicalHostName() */"127.1.1.1"+ ":" + UUID.randomUUID();
        LOG.info("Using workerId: " + workerId);

        // Get credentials from IMDS. If unsuccessful, get them from the credential profiles file.
       AWSCredentialsProvider credentialsProvider = null;
        try {
            credentialsProvider = new InstanceProfileCredentialsProvider();
            // Verify we can fetch credentials from the provider
            credentialsProvider.getCredentials();
            LOG.info("Obtained credentials from the IMDS.");
        } catch (AmazonClientException e) {
            LOG.info("Unable to obtain credentials from the IMDS, trying classpath properties", e);
            credentialsProvider = new ProfileCredentialsProvider();
            // Verify we can fetch credentials from the provider
            credentialsProvider.getCredentials();
            LOG.info("Obtained credentials from the properties file.");
        }

        LOG.info("Using credentials with access key id: " + credentialsProvider.getCredentials().getAWSAccessKeyId());

        kinesisClientLibConfiguration = new KinesisClientLibConfiguration(applicationName, streamName,
                credentialsProvider, workerId).withInitialPositionInStream(initialPositionInStream);
    }
}
