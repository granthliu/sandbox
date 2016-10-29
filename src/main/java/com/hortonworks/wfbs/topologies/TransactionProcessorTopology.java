/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
* 
*      http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.hortonworks.wfbs.topologies;

import javax.jms.Session;

import nl.minvenj.nfi.storm.kafka.KafkaSpout;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

import com.hortonworks.wfbs.bolts.IndexBolt;
import com.hortonworks.wfbs.bolts.JMSRepubMessageProducer;
import com.hortonworks.wfbs.bolts.KafkaBolt;
import com.hortonworks.wfbs.bolts.NonPersistJMSBolt;
import com.hortonworks.wfbs.bolts.NormalizationBolt;
import com.hortonworks.wfbs.bolts.StreamingToHDFSBolt;
import com.hortonworks.wfbs.spouts.SpringJmsProvider;
import com.hortonworks.wfbs.spouts.WFBJMSpout;
import com.hortonworks.wfbs.spouts.WFBTupleProducer;
//import nl.minvenj.nfi.storm.kafka.KafkaSpout;
import com.hortonworks.wfbs.utils.ConfigurationClient;

// Arguments (in order, see below for usage examples): 
// $TOPOLOGYNAME  - name of topology
// $NUMWORKER  - number of workers to use
// $DEPLOYMODE - cluster or local
// $EXECTUOR - executor number applied to each bolt/spout
// $SPOUTRATIO - executor ration for spout
// $NORMALIZATIONRATIO  - executor ration for normalization bolt
// $JMSPUBRATIO - executor ration for jms repub bolt 
// $HDFSRATIO - executor ration for hdfs bolt 
// $INDEXRATIO - executor ration for index bolt 
// $PARALLELPATHS - how many parallel spout=>bolt=>bolt paths to run, up to MAX_PARALLEL_PATHS
// $TRUNCATENORMALIZE - for tuning, truncate from normalize onwards
// $TRUNCATEREPUB - for tuning, truncate repub
// $TRUNCATEHDFS - for tuning, truncate hdfs sink
// $TRUNCATEINDEX - for tuning, truncate index
// $PREFETCH JMS - for tuning
// $MAX THREAD POOL SIZE JMS
// $JMS ASYNCH SESSION
// $JMS OPTIMIZE ACK
// $HDFS ARCHIVE RAW
// $SLEEP TIME JMS SPOUT
// $KAFKA ON
// $HDFS INLINE RAW

/*USAGE - LOOK HERE
I know it's brittle....example run from command line from root of this project after you 'mvn package':
storm jar target/poc-1.0-SNAPSHOT-jar-with-dependencies.jar com.hortonworks.wfbs.topologies.TransactionProcessorTopology WFB 80 cluster 10 1 4 4 4 6 1 false true false true 25000 1500 false true false 0 true false true

Example Run Configuration Options for Eclipse:
WFB 4 local 2 1 1 1 1 1 1 false false false false 10000 1500 false true false 0 true false true
*/

public class TransactionProcessorTopology {
	public static final String JMS_QUEUE_SPOUT = "stream_data_spout";
	public static final String KAFKA_QUEUE_SPOUT = "kafka_data_spout";
	public static final String NORMALIZATION_BOLT = "normalization_bolt";
	public static final String INDEX_BOLT_XMLTXT = "index_bolt_xmltxt";
	public static final String INDEX_BOLT_FIX = "index_bolt_fix";	
	public static final String REPUB_BOLT= "repub_bolt_xmltxt";
	public static final String KAFKA_REPUB_BOLT= "kafka_repub_bolt_xmltxt";
	public static final String STREAMING_TO_HDFS_BOLT = "streaming_to_hdfs_bolt";
	private static final int MAX_PARALLEL_PATHS = 6;
	
	private static Logger logger = Logger.getLogger(TransactionProcessorTopology.class);

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		
		String topName = args.length > 0 ? args[0] : "Topology";
		int numWorkers = args.length > 1 ? Integer.parseInt(args[1]) : 4;
		String stormMode = args.length > 2 ? args[2] : "local";
		//Control storm executors
		int executors = args.length > 3 ? Integer.parseInt(args[3]) : 20;
		//Control storm executors ratios for each hop
		int executors_spoutRatio = args.length > 4 ? Integer.parseInt(args[4]) : 2;
		int executors_normalizationRatio = args.length > 5 ? Integer.parseInt(args[5]) : 4;
		int executors_outjmsRatio = args.length > 6 ? Integer.parseInt(args[6]) : 4;
		int executors_outHDFSRatio = args.length > 7 ? Integer.parseInt(args[7]) : 4;
		int executors_outIndexRatio = args.length > 8 ? Integer.parseInt(args[8]) : 4;
		//We can have up to MAX_PARALLEL_PATHS parallel paths from spout to normalization to all the various sinks
		int parallelPaths = args.length > 9 ? Integer.parseInt(args[9]) : MAX_PARALLEL_PATHS;
		if (parallelPaths < 0 || parallelPaths > MAX_PARALLEL_PATHS) {
			parallelPaths = MAX_PARALLEL_PATHS;			
		}
		//For the downstream systems, we can choose to turn off normalization onwards, or turn off each
		//of the sinks for easier tuning
		boolean truncateFromNormalize = args.length > 10 ? Boolean.parseBoolean(args[10]) : false;
		logger.debug("arg11:" + args[10]);
		logger.debug("truncateFromNormalize:" + truncateFromNormalize);
		boolean truncateRepub = args.length > 11 ? Boolean.parseBoolean(args[11]) : false;
		logger.debug("arg12:" + args[11]);
		logger.debug("truncateRepub:" + truncateRepub);
		boolean truncateHDFS = args.length > 12 ? Boolean.parseBoolean(args[12]) : false;
		logger.debug("arg13:" + args[12]);
		logger.debug("truncateHDFS:" + truncateHDFS);
		boolean truncateIndex = args.length > 13 ? Boolean.parseBoolean(args[13]) : false;
		logger.debug("arg14:" + args[13]);
		logger.debug("truncateIndex:" + truncateIndex);

		int jms_prefetch = args.length > 14 ? Integer.parseInt(args[14]) : 15000;
		int jms_maxthreadpoolsize = args.length > 15 ? Integer.parseInt(args[15]) : 1000;
		boolean jms_sessionasynch = args.length > 16 ? Boolean.parseBoolean(args[16]) : false;
		boolean jms_optimizeack = args.length > 17 ? Boolean.parseBoolean(args[17]) : true;

		boolean archiveRaw = args.length > 18 ? Boolean.parseBoolean(args[18]) : false;
		logger.debug("arg19:" + args[18]);
		logger.debug("archiveRaw:" + archiveRaw);
		//System.err.println("archiveRaw " + archiveRaw);

		long JMSSleepTime = args.length > 19 ? Long.parseLong(args[19]) : 20;
		logger.debug("arg20:" + args[19]);
		logger.debug("JMSSleepTime:" + JMSSleepTime);
		//System.err.println("JMSSleepTime " + JMSSleepTime);
		
		boolean jms_usecompression = args.length > 20 ? Boolean.parseBoolean(args[20]) : true;
		logger.debug("arg21:" + args[20]);
		logger.debug("jms_usecompression:" + jms_usecompression);
		//System.err.println("KAFKA " + kafka);
		
		boolean kafka = args.length > 21 ? Boolean.parseBoolean(args[21]) : false;
		logger.debug("arg22:" + args[21]);
		logger.debug("kafka:" + kafka);
		//System.err.println("KAFKA " + kafka);

		boolean inlineRaw = args.length > 22 ? Boolean.parseBoolean(args[22]) : false;
		logger.debug("arg23:" + args[22]);
		logger.debug("inlineRaw:" + inlineRaw);
		//System.err.println("inlineRaw " + inlineRaw);
		
		TopologyBuilder builder = new TopologyBuilder();

		//Spout origination of data. Kafka or JMS for now only, we are not
		//tuned to handle a double flood yet
		if (kafka) {
			logger.debug("Using Kafka for Spout");
			setupKSpouts(builder, parallelPaths, executors, executors_spoutRatio);
		} else {
			logger.debug("Using JMS for Spout");
			setupSpouts(builder, parallelPaths, executors, executors_spoutRatio, jms_sessionasynch, jms_optimizeack, jms_prefetch, jms_maxthreadpoolsize, JMSSleepTime, jms_usecompression);			
		}
		if (!truncateFromNormalize) {
			// Spout from JMS Feeds Normalization, which emits two streams
			logger.debug("Using Normalization");
			setupNormalization(builder, parallelPaths, executors, executors_normalizationRatio, kafka);
			// Normalization feeds two streams, each into index, repub, Hadoop bolts
			if (!truncateRepub) {
				if (kafka) {
					logger.debug("Using Kafka for Repub");
					setupKafkaRebub(builder, parallelPaths, executors, executors_outjmsRatio, jms_sessionasynch, jms_optimizeack, jms_prefetch, jms_maxthreadpoolsize);
				} else {
					logger.debug("Using JMS for Repub");
					setupRebub(builder, parallelPaths, executors, executors_outjmsRatio, jms_sessionasynch, jms_optimizeack, jms_prefetch, jms_maxthreadpoolsize, jms_usecompression);
				}
			}
			if (!truncateHDFS) {
				logger.debug("Using HDFS Spout");
				setupHDFS(builder, parallelPaths, executors, executors_outHDFSRatio, archiveRaw, inlineRaw);
			}
			if (!truncateIndex) {
				logger.debug("Using Index Spout");
				setupIndex(builder, parallelPaths, executors, executors_outIndexRatio);
			}
		}
						
		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(numWorkers);
		//https://github.com/nathanmarz/storm/wiki/Guaranteeing-message-processing#tuning-reliability
		conf.setNumAckers(0);
		//conf.setMaxSpoutPending(1000);
		//conf.setNumAckers(10);
		//conf.setMessageTimeoutSecs(30);
		
		 //storm.zookeeper.servers
		 conf.put("kafka.spout.topic", ConfigurationClient.getInstance().getProperty("kf.notificationTopicIn"));
		 conf.put("kafka.spout.consumer.group", "test-consumer-group");
		 conf.put("kafka.zookeeper.connect", ConfigurationClient.getInstance().getProperty("kf.hoststring"));
		 conf.put("kafka.consumer.timeout.ms", 100);
		
		logger.debug("deployTo: " + stormMode);
		if (stormMode != null && stormMode.trim().equalsIgnoreCase("cluster")) {
			logger.debug("Running Cluster Mode");			
			StormSubmitter.submitTopology(topName, conf,
					builder.createTopology());			
		} else {
			logger.debug("Running Local Mode");			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Local-WFB", conf,
					builder.createTopology());
		}
	}

	private static void setupKSpouts(TopologyBuilder builder,
			int parallelPaths, int executors, int executors_spoutRatio) {
		for(int i = 0 ; i < parallelPaths; i++) {
			builder.setSpout(KAFKA_QUEUE_SPOUT + (i+1), new KafkaSpout(), executors*executors_spoutRatio);
			
			/*List<String> hosts = new Vector<String>();
			hosts.add(ConfigurationClient.getInstance().getProperty("kf.host"+ (i+1)));
			System.err.println(hosts);
			SpoutConfig spoutConfig = new SpoutConfig(
					  KafkaConfig.StaticHosts.fromHostString(hosts, 0), // list of Kafka brokers
					  ConfigurationClient.getInstance().getProperty("kf.notificationTopicIn"), // topic to read from
					  "/tmp/zookeeper", // the root path in Zookeeper for the spout to store the consumer offsets
					  "storm" + (i+1)); // an id for this consumer for storing the consumer offsets in Zookeeper
			w kafkaSpout = new KafkaSpout(spoutConfig);
			builder.setSpout(KAFKA_QUEUE_SPOUT + (i+1), kafkaSpout, executors*executors_spoutRatio);*/
		}
	}

	private static void setupSpouts(TopologyBuilder builder, int parallelPaths, int executors, int executors_spoutRatio,
			boolean jms_sessionasynch, boolean jms_optimizeack, int jms_prefetch, int jms_maxthreadpoolsize, long JMSSleepTime, boolean jms_usecompression) {
		for(int i = 0 ; i < parallelPaths; i++) {
			WFBJMSpout queueSpout = new WFBJMSpout();
			queueSpout.setOptions(jms_sessionasynch, jms_optimizeack, jms_prefetch, jms_maxthreadpoolsize, jms_usecompression);
			queueSpout.setJmsProvider(new SpringJmsProvider("jms-activemq" + (i+1) + ".xml", 
					"jmsConnectionFactory", "notificationQueue"));
			queueSpout.setJmsTupleProducer(new WFBTupleProducer());
			queueSpout.setJmsAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
			queueSpout.setDistributed(true); // allow multiple instances
			queueSpout.setRecoveryPeriod(60000);
			queueSpout.setJMSSleepTime(JMSSleepTime);
			builder.setSpout(JMS_QUEUE_SPOUT + (i+1), queueSpout, executors*executors_spoutRatio);
		}
	}

	//This is the only place we need to adjust Spouts based on kafka and jms. 
	//Normalization bolts then feed out to the rest of the topology
	private static void setupNormalization(TopologyBuilder builder, int parallelPaths, int executors, int executors_normalizationRatio, boolean kafka) {
		for(int i = 0 ; i < parallelPaths; i++) {
			if(kafka) {
				logger.debug("Set up JMS for normalization bolt");
				builder.setBolt(NORMALIZATION_BOLT + (i+1), new NormalizationBolt(), executors*executors_normalizationRatio)
						.shuffleGrouping(KAFKA_QUEUE_SPOUT + (i+1));
			} else {
				logger.debug("Set up JMS for normalization bolt");
				builder.setBolt(NORMALIZATION_BOLT + (i+1), new NormalizationBolt(), executors*executors_normalizationRatio)
				.shuffleGrouping(JMS_QUEUE_SPOUT + (i+1), WFBTupleProducer.IN_JMS_STREAM); //Identify JMS Stream So we can fork logic. We have access to Spout code.			
			}
		}
	}

	private static void setupKafkaRebub(TopologyBuilder builder, int parallelPaths, int executors, int executors_outjmsRatio,
				boolean jms_sessionasynch, boolean jms_optimizeack, int jms_prefetch, int jms_maxthreadpoolsize) {
		for(int i = 0 ; i < parallelPaths; i++) {
			KafkaBolt kafkaBolt = new KafkaBolt();
			builder.setBolt(KAFKA_REPUB_BOLT + (i+1), kafkaBolt, executors*executors_outjmsRatio)
					.shuffleGrouping(NORMALIZATION_BOLT + (i+1), NormalizationBolt.XML_TXT_STREAM)
					.shuffleGrouping(NORMALIZATION_BOLT + (i+1), NormalizationBolt.FIX_STREAM);			
			//We used to use two repubs per normalize, but that doesn't make sense since you end up with duplicate
			//message delivery. Keep around all artifacts for up to 6 repubs, use later if it makes sense
		}
		
	}

	private static void setupRebub(TopologyBuilder builder, int parallelPaths, int executors, int executors_outjmsRatio,
			boolean jms_sessionasynch, boolean jms_optimizeack, int jms_prefetch, int jms_maxthreadpoolsize, boolean jms_usecompression) {
		for(int i = 0 ; i < parallelPaths; i++) {
			NonPersistJMSBolt jmsOutBolt_xmltxt = new NonPersistJMSBolt();
			jmsOutBolt_xmltxt.setOptions(jms_sessionasynch, jms_optimizeack, jms_prefetch, jms_maxthreadpoolsize, jms_usecompression);
			jmsOutBolt_xmltxt.setJmsProvider(new SpringJmsProvider("jms-activemqout" + (i+1) + ".xml", "jmsConnectionFactory", "notificationQueue"));
			jmsOutBolt_xmltxt.setJmsAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
	        // anonymous message producer just calls toString() on the tuple to create a jms message
			jmsOutBolt_xmltxt.setJmsMessageProducer(new JMSRepubMessageProducer());
			builder.setBolt(REPUB_BOLT + (i+1), jmsOutBolt_xmltxt, executors*executors_outjmsRatio)
					.shuffleGrouping(NORMALIZATION_BOLT + (i+1), NormalizationBolt.XML_TXT_STREAM)
					.shuffleGrouping(NORMALIZATION_BOLT + (i+1), NormalizationBolt.FIX_STREAM);			
			//We used to use two repubs per normalize, but that doesn't make sense since you end up with duplicate
			//message delivery. Keep around all artifacts for up to 6 repubs, use later if it makes sense
		}
	}

	private static void setupHDFS(TopologyBuilder builder, int parallelPaths, int executors, int executors_outHDFSRatio, boolean archiveRaw, boolean inlineRaw) {
		for(int i = 0 ; i < parallelPaths; i++) {
			StreamingToHDFSBolt hdfsStreamingBold = new StreamingToHDFSBolt(archiveRaw, inlineRaw);
			builder.setBolt(STREAMING_TO_HDFS_BOLT + (i+1), hdfsStreamingBold, executors*executors_outHDFSRatio)
					.shuffleGrouping(NORMALIZATION_BOLT + (i+1), NormalizationBolt.XML_TXT_STREAM)
					.shuffleGrouping(NORMALIZATION_BOLT + (i+1), NormalizationBolt.FIX_STREAM);			
		}
	}

	private static void setupIndex(TopologyBuilder builder, int parallelPaths, int executors, int executors_outIndexRatio) {
		for(int i = 0 ; i < parallelPaths; i++) {
			IndexBolt idxBolt = new IndexBolt();
			builder.setBolt(INDEX_BOLT_XMLTXT + (i+1), idxBolt, executors*executors_outIndexRatio)
					.shuffleGrouping(NORMALIZATION_BOLT + (i+1), NormalizationBolt.XML_TXT_STREAM)
					.shuffleGrouping(NORMALIZATION_BOLT + (i+1), NormalizationBolt.FIX_STREAM);			
		}
	}


}