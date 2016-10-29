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
package com.hortonworks.wfbs.utils;

import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Use this to index in ES
 * For the txt and XML files,  index on tradeId, sourceId, fromMessageId, contentType and id
 * Fix format, index on tags 35=, 49=, 52=, 60=, 75=, 600= and 609=
 * 
 * @author gliu
 * 
 */
public class IndexClient {
	private static int count = 0;
	
	private static IndexClient instance = null;
	private static Client client = null;
	private static ObjectMapper mapper = new ObjectMapper();
	private static Logger logger = Logger
			.getLogger(IndexClient.class);
	private static BulkRequestBuilder bulkRequest;
	private int bulksize = 100;

	//Unused, NPE everywhere, too hard to work with right now
	private static BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
		@Override
		public void beforeBulk(long executionId, BulkRequest request) {
	        logger.info("Going to execute new bulk composed of actions " + request.numberOfActions());
		}

		@Override
		public void afterBulk(long executionId, BulkRequest request,
				BulkResponse response) {
	        logger.info("Executed bulk composed of actions" + request.numberOfActions());
		}

		@Override
		public void afterBulk(long executionId, BulkRequest request,
				Throwable failure) {
	        logger.warn("Error executing bulk ", failure);
			
		}
	}).setBulkActions(100).build();



	private IndexClient() {
		//In the context of storm, i'm not sure this singleton stuff is doing what we think it is.
		//Sometimes, we will get null errors here. getProperty("es.host") will return null, which means configuration
		//was null.
		InetSocketTransportAddress insta1 = new InetSocketTransportAddress(ConfigurationClient.getInstance().getProperty("es.host1"), 9300);
		InetSocketTransportAddress insta2 = new InetSocketTransportAddress(ConfigurationClient.getInstance().getProperty("es.host2"), 9300);
		InetSocketTransportAddress insta3 = new InetSocketTransportAddress(ConfigurationClient.getInstance().getProperty("es.host3"), 9300);
		client = new TransportClient().addTransportAddresses(insta1, insta2, insta3);
		
		bulkRequest = client.prepareBulk();
		String bulkStr = ConfigurationClient.getInstance().getProperty("es.bulksize");
		bulksize = bulkStr == null ? 100 : Integer.parseInt(bulkStr);
	}

	public static IndexClient getInstance() {
		if (instance == null)
			instance = new IndexClient();
		return instance;
	}
	

	public synchronized void index(IndexableTransaction indexMe, String index)
			throws JsonProcessingException {		
		//System.err.println(mapper.writeValueAsString(indexMe));
		String json = mapper.writeValueAsString(indexMe);
		logger.debug("Index document: "+ json.substring(0,20));
		//IndexResponse response = client.prepareIndex(index, "search").setSource(json).execute().actionGet();
		//logger.debug("Indexed document with id: " + response.getId());
		
		//No bueno: http://blog.trifork.com/2013/01/10/how-to-write-an-elasticsearch-river-plugin/
		//bulkProcessor.add(new IndexRequest(index), json);
		//bulkProcessor.add(Requests.indexRequest(index).source(json).type("search"));
		
		bulkRequest.add(Requests.indexRequest(index).source(json).type("search"));
		if(++count == bulksize) {			
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			if (bulkResponse.hasFailures()) {
				logger.error("ERROR bulk request: " + bulkResponse.buildFailureMessage());
			} else {
				logger.info("SUCCESS bulk request");				
			}
			count = 0;
		}
	}
	
	public static void main(String[] args) {
		IndexableFIXTransaction trans = 
				new IndexableFIXTransaction("id", "TAG35", "TAG49", "20131126-18:20:09.00956", "TAG60", "TAG75", "TAG600", "TAG609");
		try {
			System.out.println(mapper.writeValueAsString(trans));
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}