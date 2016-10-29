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
package com.hortonworks.wfbs.bolts;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.net.util.Base64;
import org.apache.log4j.Logger;

import quickfix.FieldNotFound;
import quickfix.Message;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.hortonworks.wfbs.spouts.WFBTupleProducer;
import com.hortonworks.wfbs.utils.FixStringToMessage;
import com.hortonworks.wfbs.utils.StvtoXML;


/**
 * Normalize incoming text, FIX to XML, extract search fields and store
 * raw data, emit as two streams, one for text/XML, one for FIX
 * 
 * @author gliu
 * 
 */
public class NormalizationBolt implements IRichBolt {
	private static final long serialVersionUID = 6711927509800236001L;
	OutputCollector collector;
	private static Logger logger = Logger.getLogger(NormalizationBolt.class);

	//stream
	public static final String XML_TXT_STREAM = "XML_TXT";
	//and its fields
	public static final String ID_FN = "id";
	public static final String TRADEID_FN = "tradeId";
	public static final String SOURCEID_FN = "sourceId";
	public static final String CONTENTTYPE_FN = "contentType";
	public static final String XML_FN = "xml_message";
	public static final String RAW_FN = "raw_message";
	public static final String EFFECTIVE_DATE_FN = "effective_date";
	public static final String TERMINATION_DATE_FN = "termination_date";

	//stream
	public static final String FIX_STREAM = "FIX";
	//and its fields
	public static final String TAG35_FN = "tag35";
	public static final String TAG49_FN = "tag49";
	public static final String TAG52_FN = "tag52";
	public static final String TAG60_FN = "tag60";
	public static final String TAG75_FN = "tag75";
	public static final String TAG600_FN = "tag600";
	public static final String TAG609_FN = "tag609";

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		//System.err.println(input.getValue(0).getClass());
		//System.err.println(new String((byte[]) input.getValue(0)));

		String payload = "payload";
		String guid = "guid";
		String filename = "filename";
		String type = "type";
		String effectiveDate = "effectiveDate";
		String terminationDate = "tyterminationDatepe";

		if (input.getSourceStreamId().equals(WFBTupleProducer.IN_JMS_STREAM)) {
			type = input.getStringByField(WFBTupleProducer.TYPE);
			payload = getPayload(input, type);
			guid = input.getStringByField(WFBTupleProducer.GUID);
			filename = input.getStringByField(WFBTupleProducer.FILENAME);
			//if(filename.contains("7774")) System.err.println(payload);
			effectiveDate = input.getStringByField(WFBTupleProducer.EFFECTIVE_DATE_FN);
			terminationDate = input.getStringByField(WFBTupleProducer.TERMINATION_DATE_FN);
		} else {
			//Kafka will come in as delimited guid, filename, type, raw
			String[] split = new String((byte[]) input.getValue(0)).split("::::::");
			if(split.length == 4) {
				guid = split[0];
				filename = split[1];
				type = split[2];			
				payload = split[3];
			} else {
				logger.debug("Bad Kafka! Expected length 4, got length: " + split.length);
			}
		}

		logger.debug("Input Tuple Size:" + input.size());
		logger.debug("Input guid:" + guid);
		logger.debug("Input filename:" + filename);
		logger.debug("Input type:" + type);
		//logger.debug("Input Tuple:" + payload);
		Values response = new Values(payload);
		if (type.trim().endsWith("FIX")) {
			response = fixToValues(payload, guid);
			collector.emit(FIX_STREAM, input, response);
		} else if (type.trim().endsWith("TXT")) {
			response = txtToValues(payload, filename, guid, effectiveDate, terminationDate);
			collector.emit(XML_TXT_STREAM, input, response);
		} else {
			response = xmlToValues(payload, filename, guid, effectiveDate, terminationDate);			
			collector.emit(XML_TXT_STREAM, input, response);
		}
		collector.ack(input);
	}

	//Brittle I know. But I'm compressing/encoding based on message size. Unwrap here
	private String getPayload(Tuple payload, String type) {
		if(WFBTupleProducer.compressed(type)) {
			//byte[] ba = Base64.decodeBase64(payload);
			GZIPInputStream gzip = null;
			try {
				ByteArrayInputStream in = new ByteArrayInputStream(payload.getBinaryByField(WFBTupleProducer.RAW));
				gzip = new GZIPInputStream(in);
				StringWriter writer = new StringWriter();
				IOUtils.copy(gzip, writer, "UTF-8");
				return writer.toString();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try {
					gzip.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return payload.getStringByField(WFBTupleProducer.RAW);
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(XML_TXT_STREAM, 
				new Fields(ID_FN, TRADEID_FN, SOURCEID_FN, CONTENTTYPE_FN, XML_FN, RAW_FN, EFFECTIVE_DATE_FN, TERMINATION_DATE_FN));
		declarer.declareStream(FIX_STREAM, 
				new Fields(ID_FN, TAG35_FN, TAG49_FN, TAG52_FN, TAG60_FN, TAG75_FN, TAG600_FN, TAG609_FN, XML_FN, RAW_FN));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
		
	
	//OUTPUT TUPLE FOR STAGE2 FOR FIX INPUT:
	//(id: string, tag35: string, tag49: string, tag52: string, tag60: string, tag75: string, tag600: string, tag609: string, raw_message: string)
	private  Values fixToValues(String raw, String guid) {
		Message message =  FixStringToMessage.convert(raw);
		return new Values(
				guid, 
				handleFix(message, 35),
				handleFix(message, 49),
				handleFix(message, 52),
				handleFix(message, 60),
				handleFix(message, 75),
				handleFix(message, 600),
				handleFix(message, 609),
				message.toXML(), 
				raw);
	}
	private String handleFix(Message message, int tag) {
		String retVal = "";
		try {
			retVal= message.getHeader().getString(tag);
		} catch (FieldNotFound e) {
			logger.debug("FIX tag " + tag + " not found in header, looking in body");
			try {
				retVal = message.getString(tag);
			} catch (FieldNotFound e1) {
				logger.debug("FIX tag " + tag + " not found in header or body, setting to empty");
			}
		}
		return retVal;

	}

	//OUTPUT TUPLE FOR STAGE2 FOR TEXT AND XML INPUT: 
	//(id: string, tradeId: string, sourceId: string, contentType: string, xml_message: string, raw_message: string)
	//id is guid, sourceId is file name, tradeId is as-is parsed from raw, contentType is sourceId plus XML or RAW
	//XML type will ignore the xml field, since that's what's in raw
	private  Values txtToValues(String raw, String fn, String guid, String effectiveDate, String terminationdate) {	
		return txt_xmlToValuesHelper(raw, fn, guid, StvtoXML.getXml(raw), "RAW", effectiveDate, terminationdate);		
	}
	private  Values xmlToValues(String raw, String fn, String guid, String effectiveDate, String terminationdate) {
		return txt_xmlToValuesHelper(raw, fn, guid, "", "XML", effectiveDate, terminationdate);		
	}
	private  Values txt_xmlToValuesHelper(String raw, String fn, String guid, String XML, String ContentType, String effectiveDate, String terminationdate) {
		Values returnVal = new Values(
				guid, 
				guid, //we know tradeID is same as guid
				fn,				
				ContentType, //fn + ContentType - per WFB they wanted this, but we already have the fn, so took liberty of cleaning this up
				XML,
				raw,
				effectiveDate,
				terminationdate);
		return returnVal;		
	}	
	
}