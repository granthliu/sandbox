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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.hortonworks.wfbs.utils.ConfigurationClient;


/**
 * Stream messages to HDFS file system
 * Ability to create new file for every numberofRecordsPerFile.
 * Also archives Raw, XML as single files based in ID,
 * then writes the rest of the fields out into external hive location.
 * 
 * Careful when using - set parallelism to 1 for this or threadiness can confuse
 * the streaming bytes (you'll see ID mismatch for HDFS file write errors)
 * 
 * @author ssabbella
 * 
 */
public class StreamingToHDFSBolt implements IRichBolt {
	private static final long serialVersionUID = 6711927509800236001L;
	private static final String delim = ",";
	private static Logger logger = Logger.getLogger(StreamingToHDFSBolt.class);
	private static final String newline = System.getProperty("line.separator");

	private long monoCounter = 0L;
	private long msgCount = 0L;
	private OutputCollector collector;
	private String uri = "hdfs://localhost:8020";
	private String user = "storm";
	private String basepath = "/input/";
	int numRecordsPerFileXmlTxt = 2000;
	int numRecordsPerFileFix = 2000;
	
	private FSDataOutputStream accumulatingFileouputStream_xmltxt = null;
	private FSDataOutputStream accumulatingFileouputStream_fix = null;

	private boolean archiveRaw;
	private boolean inlineRaw;
	
	public StreamingToHDFSBolt(boolean archiveRaw, boolean inlineRaw) {
		super();
		this.archiveRaw = archiveRaw;
		this.inlineRaw = inlineRaw;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		ConfigurationClient confClient = ConfigurationClient.getInstance();	
		
		uri = confClient.getProperty("hadoop.hdfs.namenode.uri");
		user = confClient.getProperty("hadoop.hdfs.streaming.user");
		String numRecords = confClient.getProperty("storm.hdfs.streaming.numberofrecordsperfile");
		basepath = confClient.getProperty("hadoop.hdfs.basepath");
		try {
			numRecordsPerFileXmlTxt = Integer.parseInt(numRecords);
			numRecordsPerFileFix = Integer.parseInt(numRecords);
		} catch(NumberFormatException e) {
			logger.error("Could not parse number records per file:" + numRecords);
			numRecordsPerFileXmlTxt = 2000;
			numRecordsPerFileFix = 2000;
		}
		logger.debug("numRecordsPerFileXmlTxt:" + numRecordsPerFileXmlTxt);
		logger.debug("numRecordsPerFileFix:" + numRecordsPerFileFix);
	}

	@Override
	public void execute(Tuple input) {
		String id = input.getStringByField(NormalizationBolt.ID_FN);
		logger.debug("Executing:" + id);
		logger.debug("Size of input tuple:" + input.size());
		String rawpaylod = input.getStringByField(NormalizationBolt.RAW_FN);
		FSDataOutputStream fs = null;
		if (input.getSourceStreamId().equals(NormalizationBolt.FIX_STREAM)) {
			logger.debug("Processing FIX");
			if(archiveRaw) {
				try {
					fs =  getFOS(input, basepath + NormalizationBolt.FIX_STREAM + "/" + id);
					logger.debug("Writing RAW archive");
					writeFile(rawpaylod, fs);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					try {
						fs.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			logger.debug("Writing record for HIVE");
			//Accumulate Hive record
			writeFileAccumulatingFix(input.getStringByField(NormalizationBolt.ID_FN) + delim +
					input.getStringByField(NormalizationBolt.TAG35_FN) + delim + 
					input.getStringByField(NormalizationBolt.TAG49_FN) + delim + 
					input.getStringByField(NormalizationBolt.TAG52_FN) + delim + 
					input.getStringByField(NormalizationBolt.TAG60_FN) + delim + 
					input.getStringByField(NormalizationBolt.TAG75_FN) + delim + 
					input.getStringByField(NormalizationBolt.TAG600_FN) + delim + 
					input.getStringByField(NormalizationBolt.TAG609_FN) + (inlineRaw ? delim + prepRaw(rawpaylod) : ""), input);
		} else {
			logger.debug("Processing XML OR RAW");
			if(archiveRaw) {
				try {
					fs = getFOS(input, basepath + NormalizationBolt.XML_TXT_STREAM + "/" + id);
					logger.debug("Writing XML archive");
					String xmlpayload = input.getStringByField(NormalizationBolt.XML_FN);
					if(xmlpayload != null && xmlpayload.length() > 0) {
						writeFile(xmlpayload, fs);
					}
					logger.debug("Writing RAW archive");
					writeFile(rawpaylod, fs);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					try {
						fs.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			//Accumulate Hive record
			logger.debug("Writing record for HIVE");
			writeFileAccumulatingXmlTxt(input.getStringByField(NormalizationBolt.ID_FN) + delim + 
					input.getStringByField(NormalizationBolt.SOURCEID_FN) + delim + 
					input.getStringByField(NormalizationBolt.CONTENTTYPE_FN) + delim + 
					input.getStringByField(NormalizationBolt.TRADEID_FN) + (inlineRaw ? delim + prepRaw(rawpaylod) : ""), input);
		}		
		collector.ack(input);
	}
	
	//Prep raw for inlining - remove delimiter and newlines
	private static final Pattern p = Pattern.compile("(" + delim + "|\r\n|\n\r|\r|\n)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
	private static String prepRaw(String rawpaylod) {
		return p.matcher(rawpaylod).replaceAll("");
	}

	//Accumulate strings into files
	private void writeFileAccumulatingXmlTxt(String str, Tuple input) {
		if (this.accumulatingFileouputStream_xmltxt == null) {
			logger.debug("Initializing file input stream for xml txt");
			initializeFileOutputStreamXmlTxt(input);
		}
		try {
			logger.debug("About to accumulate more, current count is " + msgCount);
			writeFile(str + newline, this.accumulatingFileouputStream_xmltxt);
			if (++msgCount % numRecordsPerFileXmlTxt == 0) {
				initializeFileOutputStreamXmlTxt(input);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private void writeFileAccumulatingFix(String str, Tuple input) {
		if (this.accumulatingFileouputStream_fix == null) {
			logger.debug("Initializing file input stream for xml txt");
			initializeFileOutputStreamFix(input);
		}
		try {
			logger.debug("About to accumulate more, current count is " + msgCount);
			writeFile(str + newline, this.accumulatingFileouputStream_fix);
			if (++msgCount % numRecordsPerFileFix == 0) {
				initializeFileOutputStreamFix(input);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void initializeFileOutputStreamXmlTxt(Tuple input) {
		try {
			if (this.accumulatingFileouputStream_xmltxt != null) {
				logger.debug("Closing accumulating stream, processed this many records" + msgCount);				
				logger.debug("CORRECT EXPECTED NUMBER OF MESSAGES PROCESSED = " + (msgCount == numRecordsPerFileXmlTxt));				
				this.accumulatingFileouputStream_xmltxt.close();
			}
			//Uhm....just name it after ID of the first record
			this.accumulatingFileouputStream_xmltxt =
					getFOS(input, basepath + "HIVE_XMLTXT/" + input.getStringByField(NormalizationBolt.ID_FN));
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void initializeFileOutputStreamFix(Tuple input) {
		try {
			if (this.accumulatingFileouputStream_fix != null) {
				logger.debug("Closing accumulating stream, processed this many records" + msgCount);				
				logger.debug("CORRECT EXPECTED NUMBER OF MESSAGES PROCESSED = " + (msgCount == numRecordsPerFileFix));				
				this.accumulatingFileouputStream_fix.close();
			}
			//Uhm....just name it after ID of the first record
			this.accumulatingFileouputStream_fix =
					getFOS(input, basepath + "HIVE_FIX/" + input.getStringByField(NormalizationBolt.ID_FN));

		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void writeFile(String str, FSDataOutputStream fileOut) throws IOException {
		//logger.debug("Write file:" + str.substring(0, str.length() < 50 ? (str.length() == 0 ? 0 : str.length() - 1) : 50));
		// convert String into InputStream
		BufferedInputStream bis = null;
		InputStream is = null;
		try {
			is = new ByteArrayInputStream(str.getBytes());
			bis = new BufferedInputStream(is);
			logger.debug("This was hanging when string was size 0. Size of string:" + str.length());
			IOUtils.copyBytes(bis, fileOut, str.getBytes().length,false);
		} catch (IOException e1) {
			throw e1;
		} finally {
			if (bis != null)
				try {
					bis.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			if (is != null)
				try {
					is.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}

	private FSDataOutputStream getFOS(Tuple input, String pathStr) {			
		FSDataOutputStream FOS = null;
		try {
			FileSystem fs = FileSystem.get(new URI(uri), new Configuration());
			Path outFilePath = new Path(pathStr);
			if (fs.exists(outFilePath)) {
				logger.error("Given path " + outFilePath + " already exists in HDFS.");
				outFilePath = new Path(pathStr + "(" + String.valueOf(monoCounter++) + ")");
			}
			FOS = fs.create(outFilePath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return FOS;
	}
	
	@Override
	public void cleanup() {
		logger.debug("CLEANUP" + msgCount);				
		//No guarantees this will be called. Concerned about open FSDataOutputStream if we don't hit
		//the expected number of tuples and the incoming stream stops. Guess that doesn't matter in
		//production, continuous deployment?
		if (this.accumulatingFileouputStream_xmltxt != null) {
			try {
				this.accumulatingFileouputStream_xmltxt.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (this.accumulatingFileouputStream_fix != null) {
			try {
				this.accumulatingFileouputStream_fix.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields("response"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
    //Some minor exercise
	public static void main(String[] args) {
		String input = "{SystemSourceId}=CIRCqaproject1\n" +
				"{TradeId}=${tradeId}\n" +
				"{TradeVersion}=1\n" +
				"{MessageId}=155098\n" +
				"{MessageEventType}=PENDING_TRADE\n" +
				"{PrimaryAssetClass}=FX\n" +
				"{WF_Link_ChildID}=\n" +
				"{WF_TransferTo}=\n" +
				"{WF_TransferFrom}=\n" +
				"{CC_ClearingHouse}=\n" +
				"{CC_ClearingTradeId}=\n" +
				"{CC_ClientClearing}=\n" +
				"{CC_EligibleForClearing}=\n" +
				"{CC_SendForClearing}=\n" +
				"{CC_SendForClearingTimestamp}=\n" +
				"{CC_OriginalCalypsoLE}=\n" +
				"{CC_ClearedDateTime}=\n" +
				"{TLCRootTradeId}=\n" +
				"{NovationTradeId}=\n" +
				"{ECN_Source}=\n" +
				"{CC_FCM}=\n" +
				"{USI_CURRENT}=103039933701W00000000000000000000011000938\n" +
				"{REPORTING_PARTY}=Us\n" +
				"{EXECUTION_DATETIME}=${effectiveDate}\n" +
				"{LEI_US}=CICI:SX0CI4F7GVW5530ZMN03\n" +
				"{LEI_CP}=CICI:KB1H1DSPRFMYMCUFXT09\n" +
				"{SDR_REPORTABLE}=true\n" +
				"{SDR_ELIGIBLE}=true\n" +
				"{LEI_US_USPERSON}=false\n" +
				"{LEI_US_ROLE}=non-SD/MSP\n" +
				"{LEI_US_FINANCIALENTITY}=false\n" +
				"{LEI_US_LEGALTYPE}=FAFFIL\n" +
				"{LEI_US_END_USER_EXEMPTION}=\n" +
				"{LEI_CP_USPERSON}=false\n" +
				"{LEI_CP_ROLE}=non-SD/MSP\n" +
				"{LEI_CP_FINANCIALENTITY}=false\n" +
				"{LEI_CP_LEGALTYPE}=AFFIL\n" +
				"{LEI_CP_END_USER_EXEMPTION}=\n" +
				"{CalypsoProductType}=FXSwap\n" +
				"{StructuredProductType}=NA\n" +
				"{CalypsoProductSubType}=Standard\n" +
				"{MarketType}=New Deal\n" +
				"{Book}=PI_WFBI_Hedge\n" +
				"{ProcessingOrg}=PI_WFBI\n" +
				"{BusinessAccountId}=965987\n" +
				"{DocAction}=Confirmation\n" +
				"{CONFIRM_REQUIRED}=true\n" +
				"{ECN_Source}=\n" +
				"{ECNTrade}=false\n" +
				"{ECN_State}=\n" +
				"{TradeComment}=\n" +
				"{TransactionType}=Interest Rate Swap\n" +
				"{WeBuySell}=\n" +
				"{TradeQuantity}=1.0\n" +
				"{TradeDate}=${effectiveDate}\n" +
				"{TradeSettleDate}=${terminationDate}\n" +
				"{CounterpartyName}=LDN_TSY OPICS WEST\n" +
				"{CounterpartyShortName}=LDN_TSY_WEST\n" +
				"{Marketer}=NONE\n" +
				"{Trader}=NONE\n" +
				"{CalypsoDocStatus}=PENDING\n" +
				"{MarketerId}=NOT FOUND\n" +
				"{TraderId}=NOT FOUND\n" +
				"{InternalReference}=11000938\n" +
				"{ExternalReference}=\n" +
				"{MasterType}=\n" +
				"{MasterStatus}=\n" +
				"{LegalAgreementDate}=\n" +
				"{LegalAgreementType}=\n" +
				"{ExchangeCustomerFlag}=false\n" +
				"{DTCCOkTradeFlag}=false\n" +
				"{DTCCEligible}=false\n" +
				"{DTCCParticipantFlag}=false\n" +
				"{DTCCOurParticipantId}=NONE\n" +
				"{DTCCCtyParticipantId}=NONE\n" +
				"{LEBusinessType}=NONBANK\n" +
				"{TradeTicketRequired}=NONE\n" +
				"{InternalConfirmRequired}=NONE\n" +
				"{MuniClientFlag}=false\n" +
				"{CptyContactName}=Sir or Madam\n" +
				"{CptyAttention}=Confirmations\n" +
				"{CptyStreet}=301 SOUTH COLLEGE STREET\n" +
				"{CptyCity}=CHARLOTTE\n" +
				"{CptyState}=NC\n" +
				"{CptyZip}=282020000\n" +
				"{CptyCountry}=UNITED STATES\n" +
				"{CptyFax}=<FONT COLOR=\"RED\">(xxx) xxx-xxxx</FONT>\n" +
				"{CptyPhone}=<FONT COLOR=\"RED\">(xxx) xxx-xxxx</FONT>\n" +
				"{CptyEmail}=\n" +
				"{OurContactName}=\n" +
				"{OurStreetName}=Wells Fargo Bank International\n" +
				"{OurStreet}=\n" +
				"{OurCity}=\n" +
				"{OurState}=\n" +
				"{OurZip}=\n" +
				"{OurCountry}=UNITED STATES\n" +
				"{OurFax}=\n" +
				"{OurPhone}=\n" +
				"{LegalAgreementClause}=\n" +
				"{Guarantor}=\n" +
				"{CalculationAgent}=Wells Fargo Bank International\n" +
				"{OurSettlementInstructions[1]}=SDI not available.\n" +
				"{CounterpartySettlementInstructions[1]}=Please provide written payment instructions.\n" +
				"{CounterpartySettlementInstructions[2]}=Wells Fargo Bank International will make no payments until\n" +
				"{CounterpartySettlementInstructions[3]}=written payment instructions are received.\n" +
				"{CounterpartySettlementInstructions[4]}=Phone: 1-800-249-3865 Fax: 1-704-383-8429\n" +
				"{CollateralLinkedTrades}=\n" +
				"{InitialMargin}=\n" +
				"{ManualReviewFlag}=false\n" +
				"{BookingDate}=07/30/2013\n" +
				"{KW.PreciousMetal-allocation}=unallocated\n" +
				"{KW.PreciousMetalPrecision}=PreciousMetalLocationSpreadDecimals=2,SpreadAdjustedSpotRateDecimals=6\n" +
				"{KW.FAR_CLS}=false\n" +
				"{KW.RatesPrecision}=GBP/USD=6\n" +
				"{KW.CustomSplitSetting}=NO\n" +
				"{KW.FarLegPrecision}=FarLegForwardPointDecimals=2,FarLegFinalRateDecimals=6,FarLegMarginPointDecimals=2,FarLegTraderRateDecimals=6\n" +
				"{KW.LEI_CP}=CICI:KB1H1DSPRFMYMCUFXT09\n" +
				"{KW.NearLegPrecision}=TraderRateDecimals=6,MarginPointDecimals=2,FinalRateDecimals=6,ForwardPointDecimals=2\n" +
				"{KW.USI_CURRENT}=103039933701W00000000000000000000011000938\n" +
				"{KW.LEI_US}=CICI:SX0CI4F7GVW5530ZMN03\n" +
				"{KW.CustomTransferSetting}=NO\n" +
				"{KW.SDR_ELIGIBLE}=true\n" +
				"{KW.CustomForwardRiskTransferSetting}=NO\n" +
				"{KW.EXECUTION_DATETIME}=2013-07-30T14:38:05-04:00\n" +
				"{KW.CustomB2BSetting}=NO\n" +
				"{KW.NEAR_CLS}=false\n" +
				"{KW.SDR_REPORTABLE}=true\n" +
				"{KW.SDR_MarketType}=New Deal\n" +
				"{KW.REPORTING_PARTY}=Us\n" +
				"{KW.DTCC_OK}=No\n" +
				"{KW.NegotiatedCurrency}=GBP\n" +
				"{Parent.OriginalNotional}=0.00\n" +
				"{FXPrimaryCurrency}=GBP\n" +
				"{FXQuotingCurrency}=USD\n" +
				"{FXSettleDate[1]}=08/01/2013\n" +
				"{FXSettleDate[2]}=09/03/2013\n" +
				"{FXPrimaryAmount[1]}=5000000.00\n" +
				"{FXPrimaryAmount[2]}=-5000000.00\n" +
				"{FXQuotingAmount[1]}=-7552000.00\n" +
				"{FXQuotingAmount[2]}=7552000.00\n" +
				"{FXRate[1]}=1.51040\n" +
				"{FXRate[2]}=1.51040\n" +
				"{FXPrimaryFwdRate[1]}=0.00000\n" +
				"{FXPrimaryFwdRate[2]}=0.00000\n" +
				"{FXQuotingFwdRate[1]}=0.00000\n" +
				"{FXQuotingFwdRate[2]}=0.00000\n";
		System.out.println(prepRaw(input));
	}

}