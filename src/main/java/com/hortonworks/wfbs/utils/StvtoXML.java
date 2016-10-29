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

import com.sun.xml.txw2.output.IndentingXMLStreamWriter;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a very quick and dirty hack to write something that
 * is able to convert STV to XML
 */

public class StvtoXML {

    public static String getXml(String stv) {

        // This is probably not a keeper :P

        XMLOutputFactory factory = XMLOutputFactory.newInstance();

        StringWriter sw = new StringWriter();

        try {
            XMLStreamWriter writer = new IndentingXMLStreamWriter(factory.createXMLStreamWriter(sw));
            writer.writeStartDocument();
            writer.writeStartElement("message");
            writer.writeStartElement("data");

            // While we build up the document we will build up the arrays and objects to
            // build at the end
            Map<String, List<String>> arrays = new HashMap<String, List<String>>();
            Map<String, Map<String, String>> objects = new HashMap<String, Map<String, String>>();

            for (String line : stv.split(System.getProperty("line.separator"))) {
                String key = line.substring(line.indexOf('{') + 1, line.indexOf('}'));
                String value = null;
                if (line.split("=").length > 1)
                    value = line.split("=")[1];

                // Keys can mean a few things
                if (key.contains("[")) {
                    // We have an array for the time being we are going to
                    // assume they coming in order
                    String arrayName = key.substring(0, key.indexOf('['));
                    if (!arrays.containsKey(arrayName)) {
                        arrays.put(arrayName, new ArrayList());
                    }
                    arrays.get(arrayName).add(value);
                } else if (key.contains(".")) {
                    // We have an array for the time being we are going to
                    // assume they coming in order
                    String objectName = key.substring(0, key.indexOf('.'));
                    if (!objects.containsKey(objectName)) {
                        objects.put(objectName, new HashMap<String, String>());
                    }
                    objects.get(objectName).put(key.substring(key.indexOf('.') + 1), value);
                } else {
                    // we have a basic property which we can treat as an attribute
                    if (value != null) {
                        writer.writeStartElement("property");
                        writer.writeAttribute("name", key);
                        writer.writeAttribute("value", value.trim());
                        writer.writeEndElement();
                    }
                }
            }

            // We have processed all the STV now lets dump the objects and arrays
            for (String key : arrays.keySet()) {
                writer.writeStartElement(key);
                for (String value : arrays.get(key)) {
                    writer.writeStartElement("entry");
                    writer.writeAttribute("value", value.trim());
                    writer.writeEndElement();
                }
                writer.writeEndElement();
            }

            // We have processed all the STV now lets dump the objects and arrays
            for (String key : objects.keySet()) {
                writer.writeStartElement(key);
                for (String hashKey : objects.get(key).keySet()) {
                    writer.writeStartElement("property");
                    writer.writeAttribute("name", hashKey);
                    writer.writeAttribute("value", objects.get(key).get(hashKey).trim());
                    writer.writeEndElement();
                }
                writer.writeEndElement();
            }


            writer.writeEndElement();
            writer.writeEndDocument();

            writer.flush();
            writer.close();

            return sw.toString();

        } catch (Exception e) {
            throw new RuntimeException("Unable build XML writer", e);
        }
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
		System.out.println(StvtoXML.getXml(input));
	}
}
