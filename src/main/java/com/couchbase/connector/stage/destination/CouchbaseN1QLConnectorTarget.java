/*
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.connector.stage.destination;

import com.couchbase.connector.stage.connection.CouchbaseConnector;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.connector.stage.lib.Errors;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This target is a used to connect to a Couchbase NoSQL Database VIA N1QL.
 */
public abstract class CouchbaseN1QLConnectorTarget extends BaseTarget {
    
    private CouchbaseConnector connector;
    
    private DataGeneratorFactory generatorFactory;
    
    private static final Logger LOG = LoggerFactory.getLogger(CouchbaseN1QLConnectorTarget.class);
    
    private static final String CUS_DOC_KEY_RESOURCE_NAME = "customDocumentKey";
    private ELEval customDocKeyEvals;
    private ELVars customDocKeyVars;

  /** {@inheritDoc} */
  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();
    
    //EL Init
    customDocKeyEvals = getContext().createELEval(CUS_DOC_KEY_RESOURCE_NAME);
    customDocKeyVars = getContext().createELVars();
    
    //Connect to Couchbase DB
    LOG.info("Connecting to Couchbase " +  " with details: " + getURL() + " " + getBucket());
    
    //Connect to Couchbase
    connector = CouchbaseConnector.getInstance(getURL(), getBucket(), getUserName(), getUserPassword());
    
    //Data Generator for JSON Objects to Couchbase
    DataGeneratorFactoryBuilder builder = new DataGeneratorFactoryBuilder(
        getContext(),
        DataFormat.JSON.getGeneratorFormat()
    );
    builder.setCharset(StandardCharsets.UTF_8);
    builder.setMode(Mode.MULTIPLE_OBJECTS);
    generatorFactory = builder.build();

    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
    // Clean up any open resources.
    super.destroy();
    connector.closeConnection();
    connector = null;
    
  }

  /** {@inheritDoc} */
  @Override
  public void write(Batch batch) throws StageException {
     
    Iterator<Record> batchIterator = batch.getRecords();
    
    //Create a list of JSON documents
    List<JsonDocument> documentList = new ArrayList<JsonDocument>();
    
    //Create a List of JSON Document for Batch Iterator
    while (batchIterator.hasNext()) {
      Record record = batchIterator.next();
              
      try {
        //Get JsonDocument from Record
        JsonDocument doc = getJsonDocument(record);
        //Add to list
        //System.out.println(doc.content().get("ID"));
        documentList.add(doc);
        
      } catch (Exception e) {
        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            getContext().toError(record, Errors.SAMPLE_01, e.toString());
            break;
          case STOP_PIPELINE:
            throw new StageException(Errors.SAMPLE_01, e.toString());
          default:
            throw new IllegalStateException(
                Utils.format("Unknown OnError value '{}'", getContext().getOnErrorRecord(), e)
            );
        }
      }
    }
    
    //Create INSERT/UPDATE Stament for N1QL Insert/Update
    if (documentList.size() > 0) {
        //n1QL String
        String n1qlStatement = "INSERT INTO `" + getBucket() + "` (KEY, VALUE) ";
        
        LOG.info("Writing BATCH with " + documentList.size() + " number of records. ");
        
        //Add JSON Values to N1QL Statement
        Iterator<JsonDocument> documentIterator =  documentList.iterator();
        
        while (documentIterator.hasNext()) {
            JsonDocument jsonDoc = documentIterator.next();
            JsonObject jsonObject = jsonDoc.content();
            
            String valueString = "VALUES ( \"" + jsonDoc.id() + "\",  " + jsonObject.toString() + "), ";
            n1qlStatement = n1qlStatement + valueString;
        }
        
        //Remove last comman and replace with inverted comma
        n1qlStatement = n1qlStatement.substring(0, n1qlStatement.length() - 2);
        n1qlStatement = n1qlStatement + ";";
        LOG.debug("STATEMENT " + n1qlStatement);
        
        //Write Batch to Couchbase
        connector.queryBucket(n1qlStatement); 
        
    }
        LOG.info("Batch size is zero");
    
    
  }
 
  private JsonDocument getJsonDocument(Record record) {
      JsonDocument doc = null;
      
      try {
        //Generate data from the record object and create JsonObject from byte ARRAY String   
        //LOG.info("Here is the record: " + record);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
        DataGenerator generator = generatorFactory.getGenerator(baos);
        generator.write(record);
        generator.close();
        JsonObject jsonObject = JsonObject.fromJson(new String(baos.toByteArray()));
        
        //LOG.info("DATA - " + jsonObject);
        
        //Either get key JSON or generate unique one
        Object keyObject = null;
        
        if (generateDocumentKey()) {
            UUID uuid = UUID.randomUUID();
            keyObject = uuid.toString();
        }
        else {
            keyObject = getCustomDocumentKeyValue(getCustomDocumentKey(), record);
                
                
            if (keyObject == null)
                throw new NullPointerException("Document Key is Null");
        }
        
        String keyString = keyObject.toString();
        
        doc = JsonDocument.create(keyString, jsonObject);
        
      } catch (ELEvalException ele) {
            LOG.error(ele.getMessage());
      } catch (IOException ioe) {
            LOG.error(ioe.getMessage());
      } catch (DataGeneratorException dge) {
        LOG.error(dge.getMessage());
      }
        
      return doc;
  }
  
  private String getCustomDocumentKeyValue(String customDocumentKey, Record record) throws ELEvalException {
      RecordEL.setRecordInContext(customDocKeyVars, record);
      return customDocKeyEvals.eval(customDocKeyVars, customDocumentKey, String.class);
  }
  
   //Configuration get methods
  public abstract String getURL();
  
  public abstract String getUserName();
  
  public abstract String getUserPassword();
   
  public abstract String getBucket();
  
  public abstract boolean generateDocumentKey();
  
  public abstract String getCustomDocumentKey();
  
}
