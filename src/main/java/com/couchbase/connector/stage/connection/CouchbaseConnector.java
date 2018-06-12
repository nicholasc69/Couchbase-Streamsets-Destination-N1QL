/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.couchbase.connector.stage.connection;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.util.retry.RetryBuilder;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.JsonStringDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.AsyncN1qlQueryRow;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.connector.stage.destination.CouchbaseN1QLConnectorTarget;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;


/**
 *
 * @author Nick Cadehead
 * @version 1.1
 * 
 * CouchbaseConnector is singleton class that manages all connection requirements to a
 * Couchbase bucket. 
 * 
 * CouchbaseConnector handles all CRUD operations for the Couchbase Destination.
 */
public class CouchbaseConnector {
    private Cluster cluster;
    private Bucket bucket;
    
   
    
    private static final Logger LOG = LoggerFactory.getLogger(CouchbaseN1QLConnectorTarget.class);
    
    
   
    
    /**
     * CouchbaseConnector                           
     * <p>
     * Constructor methods which takes standard connection parameters for a Couchbase Cluster (Version 5)
     * <p>
     *
     * @param  urlString URL Endpoint to the Couchbase Cluster.
     * @param  bucketString Couchbase Bucket Name
     * @param  userName Couchbase UserName
     * @param  userPassword Couchbase Couchbase User password
     */
    private CouchbaseConnector(String urlString, String bucketString, String userName, String userPassword) {
        connectToCouchbaseServer(urlString, bucketString, userName, userPassword);
    }
    
        /**
     * connectToCouchbaseServer                           
     * <p>
     * Connection method to version 5
     * <p>
     *
     * @param  url URL Endpoint to the Couchbase Cluster.          
     * @param  bucketName Couchbase Bucket Name
     * @param  userName Couchbase UserName
     * @param  userPassword Couchbase Couchbase User password
     */
    private void connectToCouchbaseServer(String url, String bucketName, String userName, String userPassword) {
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment
                .builder()
                .retryStrategy(BestEffortRetryStrategy.INSTANCE)
                .build();
             
        
        //Init Couchbase
        cluster  = CouchbaseCluster.create(env, url);
        cluster.authenticate(userName, userPassword);
       
        try {
            bucket = cluster.openBucket(bucketName);
            LOG.info("Connected to Couchbase version 5");
        } catch (Exception e) {
            LOG.info("Exception" + e + " occurred while connecting to Couchbase version 5");
            e.printStackTrace();
        }
       
        
    }
    
        /**
     * connectToCouchbaseServer                           
     * <p>
     * Get an instance to connect to Couchbase Version 5
     * <p>
     *
     * @param  url URL Endpoint to the Couchbase Cluster.          
     * @param  bucket Couchbase Bucket Name
     * @param  userName Couchbase Username
     * @param  userPassword Couchbase User Password
     */
    public static CouchbaseConnector getInstance(String url, String bucket, String userName, String userPassword) {
        
        return new CouchbaseConnector(url, bucket, userName, userPassword);
    }
        
     /**
     * queryBucket 
     * <p>
     * Send a N1QL query to Couchbase Bucket
     * <p>
     *
     * @param  queryString N1QL Query.          
     */    
    
    public N1qlQueryResult queryBucket(String query) {
    
        // Construct Query
        N1qlQuery n1qlQuery = N1qlQuery.simple(query);
        
        N1qlQueryResult result = bucket.query(n1qlQuery);
        /*
        bucket.async()
            .query(n1qlQuery)
            .flatMap(result ->
                result.errors()
                .flatMap(e -> Observable.<AsyncN1qlQueryRow>error(new CouchbaseException("N1QL Error/Warning: " + e)))
                .switchIfEmpty(result.rows())
            )
            .map(AsyncN1qlQueryRow::value)
            .subscribe(
                rowContent -> System.out.println(rowContent),
                runtimeError -> runtimeError.printStackTrace()
            ); */
        
        return result;
    }
    
    public boolean closeConnection()
    {
        return bucket.close();
    }
             
}
