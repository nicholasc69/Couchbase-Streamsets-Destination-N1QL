package com.couchbase.connector.stage.destination;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;

@StageDef(
    version = 1,
    label = "Couchbase N1QL Ingestion",
    description = "Couchbase N1QL-based Destination",
    icon = "couchbase.png",
    recordsByRef = true,
    onlineHelpRefUrl = ""
)

@ConfigGroups(value = Groups.class)
@GenerateResourceBundle

public class CouchbaseN1QLConnectorDTarget extends CouchbaseN1QLConnectorTarget {

    @ConfigDef(
        required = true,
        type = ConfigDef.Type.STRING,
        defaultValue = "localhost:8093",
        label = "URL",
        displayPosition = 10,
        description = "The URL endpoint of the Couchbase NoSQL Database Cluster",
        group = "COUCHBASE_N1QL_TARGET"
    )
    public String URL;

    /** {@inheritDoc} */
    @Override
    public String getURL() {
      return URL;
    }

      @ConfigDef(
        required = true,
        type = ConfigDef.Type.STRING,
        defaultValue = "",
        label = "Bucket",
        displayPosition = 20,
        description = "Couchbase Destination Bucket to ingesting data",
        group = "COUCHBASE_N1QL_TARGET"
      )
    public String bucket;

    /** {@inheritDoc} */
    @Override
    public String getBucket() {
      return bucket;
    }
    @ConfigDef(
        required = true,
        type = ConfigDef.Type.STRING,
        defaultValue = "",
        label = "Couchbase User Name",
        displayPosition = 40,
        description = "Specify a Couchbase user name for connecting the bucket",
        group = "COUCHBASE_N1QL_TARGET"
    )
    public String userName;

    /** {@inheritDoc} */
    @Override
    public String getUserName() {
      return userName;
    }
    
    @ConfigDef(
        required = true,
        type = ConfigDef.Type.STRING,
        defaultValue = "",
        label = "Couchbase User Password",
        displayPosition = 50,
        description = "Specify a password for the Couchbase user name",
        group = "COUCHBASE_N1QL_TARGET"
    )
    public String userPassword;

    /** {@inheritDoc} */
    @Override
    public String getUserPassword() {
      return userPassword;
    }
      
    @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Custom Document Key",
      displayPosition = 60,
      description = "Custom Document Key. Use the Expression Language to define a custom document key. "
              + "Example: str:concat('customer::', records:value('/customerID'))",
      group = "COUCHBASE_N1QL_TARGET",
      elDefs = {RecordEL.class, TimeEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
    )

    public String customDocumentKey;

    /** {@inheritDoc} */
    @Override
    public String getCustomDocumentKey() {
      return customDocumentKey;
    }

    @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Generate unique Document Key",
      displayPosition = 70,
      description = "Generate a unique document key if document key field cannot be set",
      group = "COUCHBASE_N1QL_TARGET"
    )

    public boolean generateDocumentKey;

    /** {@inheritDoc} */
    @Override
    public boolean generateDocumentKey() {
        return generateDocumentKey;
    }
}
