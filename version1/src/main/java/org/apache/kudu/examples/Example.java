// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.kudu.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.SessionConfiguration.FlushMode;

public class Example {
  private static Random rd;
  private static final Double DEFAULT_DOUBLE = 12.345;
  private static final String KUDU_MASTERS = System.getProperty("kuduMasters", "localhost:7051");

  static void createExampleTable(KuduClient client, String tableName)  throws KuduException {
    // Set up a simple schema.
    List<ColumnSchema> columns = new ArrayList<>(3);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
        .key(true)
        .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.DOUBLE).nullable(false)
        .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("version", Type.STRING).nullable(false)
        .build());
    Schema schema = new Schema(columns);

    CreateTableOptions cto = new CreateTableOptions();
    List<String> hashKeys = new ArrayList<>(1);
    hashKeys.add("key");
    int numBuckets = 8;
    cto.addHashPartitions(hashKeys, numBuckets);

    // Create the table.
    client.createTable(tableName, schema, cto);
    System.out.println("Created table " + tableName);
  }

  static Double GetRandomNumber(){
    Double randomDouble=rd.nextDouble();
    Int randomInt = rd.nextInt(6);
    Double degrees = 32.0;
    if(randomInt%2==0){
      degrees -= randomDouble;
    }
    else{
      degrees += randomDouble;
    }
    return degrees;
  }

  static void insertRows(KuduClient client, String tableName, int numRows) throws KuduException {
    // Open the newly-created table and create a KuduSession.
    KuduTable table = client.openTable(tableName);
    KuduSession session = client.newSession();
    session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND);
    for (int i = 0; i < numRows; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addInt("key", i);
      row.addString("value", GetRandomNumber());
      row.addString("version", "1");
      session.apply(insert);
    }

    // Call session.close() to end the session and ensure the rows are
    // flushed and errors are returned.
    // You can also call session.flush() to do the same without ending the session.
    // When flushing in AUTO_FLUSH_BACKGROUND mode (the mode recommended
    // for most workloads, you must check the pending errors as shown below, since
    // write operations are flushed to Kudu in background threads.
    session.close();
    if (session.countPendingErrors() != 0) {
      System.out.println("errors inserting rows");
      org.apache.kudu.client.RowErrorsAndOverflowStatus roStatus = session.getPendingErrors();
      org.apache.kudu.client.RowError[] errs = roStatus.getRowErrors();
      int numErrs = Math.min(errs.length, 5);
      System.out.println("there were errors inserting rows to Kudu");
      System.out.println("the first few errors follow:");
      for (int i = 0; i < numErrs; i++) {
        System.out.println(errs[i]);
      }
      if (roStatus.isOverflowed()) {
        System.out.println("error buffer overflowed: some errors were discarded");
      }
      throw new RuntimeException("error inserting rows to Kudu");
    }
    System.out.println("Inserted " + numRows + " rows");
  }

  public static void main(String[] args) {
    rd = new Random();
    System.out.println("-----------------------------------------------");
    System.out.println("Will try to connect to Kudu master(s) at " + KUDU_MASTERS);
    System.out.println("Run with -DkuduMasters=master-0:port,master-1:port,... to override.");
    System.out.println("-----------------------------------------------");
    String tableName = "bigdata_temperature";
    KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();

    try {
      createExampleTable(client, tableName);

      int numRows = 1000;
      insertRows(client, tableName, numRows);

      
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      
        try {
          client.shutdown();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }