/*
 * INTEL CONFIDENTIAL
 * Copyright 2021 Intel Corporation
 *
 * The source code contained or described herein and all documents related to
 * the source code ("Material") are owned by Intel Corporation or its suppliers
 * or licensors. Title to the Material remains with Intel Corporation or
 * its suppliers and licensors. The Material contains trade secrets and
 * proprietary and confidential information of Intel or its suppliers and
 * licensors. The Material is protected by worldwide copyright and trade secret
 * laws and treaty provisions. No part of the Material may be used, copied,
 * reproduced, modified, published, uploaded, posted, transmitted, distributed,
 * or disclosed in any way without Intel's prior express written permission.
 *
 * No license under any patent, copyright, trade secret or other intellectual
 * property right is granted to or conferred upon you by disclosure or delivery
 * of the Materials, either expressly, by implication, inducement, estoppel or
 * otherwise. Any license under such intellectual property rights must be express
 * and approved by Intel in writing.
*/

package com.intel.featureStorage.utils;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.DatatypeConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class HbaseConnectionHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseConnectionHelper.class);

    private static Connection connection = null;

    private static Configuration configuration = null;

    private static final int writeBufferSize = 5 * 1024 * 1024;

    static Connection getHBaseConnection() throws IOException {
        if (connection == null || connection.isClosed()) {
            LOGGER.info("Connection does not exist or was closed. Create a new connection.");
            synchronized (HbaseConnectionHelper.class) {
                if (connection == null || connection.isClosed()) {
                    LOGGER.info("synchronized : Connection does not exist or was closed. Create a new connection.");
                    configuration = HBaseConfiguration.create();
                    configuration.addResource(new Path("hbase-site.xml"));
                    connection = ConnectionFactory.createConnection(configuration);
                }
            }
        }
        LOGGER.info("Connection does exist or was not closed");
        return connection;
    }

    public static void closeConnection() throws IOException {
        if(connection!= null && !connection.isClosed()) {
            try {
                connection.close();
            }catch(IOException e)
            {
                LOGGER.error("closeConnection failure !", e);
            }
        }
    }

    public static void createTable(String tableName, String[] columnFamilies){
        TableName name = TableName.valueOf(tableName);
        try(Admin admin = getHBaseConnection().getAdmin()) {
            boolean isExists = admin.tableExists(name);
            if (isExists) {
                throw new TableExistsException("Table " + tableName + " exists!");
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(name);
            for (int i = 0; i < columnFamilies.length; i++) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamilies[i]);
                //hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
                hColumnDescriptor.setMaxVersions(1);
                tableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(tableDescriptor);
        }catch (IOException e) {
            LOGGER.error("Error creating table {} ; proceeding.", tableName, e);
        }
    }

    public static void dropTable(String tableName){
        TableName name = TableName.valueOf(tableName);
        try(Admin admin = getHBaseConnection().getAdmin()) {
            boolean isExists = admin.tableExists(name);
            if (!isExists) {
                LOGGER.warn("Table: {} does not exists!", tableName);
                return;
            }
            admin.disableTable(name);
            admin.deleteTable(name);
            LOGGER.info("Table: {} was deleted successfully!", tableName);
        }catch (IOException e) {
            LOGGER.error("Error dropping table {} ; proceeding.", tableName, e);
        }
    }

    public static boolean hasTable(String tableName) {
        TableName name = TableName.valueOf(tableName);
        try(Admin admin = getHBaseConnection().getAdmin()) {
            return admin.tableExists(name);
        }catch (IOException e){
            LOGGER.error("Error checking whether table {} exists; proceeding.", tableName, e);
        }
        return false;
    }

    public static long putBatch(String tableName, List<Put> puts) throws IOException {
        long currentTime = System.currentTimeMillis();
        Connection conn = getHBaseConnection();
        final BufferedMutator.ExceptionListener listener = (e, mutator) -> {
            for (int i = 0; i < e.getNumExceptions(); i++) {
                LOGGER.error("Failed to sent put " + e.getRow(i) + ".");
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName))
            .listener(listener);
        params.writeBufferSize(writeBufferSize);

        try(final BufferedMutator mutator = conn.getBufferedMutator(params);) {
            mutator.mutate(puts);
            mutator.flush();
        }catch(IOException e){
            LOGGER.error("Error writing to table {}; proceeding.", tableName, e);
        }
        return System.currentTimeMillis() - currentTime;
    }

    public static void delete(String tableName, String[] rowKeys) {
        try (Table table = getHBaseConnection().getTable(TableName.valueOf(tableName));) {
            if (table != null) {
                List<Delete> list = new ArrayList<>();
                for (String row : rowKeys) {
                    Delete d = new Delete(DatatypeConverter.parseBase64Binary(row));
                    list.add(d);
                }
                if (list.size() > 0) {
                    table.delete(list);
                }
            }
        } catch (IOException e) {
            LOGGER.error("Error deleting rows in table {}; proceeding.", tableName, e);
        }
    }

    public static Result[] get(String tableName, List<byte[]> rowKeys)  throws IOException{
        List<Get> gets = null;
        Result[] results = null;
        try(Table table = getHBaseConnection().getTable(TableName.valueOf(tableName));){
            if (table != null) {
                gets = new ArrayList<>();
                for (byte[] row : rowKeys) {
                    if(row != null){
                        gets.add(new Get(row));
                    }
                }
            }
            if (gets.size() > 0) {
                results = table.get(gets);
            }
        } catch (IOException e) {
            LOGGER.error("Error getting Rows from table {}", tableName, e);
        }
        return results;
    }

}
