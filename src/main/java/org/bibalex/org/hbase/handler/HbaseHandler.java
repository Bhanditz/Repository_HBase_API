package org.bibalex.org.hbase.handler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class HbaseHandler {


    private static HbaseHandler instance = null;
    private Configuration config;
    private Connection connection;
    private Admin admin;

    private HbaseHandler() {
        config = HBaseConfiguration.create();
        String path = this.getClass()
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        config.addResource(new Path(path));

        try {
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();

        } catch (IOException e) {
            System.err.println("Failed to initailze connection with HBase as an admin." + e);
        }
    }

    public static HbaseHandler getHbaseHandler() {

        if (instance == null) {
            instance = new HbaseHandler();
        }
        return instance;
    }

    public boolean createTable(String tableName, String[] columFamilies) {
        try {
            HTableDescriptor table =
                    new HTableDescriptor(TableName.valueOf(tableName));
            for (int i = 0; i < columFamilies.length; i++)
                table.addFamily(new HColumnDescriptor(columFamilies[i]));
            HBaseAdmin.checkHBaseAvailable(config);

            if (!admin.tableExists(table.getTableName())) {
                admin.createTable(table);
            }
            return true;
        } catch (Exception e) {
            System.err.println("Failed to create Table on HBase " + e);
            return false;
        }
    }

    public boolean dropTable(String tableName) {
        try {
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
            if (admin.tableExists(table.getTableName())) {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            return true;
        } catch (IOException e) {
            System.err.println("Failed to drop table " + tableName + " from HBase " + e);
            return false;
        }
    }

    public boolean addRow(String tableName, Put object) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            table.put(object);
            table.close();
            return true;
        } catch (IOException e) {
            System.err.println("Failed to add row to " + tableName + " Table on HBase " + e);
            return false;
        }
    }

    public boolean addcolumn(String tableName, byte[] columFamily, byte[] rowKey, byte[] columName,
                             byte[] value) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            // Construct a "put" object for insert
            Put p = new Put(rowKey);
            p.addColumn(columFamily, columName, value);
            table.put(p);
            table.close();
            return true;
        } catch (IOException e) {
            System.err.println("Failed to add row to " + tableName + " Table on HBase " + e);
            return false;
        }
    }

    public Result getRow(String tableName, byte[] rowKey) {

        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get g = new Get(rowKey);
            // Get the result by passing the getter to the table
            Result r = table.get(g);
            table.close();
            return r;
        } catch (IOException e) {
            System.err.println("Failed to add row to " + tableName + " Table on HBase: " + e);
            return null;
        }
    }

    public ResultScanner scan(String tableName, FilterList filterList, String startTimestamp,String endTimestamp,byte[] startRow) {
        try {

            ResultScanner results = null;
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            if(startRow != null) {
                byte[] POSTFIX = new byte[] { 0x00 };
                startRow = Bytes.add(startRow, POSTFIX);
                scan.setStartRow(startRow);
            }
            if(startTimestamp != null&&endTimestamp == null)
                scan.setTimeRange(Long.parseLong(startTimestamp), System.currentTimeMillis());
            if(startTimestamp != null&&endTimestamp!=null)
                scan.setTimeRange(Long.parseLong(startTimestamp),Long.parseLong(endTimestamp) );
            if(filterList != null)
                scan.setFilter(filterList);
            results = table.getScanner(scan);
            return results;
        } catch (IOException e) {
            System.err.println("Failed to add row to " + tableName + " Table on HBase " + e);
            return null;
        }
    }

    public boolean release() {
        try {
            connection.close();
            admin.close();
            instance = null;
            return true;
        } catch (IOException e) {
            System.err.println("Failed to relase Hbase Handler ");
            return false;
        }
    }


    public boolean deleteColumn(String tableName, byte[] rowKey, byte[] columnFamily, byte[] columnQualifier) {

        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(rowKey);
            delete.addColumn(columnFamily,columnQualifier, Long.parseLong("1520846070116"));
//            delete.deleteColumn(columnFamily, columnQualifier);
            table.delete(delete);


            table.close();
            return true;
        } catch (IOException e) {
            System.err.println("Failed to add row to " + tableName + " Table on HBase: " + e);
            return false;
        }
    }

    public static void main(String[] args)
    {
        HbaseHandler hb = HbaseHandler.getHbaseHandler();
//   hb.createTable("nodes", new String[] { "names", "refs" } );
//   hb.scan("Nodes", null, null);
    }



}
