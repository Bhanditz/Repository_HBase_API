package org.bibalex.org.hbase.handler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.bibalex.org.hbase.models.NodeRecord;
import org.bibalex.org.hbase.models.Taxon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


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
            if(startRow != null)
                scan.setStartRow(startRow);
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
            System.err.println("Failed to release HBase Handler ");
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
    public boolean deleteResourceRecords (int resId, String resourceCreatedAtTimeStamp) throws IOException {

        Table nodesTable = connection.getTable(TableName.valueOf("Nodes")),
        mediaTable = connection.getTable(TableName.valueOf("Media"));
        FilterList filterList = new FilterList();
        filterList.addFilter(new PrefixFilter(Bytes.toBytes(resId+"_")));
        List<Delete> deleteNodeRecordList = new ArrayList<>();
        List<Delete> deleteGuids = new ArrayList<>();
        NodesHandler nodesHandler = new NodesHandler(HbaseHandler.getHbaseHandler(),"Nodes", "");
        ResultScanner resultScanner = scan("Nodes", filterList, resourceCreatedAtTimeStamp, String.valueOf(System.currentTimeMillis()), "-1".getBytes());
        System.out.println(resourceCreatedAtTimeStamp);
        for (Result r: resultScanner){
            System.out.println(Bytes.toString(r.getRow()));
            NodeRecord node = new NodeRecord();
            String[] rowKeyParts = Bytes.toString(r.getRow()).split("_");
            node.setGeneratedNodeId(rowKeyParts[1]);
            node.setResourceId(Integer.parseInt(rowKeyParts[0]));
            Set<byte[]> relationsColumnQualifiers = r.getFamilyMap(Bytes.toBytes("Relations")).keySet();
            for (byte[] i : relationsColumnQualifiers) {
                try {
                    Taxon taxon = (Taxon) NodesHandler.deserialize(r.getValue(Bytes.toBytes("Relations"), i));
                    node.setTaxon(taxon);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    System.out.println("relation serialization Error");
                }
            }
            if (node != null && node.getTaxon() != null && node.getTaxon().getGuids() != null) {
                node.setMedia(nodesHandler.getMediaOfNode(node.getTaxon().getGuids(), "0"));
            }
            System.out.println(((node.getTaxon().getGuids()).size()));
            System.out.println(String.valueOf(node.getTaxon().getGuids()));
            for (String guid: node.getTaxon().getGuids())
                deleteGuids.add(new Delete (Bytes.toBytes(guid)));
//            deleteGuids.add(new Delete(Bytes.toBytes(String.valueOf(node.getTaxon().getGuids()))));
//            deleteNodeTaxonMediaList.add((List<Delete>) deleteGuids);
            deleteNodeRecordList.add(new Delete(r.getRow()));
        }
        mediaTable.delete(deleteGuids);
        nodesTable.delete(deleteNodeRecordList);
        return true;
    }

    public static void main(String[] args) throws IOException {
        HbaseHandler hb = HbaseHandler.getHbaseHandler();
//        hb.deleteResourceRecords(503);
//   hb.createTable("nodes", new String[] { "names", "refs" } );
//   hb.scan("Nodes", null, null);
    }



}
