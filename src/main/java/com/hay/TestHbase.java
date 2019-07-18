package com.hay;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestHbase {

    private static Admin admin = null;
    private static Configuration configuration = null;
    private static Connection connection = null;

    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "192.168.0.113");


        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void close(Connection conn, Admin admin) {
        if (conn != null)
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        if (admin != null)
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

    }


    public static boolean isTableExist(String tableName) throws IOException {

        boolean tableExists = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        return tableExists;
    }


    public static void createTable(String tableName, String... columnFamily) throws IOException {
        //判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在");

            //System.exit(0);
        } else {
            //创建表属性对象,表名需要转字节
            HTableDescriptor htabledescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for (String cf : columnFamily) {
                htabledescriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置，创建表
            admin.createTable(htabledescriptor);
            System.out.println("表" + tableName + "创建成功!");
        }
    }


    public static void dropTable(String tableName) throws MasterNotRunningException,
            ZooKeeperConnectionException, IOException {
        HBaseAdmin admin = new HBaseAdmin(configuration);
        if (isTableExist(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表" + tableName + "删除成功!");
        } else {
            System.out.println("表" + tableName + "不存在!");
        }
    }


    public static void addRowData(String tableName, String rowKey, String columnFamily, String
            column, String value) throws IOException {
        //创建 HTable 对象
        HTable hTable = new HTable(configuration, tableName); //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //向 Put 对象中组装数据
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
        System.out.println("插入数据成功");
    }


    public static void deleteMultiRow(String tableName, String... rows) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        //HTable hTable = new HTable(configuration, tableName);
        List<Delete> deleteList = new ArrayList<Delete>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        table.delete(deleteList);
        table.close();
    }

    public static void scantable(String tabelname) throws IOException {
        //获取table
        Table table = connection.getTable(TableName.valueOf(tabelname));
        //获取扫描器
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);

        //遍历数据打印
        for (Result result : results) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ",Value:" + Bytes.toString(CellUtil.cloneValue(cell)));

            }

        }
        table.close();

    }


    public static void getData(String tablename, String rowkey, String cf, String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tablename));
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                    ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    ",Value:" + Bytes.toString(CellUtil.cloneValue(cell)));

        }
        table.close();

    }


    public static void main(String[] args) throws IOException {
        //System.out.println(isTableExist("test"));

        //addRowData("student","1002","info","sex","man");
        //createTable("student","info");
        // deleteMultiRow("student","1002");
        //scantable("student");
        getData("student", "1001", "info", "name");
        close(connection, admin);
    }
}


