package weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WeiBoUtil {

    //获取配置 conf
    private static Configuration configuration = HBaseConfiguration.create();
    static {
        configuration.set("hbase.zookeeper.quorum", "192.168.0.113");
    }
    //创建命名空间
    public static void createNamespace(String ns) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        admin.createNamespace(namespaceDescriptor);

        admin.close();
        connection.close();

    }
    //创建表
    public static void createTable(String tableName,int versions, String... columnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        HTableDescriptor htabledescriptor = new HTableDescriptor(TableName.valueOf(tableName));


        //循环添加列族
        for (String cf:columnFamily) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hColumnDescriptor.setMaxVersions(versions);
            htabledescriptor.addFamily(hColumnDescriptor);
        }

        admin.createTable(htabledescriptor);

        admin.close();
        connection.close();
    }

    //发布微博
    public static void createData(String uid,String content) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);

        //获取三张表对象
        Table contTable = connection.getTable(TableName.valueOf(Constant.CONTENT));
        Table relaTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));

        long ts = System.currentTimeMillis();

        String rowKey=uid+"_"+ts;

        //生成put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //添加数据
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("content"),Bytes.toBytes(content));
        contTable.put(put);

        //获取关系表fans
        Get get =new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes("fans"));
        Result result = relaTable.get(get);
        Cell[] cells = result.rawCells();

        if (cells.length<=0){
            return;
        }

        //更新fans收件表
        List<Put> puts=new ArrayList<>();
        for (Cell cell:cells) {
            byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
            Put inboxPut = new Put(cloneQualifier);
            inboxPut.addColumn(Bytes.toBytes("info"),Bytes.toBytes(uid),ts,Bytes.toBytes(rowKey));
            puts.add(inboxPut);
        }
        inboxTable.put(puts);

        inboxTable.close();
        relaTable.close();
        contTable.close();

        connection.close();

    }



    //关注用户
    /**1.在用户关系表
     * 添加操作的人（A）的attends
     * 添加被操作的人的fans
     *
     * 2.收件箱中
     *  微博内容中获取3条微博
     *  收件箱中添加操作人的关注者信息
     **/
    public static void addAttend(String uid,String ...uids) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);

        //获取三张表对象
        Table contTable = connection.getTable(TableName.valueOf(Constant.CONTENT));
        Table relaTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));

        //操作者put对象
        Put relaput = new Put(Bytes.toBytes(uid));

        ArrayList<Put> puts=new ArrayList<>();



        for (String s:uids) {
            relaput.addColumn(Bytes.toBytes("attends"),Bytes.toBytes(s),Bytes.toBytes(s));

            //创建被关注者（粉丝)的put对象
            Put fansput=new Put(Bytes.toBytes(s));
            fansput.addColumn(Bytes.toBytes("fans"),Bytes.toBytes(uid),Bytes.toBytes(uid));
            puts.add(fansput);
        }

        puts.add(relaput);
        relaTable.put(puts);



        Put inboxPut=new Put(Bytes.toBytes(uid));
        //获取内容表被关注的人的rowKey
        for (String s : uids) {
            Scan scan = new Scan(Bytes.toBytes(s),Bytes.toBytes(s+"|"));
            ResultScanner results = contTable.getScanner(scan);
            for (Result result : results) {
                String rowkey = Bytes.toString(result.getRow());
                String[] split = rowkey.split("_");
                byte[] row = result.getRow();
                inboxPut.addColumn(Bytes.toBytes("info"),Bytes.toBytes(s),Long.parseLong(split[1]),row);
            }
        }

        inboxTable.put(inboxPut);

        inboxTable.close();
        relaTable.close();
        contTable.close();

        connection.close();

    }




    //取关
    /**
    * 1.删除操作者关注列族的要取关的用户
    *   删除代取关用户fans列族的操作者
      2.收件表
     删除操作者待取关用户的信息
    *
    * */
    public static void delAttend(String uid ,String... uids) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);

        Table relaTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));


        Delete relaDel = new Delete(Bytes.toBytes(uid));


        ArrayList<Delete> deletes = new ArrayList<>();
        for (String s : uids) {
            Delete fansDel = new Delete(Bytes.toBytes(s));
            fansDel.addColumns(Bytes.toBytes("fans"),Bytes.toBytes(uid));
            relaDel.addColumns(Bytes.toBytes("attends"),Bytes.toBytes(s));
            deletes.add(fansDel);
        }

        deletes.add(relaDel);

        relaTable.delete(deletes);


        Delete inboxDel = new Delete(Bytes.toBytes(uid));
        for (String s : uids) {
            inboxDel.addColumns(Bytes.toBytes("info"),Bytes.toBytes(s));
        }
        inboxTable.delete(inboxDel);

        inboxTable.close();
        relaTable.close();

        connection.close();
    }


    //获取微博内容（初始化页面）
    public static void getInit(String uid) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);

        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));
        Table contTable = connection.getTable(TableName.valueOf(Constant.CONTENT));
        
        
        //收件箱表数据
        Get get = new Get(Bytes.toBytes(uid));
        get.setMaxVersions();
        Result result = inboxTable.get(get);


        ArrayList<Get> gets = new ArrayList<>();

        //遍历返回内容封装为get对象
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            Get conGet = new Get(CellUtil.cloneValue(cell));
            gets.add(conGet);
        }

        //更具收件箱取值去内容表取微博内容
        Result[] results = contTable.get(gets);
        for (Result result1 : results) {
            Cell[] cells1 = result1.rawCells();
            for (Cell cell : cells1) {
                System.out.println("RK:"+Bytes.toString(CellUtil.cloneRow(cell))+",Content:"+
                        Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }


    }




    //获取微博内容（查看某个人所有微博）
    public static void getData(String uid) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);

        Table table = connection.getTable(TableName.valueOf(Constant.CONTENT));
        Scan scan = new Scan();
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_"));

        scan.setFilter(rowFilter);
        ResultScanner results = table.getScanner(scan);

        for (Result result : results) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("RK:"+Bytes.toString(CellUtil.cloneRow(cell))+",Content:"+
                        Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        table.close();
        connection.close();

    }

}
