package weibo;

import java.io.IOException;

public class Weibo {
    public  static void init() throws IOException {
        //创建相关命名空间和表
        WeiBoUtil.createNamespace(Constant.NAMESPACE);
        //创建内容表
        WeiBoUtil.createTable(Constant.CONTENT,1,"info");
        //创建用户关系表
        WeiBoUtil.createTable(Constant.RELATIONS,1,"attends","fans");
        //创建收件箱表（多版本）
        WeiBoUtil.createTable(Constant.INBOX,2,"info");
    }

    public static void main(String[] args) throws IOException {
        //测试
        //init();

        //1001,1002发布微博
        //WeiBoUtil.createData("1001","今天天气还可以！！！");
        //WeiBoUtil.createData("1002","今天天气不行！！！");


        //1001关注1002和1003
       // WeiBoUtil.addAttend("1001","1002","1003");

        //获取1001初始化页面信息
        //WeiBoUtil.getInit("1001");

        //1003发布微博
        //WeiBoUtil.createData("1003","1003发博啦！！！");
        //WeiBoUtil.createData("1003","1003二次发博啦！！！");
        //WeiBoUtil.createData("1003","1003三次发博啦！！！");
        //System.out.println("---------------------------------");

        //1001初始化
        WeiBoUtil.getInit("1001");

        //取关
        WeiBoUtil.delAttend("1001","1002");


        System.out.println();

        WeiBoUtil.getInit("1001");
    }
}
