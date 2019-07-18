package com.hay.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FruitDriver extends Configuration implements Tool {

    private Configuration configuration = null;

    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance(configuration);

        job.setJarByClass(FruitDriver.class);

        TableMapReduceUtil.initTableMapperJob(
                "fruit", //数据源的表名
                new Scan(), //scan扫描控制器
                FruitMapper.class,//设置 Mapper 类
                ImmutableBytesWritable.class,//设置 Mapper 输出 key 类型
                Put.class,//设置 Mapper 输出 value 值类型
                job//设置给哪个 JOB
        );

        TableMapReduceUtil.initTableReducerJob("fruit_mr", FruitReducer.class, job);
//设置 Reduce 数量，最少 1 个 job.setNumReduceTasks(1);
        boolean isSuccess = job.waitForCompletion(true);

        return isSuccess ? 0 : 1;


    }

    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int i = ToolRunner.run(conf, new FruitDriver(), args);
    }
}
