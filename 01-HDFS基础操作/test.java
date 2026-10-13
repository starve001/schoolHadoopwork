package edu.sugon.hdfs;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary;
import com.hankcs.hanlp.seg.common.Term;
import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;

import java.io.StringReader;
import java.net.URI;
import java.util.List;

public class test {
    public static void main(String[] args) throws Exception {
        // 加载 log4j 日志配置
        PropertyConfigurator.configure("src/log4j.properties");

        // HDFS 连接地址
        String hdfsUrl = "hdfs://localhost:9000";
        FileSystem fs = null;

        try {
            Configuration conf = new Configuration();
            // 关闭写失败时的 DataNode 替换策略
            conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");

            URI uri = new URI(hdfsUrl);
            // 连接 HDFS，指定用户为 hadoop
            fs = FileSystem.get(uri, conf, "hadoop");

            // 在 HDFS 根目录创建 /test 目录
            fs.mkdirs(new Path("/test"));
            System.out.println("目录 /test 创建成功！");
        } finally {
            // 关闭文件系统，释放资源
            if (fs != null) {
                fs.close();
            }
        }
    }

}
