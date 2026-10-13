package edu.sugon.hdfs;
import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
public class HDFSOpt {
//HDFS 集群地址常量
    public static final String HDFS_URI = "hdfs://localhost:9000";
    // 日志对象，用于记录操作日志
    private static final Logger logger = Logger.getLogger(HDFSOpt.class);


    public static FileSystem getFileSystem() throws Exception {
        Configuration conf = new Configuration();                                           // 追加追加文件的容错配置
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        URI uri = new URI(HDFS_URI);
        // 获取HDFS连接（指定URI、配置、操作用户）
        FileSystem fs = FileSystem.get(uri, conf, "hadoop");
        return fs;
    }

    public static void mkdir(Path hdfsPath, FileSystem fs) throws Exception
    {
        logger.info("开始创建目录：" + hdfsPath);
        fs.mkdirs(hdfsPath);
        logger.info(hdfsPath + "目录创建完成");
    }

    public static void exists(Path hdfsPath, FileSystem fs) throws Exception{

        boolean result = fs.exists(hdfsPath);

        if(result){

            logger.info(hdfsPath + "已存在");

        }else{

            logger.info(hdfsPath + "不存在");

        }

    }

    public static void copyFromLocal(Path localPath, Path hdfsPath, FileSystem fs) throws Exception{
        logger.info("文件开始上传");
        fs.copyFromLocalFile(localPath, hdfsPath);
        logger.info("上传完成");
    }

    public static void copyToLocal(Path hdfsPath, Path localPath, FileSystem fs) throws Exception{
        logger.info("文件开始下载");
        fs.copyToLocalFile(hdfsPath, localPath);
        logger.info("下载完成");
    }

    public static void isDirectory(Path hdfsPath, FileSystem fs) throws Exception{
        FileStatus status = fs.getFileStatus(hdfsPath);
        boolean result = status.isDirectory();
        if(result){
            logger.info(hdfsPath+"是目录");
        }else{
            logger.info(hdfsPath+"是文件");
        }
    }

    public static void delete(Path hdfsPath, FileSystem fs) throws Exception{
        fs.delete(hdfsPath, true);
        logger.info(hdfsPath+"删除完成");
    }

    public static void append(String content, Path hdfsPath, FileSystem fs) throws Exception{
        FSDataOutputStream out =  fs.append(hdfsPath);
        out.write(content.getBytes(StandardCharsets.UTF_8));
        out.close();
    }

    public static void copy(Path srcPath, Path dstPath, FileSystem fs) throws Exception{
        FileUtil.copy(fs, srcPath, fs, dstPath, false, fs.getConf());
        logger.info("文件复制完成");
    }

    public static  void cat(Path hdfsPath, FileSystem fs) throws Exception{
        FSDataInputStream in = fs.open(hdfsPath);
        IOUtils.copyBytes(in, System.out, 1024, true);
    }

    public static void ls(Path hdfsPath, FileSystem fs) throws Exception{
        FileStatus[] status = fs.listStatus(hdfsPath);
        for(FileStatus f : status){
            logger.info("文件路径： " + f.getPath());
            logger.info(f.getLen());          // 文件大小（字节）
            logger.info(f.getReplication()); // 副本数
            logger.info(f.getOwner());       // 文件所有者
            logger.info(f.getModificationTime()); // 修改时间
            logger.info("************************");
        }
    }
    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure("src/log4j.properties");
        FileSystem fs = getFileSystem();
        Path hdfsPath = new Path("/032401");
        //mkdir(new Path("/032401"), fs);
        //exists(hdfsPath, fs);
        copyFromLocal(new Path("src/log4j.properties"), hdfsPath, fs);
        //copyToLocal(new Path("/032401/log4j.properties"), new Path("src/a.txt"), fs);
        //isDirectory(hdfsPath, fs);
        //delete(hdfsPath, fs);
        //append("你好",new Path("/031803"),fs);
        //copy(new Path("/031803"), new Path("/031803.txt"), fs);
        //cat(new Path("/031803"), fs);
        ls(new Path("/test"), fs);
    }
}
