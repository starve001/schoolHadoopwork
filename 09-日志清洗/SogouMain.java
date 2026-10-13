package hadoop01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 搜狗日志清洗 MapReduce 主类
 * 包含：SogouMapper（静态内部类）、main 方法
 * 无 Reducer，只做数据过滤清洗
 */
public class SogouMain {

    // ==================== Mapper ====================
    /**
     * Mapper：搜狗日志数据清洗
     * 过滤字段不足6的行，输出完整记录
     * 输出：<Text(清洗后的记录), NullWritable>
     */
    public static class SogouMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            // 空行跳过
            if (line.isEmpty()) {
                return;
            }
            // 按1个或多个空格分割
            String[] fields = line.split("\\s+");
            // 过滤字段不足6的行
            if (fields.length < 6) {
                return;
            }
            // 输出完整记录，字段间用制表符分隔（方便Hive导入）
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < fields.length; i++) {
                sb.append(fields[i]);
                if (i < fields.length - 1) {
                    sb.append("\t");
                }
            }
            context.write(new Text(sb.toString()), NullWritable.get());
        }
    }

    // ==================== Main ====================
    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String inputPath = "hdfs://localhost:9000/001/input/搜狗日志查询分析.sample";
        String outputPath = "hdfs://localhost:9000/001/output";

        Path out = new Path(outputPath);
        FileSystem fs = out.getFileSystem(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        Job job = Job.getInstance(conf, "SogouLogClean");
        job.setJarByClass(SogouMain.class);

        job.setMapperClass(SogouMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 关键：无 Reducer，只做清洗过滤
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, out);

        job.waitForCompletion(true);
    }
}