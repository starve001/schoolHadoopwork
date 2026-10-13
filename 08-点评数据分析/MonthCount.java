package hadoop01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// 统计各月浏览人数
public class MonthCount {

    static class yueCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text nianyue = new Text();
        private LongWritable total = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if(line.startsWith("_id") || line.isEmpty()) return;
            String[] fields = line.split(",");
            if(fields.length >= 6) {
                String ny = fields[5].trim().replaceAll("\"", "");
                if (ny.isEmpty()) return;
                String[] nySplit = ny.split("-");

                if (nySplit.length < 3) {
                    return;
                }
                String sf = nySplit[0].trim() + nySplit[1].trim();
                if (sf.isEmpty()) {
                    return;
                }

                nianyue.set(sf);
                context.write(nianyue, total);
            }
        }
    }

    static class yueCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable count = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            count.set(sum);
            context.write(key, count);
        }
    }

    public static void main(String[] args) throws Exception {
        String URI = "hdfs://localhost:9000";
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, MonthCount.class.getName());
        job.setJarByClass(MonthCount.class);

        // 设置 Mapper 及其输出类型
        job.setMapperClass(MonthCount.yueCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 设置 Reducer 及其输出类型
        job.setReducerClass(MonthCount.yueCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 配置输入输出路径
        Path inputPath = new Path(URI+"/041601/避暑山庄评论.csv");
        Path outputPath = new Path(URI+"/041601/MonthCount");

        FileInputFormat.setInputPaths(job, inputPath);

        // 判断输出目录是否存在，存在则删除
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileOutputFormat.setOutputPath(job, outputPath);

        // 提交作业并等待完成
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
