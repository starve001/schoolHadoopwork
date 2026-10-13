package hadoop01;

import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.shaded.com.sun.jersey.spi.StringReader;

import java.io.IOException;

// 统计各星级评分人数，按照人数升序
// 使用两个Job
public class StarCountcopliot{

    static class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text xing = new Text();
        private IntWritable total = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            // 跳过表头和空行
            if (line.startsWith("_id") || line.isEmpty()) return;
            String[] fields = line.split(",");
            // 确保字段数足够（评分在第3列，索引2）
            if (fields.length >= 3) {
                // 去除评分字段的引号和空格
                String fenShu = fields[2].trim().replaceAll("\"", "");
                if (fenShu.isEmpty()) return;
                String[] fenShuSplit = fenShu.split(" ");

                if (fenShuSplit.length < 2) {
                    return;
                }
                String sf = fenShuSplit[0].trim();
                if (sf.isEmpty()) {
                    return;
                }

                xing.set(sf);
                context.write(xing, total);
            }
        }
    }

    static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable count = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            count.set(sum);
            context.write(key, count);
        }
    }

    // 排序Mapper：输入是统计Job的输出（星级\t数量），输出（数量，星级）用于排序
    static class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        IntWritable count = new IntWritable();
        Text star = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 修复：制表符分隔符应为 \t 而非 /t
            String[] cols = value.toString().split("\t");
            // 增加空值/长度校验，避免数组越界
            if (cols.length < 2) return;
            try {
                count.set(Integer.parseInt(cols[1].trim()));
                star.set(cols[0].trim());
                context.write(count, star);
            } catch (NumberFormatException e) {
                // 捕获数字转换异常，避免程序崩溃
                System.err.println("数字转换失败：" + cols[1]);
                return;
            }
        }
    }

    static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(val, key);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String URI = "hdfs://localhost:9000";
        Configuration conf = new Configuration();

        // 第一步：统计各星级人数的Job
        Job countjob = Job.getInstance(conf, "countJob");
        countjob.setJarByClass(StarCountcopliot.class);

        // 设置Mapper及其输出类型
        countjob.setMapperClass(CountMapper.class);
        countjob.setMapOutputKeyClass(Text.class);
        countjob.setMapOutputValueClass(IntWritable.class);

        // 设置Reducer及其输出类型
        countjob.setReducerClass(CountReducer.class);
        countjob.setOutputKeyClass(Text.class);
        countjob.setOutputValueClass(IntWritable.class);

        // 配置输入输出路径
        Path inputPath = new Path(URI + "/041601/避暑山庄评论.csv");
        Path countOutputPath = new Path(URI + "/041601/starCount1Job");

        FileInputFormat.setInputPaths(countjob, inputPath);

        // 判断输出目录是否存在，存在则删除
        FileSystem fs = countOutputPath.getFileSystem(conf);
        if (fs.exists(countOutputPath)) {
            fs.delete(countOutputPath, true);
        }
        FileOutputFormat.setOutputPath(countjob, countOutputPath);

        // 提交统计Job并等待完成（修复：移除System.exit，避免程序提前退出）
        boolean countSuccess = countjob.waitForCompletion(true);
        if (!countSuccess) {
            System.err.println("统计Job执行失败！");
            System.exit(1);
        }

        // 第二步：排序Job（基于统计Job的输出结果排序）
        Job sortjob = Job.getInstance(conf, "sortJob");
        sortjob.setJarByClass(StarCountcopliot.class);

        // 设置排序Mapper及其输出类型（修复：匹配SortMapper实际输出类型）
        sortjob.setMapperClass(SortMapper.class);
        sortjob.setMapOutputKeyClass(IntWritable.class);  // SortMapper输出Key是IntWritable
        sortjob.setMapOutputValueClass(Text.class);       // SortMapper输出Value是Text

        // 设置排序Reducer及其输出类型
        sortjob.setReducerClass(SortReducer.class);
        sortjob.setOutputKeyClass(Text.class);
        sortjob.setOutputValueClass(IntWritable.class);

        // 配置排序Job的输入（统计Job的输出）和输出路径
        Path sortOutputPath = new Path(URI + "/040901/starCountcopliot");
        FileInputFormat.setInputPaths(sortjob, countOutputPath);

        // 删除已存在的排序输出目录
        if (fs.exists(sortOutputPath)) {
            fs.delete(sortOutputPath, true);
        }
        FileOutputFormat.setOutputPath(sortjob, sortOutputPath);

        // 提交排序Job并等待完成
        boolean sortSuccess = sortjob.waitForCompletion(true);
        System.exit(sortSuccess ? 0 : 1);
    }
}