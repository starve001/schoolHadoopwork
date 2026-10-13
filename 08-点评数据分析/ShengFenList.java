package hadoop01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// 统计都有哪些省份
public class ShengFenList {

    static class ShengFenMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text shengFenWritable = new Text();
        private final static IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if(line.startsWith("_id") || line.isEmpty()) return;
            String[] fields = line.split(",");
            if(fields.length >= 7) {
                String address = fields[3].trim().replaceAll("\"", "");
                if (address.isEmpty() || "IP属地：未知".equals(address)) return;
                String[] addressSplit = address.split("：");
                // 格式不合法，跳过
                if (addressSplit.length < 2) {
                    return;
                }
                String shengfen = addressSplit[1].trim();
                // 省份为空，跳过
                if (shengfen.isEmpty()) {
                    return;
                }

                // 5. 输出<省份, 1>
                shengFenWritable.set(shengfen);
                context.write(shengFenWritable, ONE);
            }
        }
    }

    static class ShengFenReducer extends Reducer<Text, IntWritable, Text, Text> {
        private final Text result = new Text("");
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        String URI = "hdfs://localhost:9000";
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, ShengFenList.class.getName());
        job.setJarByClass(ShengFenList.class);

        // 设置 Mapper 及其输出类型
        job.setMapperClass(ShengFenList.ShengFenMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置 Reducer 及其输出类型
        job.setReducerClass(ShengFenList.ShengFenReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 配置输入输出路径
        Path inputPath = new Path(URI+"/041601/避暑山庄评论.csv");
        Path outputPath = new Path(URI+"/041601/ShengFenList");

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
