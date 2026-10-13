package hadoop01;
import com.opencsv.CSVReader;
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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.TreeMap;

public class StarCount {
    static class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        Text word = new Text();
        IntWritable one = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(key.get() != 0){ // 跳过表头行
                try {
                    CSVReader reader = new CSVReader(new StringReader(value.toString()));
                    String[] cols = reader.readNext();
                    reader.close();
                    if(cols.length >= 7){
                        // 修复1：清理评分列的特殊字符和空格，避免重复key
                        String star = cols[2].replace("\"", "").trim();
                        // 过滤异常值（如出现的675b8d15a0878211da1bba26这类非星级值）
                        if(star.startsWith("1分") || star.startsWith("2分") || star.startsWith("3分") ||
                                star.startsWith("4分") || star.startsWith("5分")){
                            word.set(star.split(" ")[0]);
                            context.write(word, one);
                        }
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }

    static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable count = new IntWritable();
        // 修复2：TreeMap改为 星级(key) -> 统计数(value)，保留按统计数排序的逻辑
        TreeMap<Integer, ArrayList<Text>> countToStars = new TreeMap<>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values){
                sum += val.get();
            }
            // 修复3：避免重复添加相同星级（解决重复行问题）
            if(!countToStars.containsKey(sum)){
                countToStars.put(sum, new ArrayList<>());
            }
            // 先判断列表中是否已存在该星级，不存在才添加
            ArrayList<Text> starList = countToStars.get(sum);
            boolean exists = false;
            for(Text t : starList){
                if(t.toString().equals(key.toString())){
                    exists = true;
                    break;
                }
            }
            if(!exists){
                starList.add(new Text(key)); // 新建Text对象避免引用覆盖
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 修复4：按统计数降序输出（更符合直观的“从多到少”展示）
            for(Integer cnt : countToStars.descendingKeySet()){
                count.set(cnt);
                ArrayList<Text> stars = countToStars.get(cnt);
                for(Text star : stars){
                    context.write(star, count);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "starCount");
        job.setJarByClass(StarCount.class);
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);
        // 修复5：修正MapOutputValueClass的配置错误（原代码写错成KeyClass）
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path output = new Path("/041601/starCount");
        FileSystem fs = output.getFileSystem(conf);
        if(fs.exists(output)){
            fs.delete(output, true);
        }
        FileInputFormat.setInputPaths(job, new Path("/041601/避暑山庄评论.csv"));
        FileOutputFormat.setOutputPath(job, output);
        job.waitForCompletion(true);
    }
}