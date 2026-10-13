package hadoop01;

import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.util.HashMap;

public class ProvinceZongHe {
    static class ZongHeMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text word = new Text();
        Text one = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(key.get() != 0){
                try {
                    CSVReader reader = new CSVReader(new StringReader(value.toString()));
                    String[] cols = reader.readNext();
                    reader.close();
                    if(cols.length >= 7){
                        String stars = cols[2].replace("\"", "").trim();
                        String times = cols[5].replace("\"", "").trim();
                        String shengfens = cols[3].replace("\"", "").trim().replace("IP属地：", "");
                        String scoreStr;
                        if(stars.startsWith("1分") || stars.startsWith("2分") || stars.startsWith("3分") ||
                                stars.startsWith("4分") || stars.startsWith("5分")){
                            scoreStr = stars.split(" ")[0].replace("分", "").trim();
                        }else{
                            return;
                        }
                        String month = times.split("-")[1];
                        String ss = "1@#@" + scoreStr + "@#@" + month;
                        word.set(shengfens);
                        one.set(ss);
                        context.write(word, one);
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }

    static class ZongHeReducer extends Reducer<Text, Text, Text, Text> {
        Text valueWritable = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int maxnum = 0;
            int minnum = 10000000;
            int cnt = 0;
            int total = 0;
            ArrayList<String> list = new ArrayList<>();
            for(Text value:values){
                String[] cols = value.toString().split("@#@");
                int num = Integer.parseInt(cols[0]);
                int rate =  Integer.parseInt(cols[1]);
                if(rate > maxnum){
                    maxnum = rate;
                }
                if(rate < minnum){
                    minnum = rate;
                }
                cnt ++;
                total += rate;
                list.add(value.toString());
            }
            double mean = (double)total / cnt;

            HashMap<String, Integer> map = new HashMap<>();
            for(String s:list){
                String[] cols = s.split("@#@");
                int num = Integer.parseInt(cols[1]);
                String month = cols[2];
                if(num == minnum){
                    if(map.containsKey(month)){
                        map.put(month, map.get(month) + 1);
                    }else {
                        map.put(month, 1);
                    }
                }
            }

            String month = "";
            int maxCount = 0;
            for(String k :  map.keySet()){
                if(map.get(k) > maxCount){
                    maxCount = map.get(k);
                    month = k;
                }
            }

            // 从人数 平均分 最高分 最低分 月份
            valueWritable.set(month + '\t' + mean + '\t' + maxnum + '\t' + minnum + '\t' + month);
            context.write(key, valueWritable);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "ZongHe");
        job.setJarByClass(ProvinceZongHe.class);
        job.setMapperClass(ZongHeMapper.class);
        job.setReducerClass(ZongHeReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        Path output = new Path("/041601/ZongHeJob");
        FileSystem fs = output.getFileSystem(conf);
        if(fs.exists(output)){
            fs.delete(output, true);
        }
        FileInputFormat.setInputPaths(job, new Path("/041601/避暑山庄评论.csv"));
        FileOutputFormat.setOutputPath(job, output);
        job.waitForCompletion(true);
    }
}
