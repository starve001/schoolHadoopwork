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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class WordCountTopN {

    static class WordMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        Text wordKey;
        IntWritable countValue;
        HashMap<String, Integer> localMap;

        @Override
        protected void setup(Context context) {
            wordKey = new Text();
            countValue = new IntWritable();
            localMap = new HashMap<>();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] words = line.split("\\s+");
            for (String w : words) {
                if (!w.isEmpty()) {
                    localMap.merge(w, 1, Integer::sum);
                }
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : localMap.entrySet()) {
                wordKey.set(entry.getKey());
                countValue.set(entry.getValue());
                context.write(wordKey, countValue);
            }
        }
    }

    static class TopNReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        LinkedHashMap<String, Integer> wordMap;

        @Override
        protected void setup(Context context) {
            wordMap = new LinkedHashMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            wordMap.put(key.toString(), sum);
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            List<Map.Entry<String, Integer>> list =
                    new ArrayList<>(wordMap.entrySet());

            Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1,
                                   Map.Entry<String, Integer> o2) {
                    return o2.getValue() - o1.getValue();
                }
            });

            int topN = Math.min(3, list.size());
            for (int i = 0; i < topN; i++) {
                Map.Entry<String, Integer> entry = list.get(i);
                context.write(new Text(entry.getKey()),
                        new IntWritable(entry.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Job job = Job.getInstance(conf, "wordcount_topn");
        job.setJarByClass(WordCountTopN.class);

        job.setMapperClass(WordMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(TopNReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outputPath = new Path("/output");
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job, new Path("/datasave/data.txt"));
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
