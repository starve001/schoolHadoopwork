package hadoop01;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary;
import com.hankcs.hanlp.seg.common.Term;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
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
import java.io.Reader;
import java.io.StringReader;
import java.util.List;

public class WordCountCN {
    static class WordCountCNMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        // 提前 new 减少每次 new 的内存占用
        private Text wordWritable = new Text();
        private IntWritable countWritable = new IntWritable(1);


        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 将 Text 类型改为 String 类型
            String ll = value.toString();

            // 忽略第一行
            if(ll.startsWith("_id") || ll.trim().isEmpty()) return;

            // 按照 , 分词
            String[] fields = ll.split(",");

            // 防护机制，列表长度为7
            if(fields.length >= 7) {
                try{
                    // 取出“评论内容”
                    String pings = fields[4];
                    // 改为 Reader 类型
                    Reader r = new StringReader(pings);
                    // 读入
                    CSVReader reader = new CSVReader(r);
                    String[] lines = null;
                    try {
                        lines = reader.readNext();
                    } catch (CsvValidationException e) {
                        throw new RuntimeException(e);
                    }
                    reader.close();
                    for (String line : lines) {
                        List<Term> termList = HanLP.segment(line);
                        for (Term term : termList) {
                            // 停用词
                            if(CoreStopWordDictionary.shouldInclude(term)){
                                wordWritable.set(term.word);
                                context.write(wordWritable, countWritable);
                            }
                        }
                    }

                }catch (Exception e){

                }
            }
        }
    }

    static class WordCountCNReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        IntWritable count = new IntWritable();

        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            count.set(sum);
            context.write(key, count);
        }
    }


    public static void main(String[] args) throws Exception {
        String URI = "hdfs://localhost:9000";
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, WordCountCN.class.getName());
        job.setJarByClass(WordCountCN.class);

        // 设置 Mapper 及其输出类型
        job.setMapperClass(WordCountCN.WordCountCNMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置 Reducer 及其输出类型
        job.setReducerClass(WordCountCN.WordCountCNReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 配置输入输出路径
        Path inputPath = new Path(URI+"/041601/避暑山庄评论.csv");
        Path outputPath = new Path(URI+"/041601/WordCountCN");

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
