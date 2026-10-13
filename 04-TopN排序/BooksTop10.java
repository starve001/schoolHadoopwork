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

/**
 * MapReduce 实现图书馆热门书籍Top10
 * 利用Reducer.cleanup全部聚合后统一排序取前10
 */
public class BooksTop10 {

    /**
     * Mapper：读取每行借阅数据，本地HashMap预统计单Map任务内书籍借阅次数
     */
    static class BookMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text bookKey;
        private IntWritable one;
        private HashMap<String, Integer> localCountMap;

        @Override
        protected void setup(Context context) {
            bookKey = new Text();
            one = new IntWritable();
            localCountMap = new HashMap<>();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 读取单行数据，本实验books.txt每行仅为书名，直接去除首尾空格
            String bookName = value.toString().trim();
            if (bookName.isEmpty()) {
                return;
            }
            // 本地map累加计数
            localCountMap.merge(bookName, 1, Integer::sum);
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            // 当前map任务所有行处理完成，输出<书名,局部借阅次数>给Reducer汇总
            for (Map.Entry<String, Integer> entry : localCountMap.entrySet()) {
                bookKey.set(entry.getKey());
                one.set(entry.getValue());
                context.write(bookKey, one);
            }
        }
    }

    /**
     * Reducer：全局汇总每本书总借阅次数，cleanup中排序输出Top10
     */
    static class Top10Reducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        // 存储全量书籍+总借阅次数
        private LinkedHashMap<String, Integer> totalBookMap;

        @Override
        protected void setup(Context context) {
            totalBookMap = new LinkedHashMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int totalBorrow = 0;
            // 累加所有map传来的局部计数，得到书籍全局总借阅量
            for (IntWritable val : values) {
                totalBorrow += val.get();
            }
            totalBookMap.put(key.toString(), totalBorrow);
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            // 将map转list，按借阅次数降序排序
            List<Map.Entry<String, Integer>> bookList = new ArrayList<>(totalBookMap.entrySet());
            Collections.sort(bookList, new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    // 降序：借阅次数大的在前
                    return o2.getValue() - o1.getValue();
                }
            });
            // 截取前10本热门图书输出
            int limit = Math.min(10, bookList.size());
            for (int i = 0; i < limit; i++) {
                Map.Entry<String, Integer> item = bookList.get(i);
                context.write(new Text(item.getKey()), new IntWritable(item.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // 创建MapReduce任务
        Job job = Job.getInstance(conf, "library_books_top10");
        job.setJarByClass(BooksTop10.class);

        // 绑定Mapper、Reducer
        job.setMapperClass(BookMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(Top10Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // HDFS输出路径，重复运行前自动删除旧目录
        Path outputDir = new Path("/library/output");
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        // HDFS输入文件路径：对应上传后的books.txt
        FileInputFormat.addInputPath(job, new Path("/library/input/books.txt"));
        FileOutputFormat.setOutputPath(job, outputDir);

        // 提交任务，程序退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}