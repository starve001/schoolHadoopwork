package hadoop01;

/*
 *  词频统计
 */

import org.apache.hadoop.conf.Configuration;
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

public class CoreStopWordDictionary {

    /**
     * 输入:
     * 行号 内容
     * 0  hello world
     * 输出：
     * hello 1
     * world 1
     */
    static  class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        // 提前 new 减少每次 new 的内存占用
        Text wordWritable = new Text();
        IntWritable countWritable = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 分词
            String line = value.toString();
            String[] words = line.split(" ");
            for(String word : words){
                wordWritable.set(word);
                context.write(wordWritable, countWritable);
            }
        }
    }

    /**
     * 输入：
     * hello [1,1,1]
     * 输出：
     * hello 3
     */
    static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        IntWritable count = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values){
                sum += val.get();
            }
            count.set(sum);
            context.write(key, count);
        }
    }

    /**
     * JOB, 定义输入、输出路径，明确Mapper类和Reducer类
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 1. 创建Hadoop配置对象Configuration
        // Configuration是Hadoop的核心配置类，会自动加载Hadoop的核心配置文件（core-site.xml、hdfs-site.xml、mapred-site.xml、yarn-site.xml等）
        // 也可以通过该对象手动设置作业的自定义参数，整个作业的运行都依赖这个配置对象
        Configuration conf = new Configuration();

        // 2. 创建MapReduce作业对象Job
        // Job是Hadoop中描述一个MapReduce作业的核心类，一个Job就是一个完整的MapReduce任务
        // getInstance是静态工厂方法，用于创建Job实例，第一个参数传入配置对象，第二个参数是作业的名称（这里用当前主类的全类名，方便在YARN界面区分作业）
        Job job = Job.getInstance(conf, WordCountCN.class.getName());

        // 3. 设置作业的Jar包
        // 该方法通过传入主类WordCount.class，让Hadoop自动根据这个类的位置，找到包含该类的Jar包
        // Hadoop会把这个Jar包分发到集群的各个节点，让节点可以运行我们自定义的Map和Reduce程序
        job.setJarByClass(WordCountCN.class);

        // ---------------------- 配置Map阶段相关参数 ----------------------
        // 设置作业使用的Mapper类，WordCountMapper是我们自定义的、实现了map业务逻辑的类（继承Hadoop的Mapper父类）
        // Hadoop在Map阶段，会调用这个类的map方法处理每一条输入数据
        job.setMapperClass(WordCountMapper.class);

        // 设置Map阶段输出的Key的数据类型
        // Text是Hadoop实现的可序列化的字符串类型，对应Java的String，这里Map输出的Key是拆分后的单词，所以用Text类型
        // Hadoop必须知道该类型，才能完成数据的序列化、网络传输、排序等操作
        job.setMapOutputKeyClass(Text.class);

        // 设置Map阶段输出的Value的数据类型
        // IntWritable是Hadoop实现的可序列化的整型类型，对应Java的Integer，这里Map输出的Value是单词计数1，所以用IntWritable类型
        job.setMapOutputValueClass(IntWritable.class);

        // ---------------------- 配置Reduce阶段相关参数 ----------------------
        // 设置作业使用的Reducer类，WordCountReducer是我们自定义的、实现了reduce业务逻辑的类（继承Hadoop的Reducer父类）
        // Hadoop在Reduce阶段，会调用这个类的reduce方法，对Map输出、经过Shuffle后的数据做聚合统计
        job.setReducerClass(WordCountReducer.class);

        // 设置Reduce阶段（整个作业最终）输出的Key的数据类型
        // 这里最终输出的Key还是单词，所以用Text类型；如果Map和Reduce的输出Key类型一致，可只设置该方法，否则必须单独设置Map的输出类型
        job.setOutputKeyClass(Text.class);

        // 设置Reduce阶段（整个作业最终）输出的Value的数据类型
        // 这里最终输出的Value是单词的总计数，所以用IntWritable类型
        job.setOutputValueClass(IntWritable.class);

        // ---------------------- 配置作业输入输出路径 ----------------------
        // 设置作业的输入路径，FileInputFormat是Hadoop处理文件输入的基类
        // setInputPaths是静态方法，第一个参数是作业对象，第二个参数是输入路径的Path对象
        // 【强制要求】输入路径必须是已经存在的路径，Hadoop会读取该路径下的所有文件作为Map阶段的输入数据，空字符串需要替换为实际的HDFS/本地路径
        FileInputFormat.setInputPaths(job, new Path("/040901/a.txt"));

        // 设置作业的输出路径，FileOutputFormat是Hadoop处理文件输出的基类
        // setOutputPath是静态方法，第一个参数是作业对象，第二个参数是输出路径的Path对象
        // 【强制要求】输出路径必须是不存在的路径，如果路径已存在，Hadoop会直接报错，防止覆盖已有数据，空字符串需要替换为实际的HDFS/本地路径
        FileOutputFormat.setOutputPath(job, new Path("/040901/WordCount"));

        // ---------------------- 提交作业并等待运行完成 ----------------------
        // 提交作业到集群，并阻塞当前线程，直到作业运行完成
        // 参数verbose为true时，会在控制台实时打印作业的运行进度、Map和Reduce阶段的完成占比等详细日志；为false则只等待完成，不打印详细进度
        // 该方法返回boolean值，代表作业最终是否执行成功
        job.waitForCompletion(true);
    }
}