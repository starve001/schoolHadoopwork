package hadoop01;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

/**
 * 高校毕业生信息分析 MapReduce 主类
 * 包含：GraduateMapper（静态内部类）、GraduateReducer（静态内部类）、main 方法
 */
public class GraduateMain {

    // ==================== Mapper ====================
    /**
     * Mapper：读取多源文件，识别文件名，对不同城市数据做差异化处理
     * 输出：<Text, IntWritable>
     */
    public static class GraduateMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        String file_name;

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            FileSplit fs = (FileSplit) context.getInputSplit();
            file_name = fs.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            String[] info = value.toString().split(",");

            if (file_name.contains("sq")) {
                // 商丘：统计就业情况（第6个字段为空则未就业）
                if (info.length > 5 && info[5].isEmpty()) {
                    context.write(new Text("商丘 未就业"), new IntWritable(1));
                } else {
                    context.write(new Text("商丘 就业"), new IntWritable(1));
                }
            } else if (file_name.contains("ly")) {
                // 洛阳：统计升学情况（第7个字段含"考研上岸"则成功）
                if (info.length > 6 && info[6].length() != 0 && info[6].contains("考研上岸")) {
                    context.write(new Text("洛阳 成功"), new IntWritable(1));
                } else {
                    context.write(new Text("洛阳 考研"), new IntWritable(1));
                }
            } else if (file_name.contains("cd")) {
                // 承德：计算薪资（第7个字段，支持范围格式如 3000-5000）
                if (info.length > 6) {
                    if (!info[6].equals("无") && !info[6].equals("")
                            && !info[6].equals("工资待遇（必填）")) {
                        if (info[6].length() > 4) {
                            String[] salary = info[6].split("-");
                            if (salary[0].length() == 4 && salary[1].length() >= 4) {
                                int sal1 = Integer.valueOf(salary[0]);
                                String salary2 = Pattern.compile("[^0-9]").matcher(salary[1]).replaceAll("");
                                int sal2 = Integer.valueOf(salary2);
                                int avg = (sal1 + sal2) / 2;
                                context.write(new Text("承德"), new IntWritable(avg));
                            }
                        } else {
                            context.write(new Text("承德"), new IntWritable(Integer.valueOf(info[6])));
                        }
                    }
                }
            } else if (file_name.contains("xc")) {
                // 许昌：计算薪资（第14个字段）
                if (info.length > 13) {
                    if (!info[13].equals("无") && !info[13].equals("")
                            && !info[13].equals("综合薪资")) {
                        int salary = Integer.valueOf(info[13]);
                        context.write(new Text("许昌"), new IntWritable(salary));
                    }
                }
            }
        }
    }

    // ==================== Reducer ====================
    /**
     * Reducer：在 reduce() 中聚合中间结果，在 cleanup() 中完成最终计算
     * 输出：<Text, DoubleWritable>
     */
    public static class GraduateReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        Map<String, Integer> map = new HashMap<String, Integer>();
        int count_cd = 0;
        int count_xc = 0;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException {
            if (key.toString().equals("承德")) {
                int total = 0;
                for (IntWritable v : values) {
                    total += v.get();
                    count_cd++;
                }
                map.put("承德", total);
            } else if (key.toString().equals("许昌")) {
                int total = 0;
                for (IntWritable v : values) {
                    total += v.get();
                    count_xc++;
                }
                map.put("许昌", total);
            } else if (key.toString().contains("商丘")) {
                int total = 0;
                for (IntWritable v : values) {
                    total += v.get();
                }
                map.put(key.toString(), total);
            } else if (key.toString().contains("洛阳")) {
                int total = 0;
                for (IntWritable v : values) {
                    total += v.get();
                }
                map.put(key.toString(), total);
            }
        }

        @Override
        protected void cleanup(Reducer<Text, IntWritable, Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException {
            // 承德平均薪资
            int total_cd = map.get("承德");
            double avg_salary_cd = total_cd / count_cd;

            // 许昌平均薪资
            int total_xc = map.get("许昌");
            double avg_salary_xc = total_xc / count_xc;

            // 商丘就业率
            int total_sq = map.get("商丘 未就业") + map.get("商丘 就业");
            double rate_sq = new BigDecimal((float) map.get("商丘 就业") / total_sq)
                    .setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();

            // 洛阳升学率
            int total_ly = map.get("洛阳 成功") + map.get("洛阳 考研");
            double rate_ly = new BigDecimal((float) map.get("洛阳 成功") / total_ly)
                    .setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();

            context.write(new Text("承德毕业生的平均薪资是"), new DoubleWritable(avg_salary_cd));
            context.write(new Text("许昌毕业生的平均薪资是"), new DoubleWritable(avg_salary_xc));
            context.write(new Text("商丘的就业率是"), new DoubleWritable(rate_sq));
            context.write(new Text("洛阳的升学率是"), new DoubleWritable(rate_ly));
        }
    }

    // ==================== Main ====================
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        String OUTPATH = "hdfs://localhost:9000/out";
        String INPUTPATH = "hdfs://localhost:9000/001/input";

        Path path = new Path(OUTPATH);
        FileSystem fs = path.getFileSystem(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        Job job = Job.getInstance(conf, "GraduateInfoAnalysis");
        job.setJarByClass(GraduateMain.class);

        job.setMapperClass(GraduateMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(GraduateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.setInputPaths(job, new Path(INPUTPATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPATH));

        job.waitForCompletion(true);
    }
}