package hadoop01;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 运营商用户基站停留时间统计 MapReduce 主类
 * 包含：PhoneMapper（静态内部类）、PhoneReducer（静态内部类）、main 方法
 * 命令行参数：时间段配置，如 "09-18-24"
 */
public class PhoneMain {

    // ==================== Mapper ====================
    /**
     * Mapper：读取基站日志，判断每条记录所属时段
     * 输出：<Text(用户ID\t时段), Text(基站位置-通讯时间)>
     */
    public static class PhoneMapper extends Mapper<LongWritable, Text, Text, Text> {
        private int[] timeRangeList;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            String timeRange = configuration.get("timeRange");
            if (timeRange == null) {
                throw new RuntimeException("not receive the arguments......");
            }
            // 如果传入 "1"，表示不分时段，直接按全天统计
            if ("1".equals(timeRange)) {
                timeRangeList = new int[1];
                timeRangeList[0] = 24;
                return;
            }
            // 解析时间段参数，如 "09-18-24" -> [9, 18, 24]
            String[] timeRangeString = timeRange.split("-");
            timeRangeList = new int[timeRangeString.length];
            for (int i = 0; i < timeRangeString.length; i++) {
                timeRangeList[i] = Integer.parseInt(timeRangeString[i]);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String values[] = value.toString().split("\\s+");
            String userId = values[0];
            String timeString = values[4];
            String baseStation = values[2];

            // 提取小时数
            int hour = Integer.parseInt(timeString.split(":")[0]);
            int startHour = 0;
            int endHour = 0;

            // 判断该记录属于哪个时段
            for (int i = 0; i < timeRangeList.length; i++) {
                if (hour < timeRangeList[i]) {
                    if (i == 0) {
                        startHour = 0;
                    } else {
                        startHour = timeRangeList[i - 1];
                    }
                    endHour = timeRangeList[i];
                    break;
                }
            }
            // 未匹配到有效时段则跳过
            if (startHour == 0 && endHour == 0) {
                return;
            }

            String timeRange = context.getConfiguration().get("timeRange");
            // 不分时段模式
            if ("1".equals(timeRange)) {
                context.write(new Text(userId + "\t"), new Text(baseStation + "-" + timeString));
                return;
            }
            // 分时段模式
            context.write(new Text(userId + "\t" + startHour + "-" + endHour + "\t"),
                    new Text(baseStation + "-" + timeString));
        }
    }

    // ==================== Reducer ====================
    /**
     * Reducer：按时间排序后计算相邻记录时间差，累加各基站停留时长
     * 输出：<Text(用户ID\t时段\t基站), LongWritable(停留分钟数)>
     */
    public static class PhoneReducer extends Reducer<Text, Text, Text, org.apache.hadoop.io.LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> valueList = new LinkedList<String>();
            Map<String, Long> residenceTimeMap = new TreeMap<String, Long>();

            // 将迭代器中的值存入 List
            for (Text value : values) {
                valueList.add(value.toString());
            }
            // 数据量不足一条无法计算停留时间
            if (valueList == null || valueList.size() <= 1) {
                return;
            }

            // 按通讯时间排序（格式：基站-时间）
            Collections.sort(valueList, new Comparator<String>() {
                public int compare(String o1, String o2) {
                    o1 = o1.split("-")[1];
                    o2 = o2.split("-")[1];
                    return o1.compareTo(o2);
                }
            });

            // 遍历计算相邻记录的时间差
            for (int i = 0; i < valueList.size() - 1; i++) {
                String station = valueList.get(i).split("-")[0];
                String time1 = valueList.get(i).split("-")[1];
                String time2 = valueList.get(i + 1).split("-")[1];

                DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
                Date date1 = null;
                Date date2 = null;
                try {
                    date1 = dateFormat.parse(time1);
                    date2 = dateFormat.parse(time2);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                // 时间异常则跳过
                if (date1.after(date2)) {
                    continue;
                }

                // 计算停留分钟数
                long minute = (date2.getTime() - date1.getTime());
                minute = minute / 1000 / 60;

                // 累加到对应基站
                Long count = residenceTimeMap.get(station);
                if (count == null) {
                    residenceTimeMap.put(station, minute);
                } else {
                    residenceTimeMap.put(station, count + minute);
                }
            }

            valueList = null;

            // 按停留时间降序排列
            List<Map.Entry<String, Long>> entryList =
                    new ArrayList<Entry<String, Long>>(residenceTimeMap.entrySet());
            Collections.sort(entryList, new Comparator<Map.Entry<String, Long>>() {
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    return -(o1.getValue().compareTo(o2.getValue()));
                }
            });

            Set<String> keySet = residenceTimeMap.keySet();
            String timeRange = context.getConfiguration().get("timeRange");

            // 不分时段模式只输出停留时间最长的基站
            if ("1".equals(timeRange)) {
                String mapKey = keySet.iterator().next();
                context.write(new Text(key + "\t" + mapKey + "\t"),
                        new org.apache.hadoop.io.LongWritable(residenceTimeMap.get(mapKey)));
                return;
            }

            // 输出所有基站的停留时间
            for (String mapKey : keySet) {
                context.write(new Text(key + "\t" + mapKey + "\t"),
                        new org.apache.hadoop.io.LongWritable(residenceTimeMap.get(mapKey)));
            }
            residenceTimeMap = null;
        }
    }

    // ==================== Main ====================
    public static void main(String[] args)
            throws InterruptedException, IOException, ClassNotFoundException {
        if (args == null || args.length != 1) {
            throw new RuntimeException("argument is not right!");
        }

        String input = "hdfs://localhost:9000/001/input/pos.txt";
        Configuration configuration = new Configuration();
        configuration.set("timeRange", args[0]);

        Job job = Job.getInstance(configuration, "PhoneBaseStationResidence");
        job.setJarByClass(PhoneMain.class);

        job.setMapperClass(PhoneMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(PhoneReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(org.apache.hadoop.io.LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/001/out"));

        job.waitForCompletion(true);
    }
}