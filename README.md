# Hadoop 课程作业集合

数据科学与大数据专业 Hadoop/MapReduce 课程实验代码，基于 Hadoop 3.x + Java 编写。

## 项目结构

| 目录 | 内容 | 核心知识点 |
|------|------|-----------|
| [01-HDFS基础操作](./01-HDFS基础操作) | HDFS Java API 封装 | FileSystem、Configuration、流式读写、元数据查看 |
| [02-WordCount词频统计](./02-WordCount词频统计) | 经典 WordCount 入门 | Mapper/Reducer/Job 三段式、Shuffle、Writable 序列化类型 |
| [03-中文分词词频统计](./03-中文分词词频统计) | HanLP 中文分词词频 | HanLP.segment、停用词过滤、CSVReader |
| [04-TopN排序](./04-TopN排序) | 词频Top3、图书借阅Top10 | setup/map/cleanup 生命周期、Reducer 全局排序 |
| [05-自定义Writable排序](./05-自定义Writable排序) | 图书借阅二次排序 | WritableComparable、write/readFields/compareTo |
| [06-毕业生多源分析](./06-毕业生多源分析) | 多城市毕业生就业/升学/薪资统计 | FileSplit 获取文件名、多源差异化处理、BigDecimal 精度 |
| [07-基站停留时间分析](./07-基站停留时间分析) | 运营商基站停留时长 | Configuration 传参、日期时间差、TreeMap 排序 |
| [08-点评数据分析](./08-点评数据分析) | 避暑山庄评论多指标统计 | CSV 解析、双 Job 链式排序、自定义分隔符、TreeMap |
| [09-日志清洗](./09-日志清洗) | 搜狗日志清洗 | Map-only 作业、setNumReduceTasks(0)、NullWritable |

## 环境要求

- JDK 1.8+
- Hadoop 3.x（伪分布式或集群）
- 依赖 Jar：hadoop-common、hadoop-mapreduce-client-core、hanlp、opencsv、log4j

## 运行方式

```bash
# 1. 编译打包（IDEA 中 Build → Build Artifacts → Jar）
# 2. 上传输入数据到 HDFS
hadoop fs -mkdir -p /input
hadoop fs -put your_data.txt /input/

# 3. 运行作业（替换主类名）
hadoop jar hadoop.jar hadoop01.WordCount

# 4. 查看结果
hadoop fs -cat /output/part-r-00000
```

## MapReduce 核心流程

```
InputFiles → InputFormat(切分) → Mapper(map) → Shuffle(分区+排序+合并)
          → Reducer(reduce) → OutputFormat → HDFS
```

## 作者

starve001
