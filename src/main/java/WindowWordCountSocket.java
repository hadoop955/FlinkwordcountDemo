/**
 * @Author: skm
 * @Date: 2019/5/19 9:55
 * @Version scala-2.11.8 +jdk-1.8+spark-2.0.1
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 滑动窗口计算
 * 1.通过socket模拟产生发单词数据
 * 2.flink对数据进行统计计算
 * 3.计算的规则是
 * 需要实现每隔1s对最近2s产生的数据进行统计计算
 */
public class WindowWordCountSocket {
    public static void main(String[] args) {

        //获取flink的运行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置socket流的配置信息
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("No port specified, use the default port 6857");
            port = 6857;
        }
        String hostname = "master";
        String delimiter = "\n";
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.socketTextStream(hostname, port, delimiter);

        /**
         * 传入的数据为 hell world hello flink
         * 则处理为
         * （hello,1）
         * (world,1)
         * (hello,1)
         * (flink,1)
         */
        SingleOutputStreamOperator<WordCount> singleOutputStreamOperator = stringDataStreamSource.flatMap(new FlatMapFunction<String, WordCount>() {
            public void flatMap(String s, Collector<WordCount> collector) throws Exception {
                //将的到的每一个值按照空格切割
                String[] splits = s.split("\\s");
                for (String everyWord : splits) {
                    collector.collect(new WordCount(everyWord, 1));
                }
            }
        })          //对（word，1）种元组进行汇总
                //设置滑动窗口的参数值（滑动窗口的大小，滑动窗口间隔时间）
                //此处滑动窗口的含义为每过1s处理前2s的数据
                .keyBy("word")
                .timeWindow(Time.seconds(2), Time.seconds(1))
                .sum("num");

        //将处理后的数据打印打到控制台
        singleOutputStreamOperator.print().setParallelism(1);

        //执行
        try {
            executionEnvironment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //创建内部静态类
    public static class WordCount {
        public String word;
        public int num;

        //默认无参构造方法
        public WordCount() {

        }

        //定义构造方法
        public WordCount(String word, int num) {
            this.word = word;
            this.num = num;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", num=" + num +
                    '}';
        }
    }
}
