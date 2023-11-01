package com.java.flink.stream.base;

import com.java.flink.stream.func.IdSequenceSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/datastream/side_output/
 * 除了从SingleOutputStreamOperator产生主流结果流之外，可以产生任意数量的SideOutput结果流。
 * SideOutput结果流不需必须与主流中的数据的类型相同，不同的SideOutput结果流输出的类型也可以不同。
 * 当想要通过复制一个流并从中过滤流拆分数据流，SideOutput更加高效。
 *
 * 侧输出额外的输出算子是和主输出算子在一个task中的，并不会额外分出task，这点和ds添加多个sink function的一样
 *
 * 支持SideOutput输出流的函数(Process)：
 *    ProcessFunction
 *    KeyedProcessFunction
 *    CoProcessFunction
 *    KeyedCoProcessFunction
 *    ProcessWindowFunction
 *    ProcessAllWindowFunction
 *
 *
 */
public class SideOutputTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<Tuple2<Integer, Long>> ds = env.addSource(new IdSequenceSource(1000));

        final OutputTag<Tuple2<Integer, Long>> outputTag1 = new OutputTag<Tuple2<Integer, Long>>("side-output1"){};
        final OutputTag<String> outputTag2 = new OutputTag<String>("side-output2"){};
        // 元素可以发送到不同的输出中
        SingleOutputStreamOperator<Tuple2<Integer, String>> mainDs = ds.process(new ProcessFunction<Tuple2<Integer, Long>, Tuple2<Integer, String>>() {
            @Override
            public void processElement(Tuple2<Integer, Long> value, ProcessFunction<Tuple2<Integer, Long>, Tuple2<Integer, String>>.Context ctx, Collector<Tuple2<Integer, String>> out) throws Exception {
                // 发送数据到主要的输出
                out.collect(Tuple2.of(value.f0, value.f1.toString()));

                // 发送数据到旁路输出
                if (value.f1 % 3 == 0) {
                    ctx.output(outputTag1, value);
                }
                if (value.f1 % 4 == 0) {
                    ctx.output(outputTag2, "sideout2-" + value);
                }
            }
        });

        // 可以在 SingleOutputStreamOperator 运算结果上使用 getSideOutput(OutputTag) 方法获取旁路输出流。这将产生一个与旁路输出流结果类型一致的 DataStream：
        // 不能在DataStream上调用
        SideOutputDataStream<Tuple2<Integer, Long>> sideOutputDs1 = mainDs.getSideOutput(outputTag1);
        SideOutputDataStream<String> sideOutputDs2 = mainDs.getSideOutput(outputTag2);

        mainDs.print("main");
        sideOutputDs1.print("side1");
        sideOutputDs2.print("side2");

        env.execute("DataStreamWordCount");
    }


}
