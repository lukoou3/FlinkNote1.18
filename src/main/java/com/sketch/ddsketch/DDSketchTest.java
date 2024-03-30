package com.sketch.ddsketch;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketches;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.mapping.LogarithmicMapping;
import org.junit.Test;

public class DDSketchTest {

    @Test
    public void test() throws Exception {
        DDSketch sketch = DDSketches.logarithmicUnboundedDense(0.01);

        for (int i = 0; i <= 100; i++) {
            sketch.accept(i);
        }

        System.out.println(sketch.getValueAtQuantile(0.5));
        System.out.println(sketch.getMinValue());
        System.out.println(sketch.getMaxValue());
    }

    @Test
    public void test2() throws Exception {
        DDSketch sketch = DDSketches.logarithmicUnboundedDense(0.01);

        for (int i = 0; i <= 100; i++) {
            sketch.accept(i);
        }

        sketch.accept(1000000);

        System.out.println(sketch.getValueAtQuantile(0.5));
        System.out.println(sketch.getMinValue());
        System.out.println(sketch.getMaxValue());
    }

    @Test
    public void test3() throws Exception {
        DDSketch sketch = DDSketches.logarithmicUnboundedDense(0.01);
        sketch.accept(0.2);
        sketch.accept(0.5);
        sketch.accept(1);

        System.out.println(sketch.getValueAtQuantile(0.5));
        System.out.println(sketch.getMinValue());
        System.out.println(sketch.getMaxValue());
    }

    @Test
    public void testLogarithmicMapping() throws Exception {
        IndexMapping indexMapping = new LogarithmicMapping(0.01);
        double[] values = new double[]{0.1, 0.5, 1, 2, 5, 10, 20, 50, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000};
        for (double value : values) {
            int index = indexMapping.index(value);
            System.out.println(String.format("%7d index: %d, value:%.2f", (int)value, index, indexMapping.value(index)));
        }

    }

    /**
     * 和hdr库类似比hdr库分的更细(hdr只支持整数)：根据给定的误差通过公式把数轴上的值划分为bins, 刚开始范围较少, 之后越来越大.
     *      比如1.01的区间:[1.00, 1.01, 1.02], 100的区间:[99.50, 100.49, 101.51], 1000的区间:[992.50, 1002.43, 1012.55],
     *      10000的区间:[9900.16, 9999.17, 10100.17], 20000的区间:[19936.95, 20136.32, 20339.72]
     * @throws Exception
     */
    @Test
    public void testLogarithmicMapping2() throws Exception {
        IndexMapping indexMapping = new LogarithmicMapping(0.01);
        for (int i = 0; i < 500; i++) {
            int index = i;
            System.out.println(String.format("index: %d, value:[%.2f, %.2f, %.2f]", index, indexMapping.lowerBound(index), indexMapping.value(index), indexMapping.upperBound(index)));
        }
    }

    @Test
    public void testLogarithmicMapping3() throws Exception {
        IndexMapping indexMapping = new LogarithmicMapping(0.01);
        for (int i = -1; i > -500; i--) {
            int index = i;
            System.out.println(String.format("index: %d, value:%.4f", index, indexMapping.value(index)));
        }
    }
}
