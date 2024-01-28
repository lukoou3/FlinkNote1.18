package com.java.flink.clickhouse.hlltest;

import it.unimi.dsi.fastutil.doubles.Double2IntAVLTreeMap;
import it.unimi.dsi.fastutil.doubles.Double2IntSortedMap;
import org.apache.datasketches.memory.internal.XxHash64;
import org.apache.flink.util.Preconditions;

import java.util.Map;

import static com.java.flink.clickhouse.hlltest.HllConstants.*;

public class Hll6Array {
    static final long DEFAULT_HASH_SEED = 0L; // 9001L
    int precision;
    int reg; // reg = 2^p
    byte[] regs;
    int maxVal;

    Hll6Array(int precision) {
        this.precision = precision;
        int reg = 1 << precision;
        int size = (reg * 6 + 7) / 8;
        this.reg = reg;
        this.regs = new byte[size];
        this.maxVal = 64 - precision + 1;
    }

    public void add(long val) {
        final long[] data = { val };
        long h = XxHash64.hashLongs(data, 0, data.length, DEFAULT_HASH_SEED);
        addHash(h);
    }

    public void add(double val) {
        final double[] data = { val };
        long h = XxHash64.hashDoubles(data, 0, data.length, DEFAULT_HASH_SEED);
        addHash(h);
    }

    public void add(String val) {
        if(val == null || val.isEmpty()){
            return;
        }
        long h = XxHash64.hashString(val, 0, val.length(), DEFAULT_HASH_SEED);
        addHash(h);
    }

    public void add(byte[] val) {
        long h = XxHash64.hashBytes(val, 0, val.length, DEFAULT_HASH_SEED);
        addHash(h);
    }

    public void addHash(long hashcode) {
        addHash1(hashcode);
    }

    // ck的实现方式
    public void addHash1(long hashcode) {
        int p = precision;
        // [0, p)
        int idx = (int) (hashcode & ((1 << p) - 1));
        // Determine the count of tailing zeros
        // long w =  (hashcode >> p) & ((1 << (64 - p)) - 1);
        long w =  (hashcode >> p) ;
        int tailing = Long.numberOfTrailingZeros(w) + 1;
        if (tailing > maxVal) {
            tailing = maxVal;
        }
        // Update the register if the new value is larger
        if (tailing > getRegister(idx)) {
            setRegister(idx, tailing);
        }

    }

    public void addHash2(long hashcode) {
        int p = getPrecision();
        // Determine the index using the first p bits
        final int idx = (int) (hashcode >>> (64 - p));
        // Shift out the index bits
        final long w = hashcode << p | 1 << (p - 1);
        // Determine the count of leading zeros
        int leading = Long.numberOfLeadingZeros(w) + 1;
        if (leading > maxVal) {
            leading = maxVal;
        }
        // Update the register if the new value is larger
        if (leading > getRegister(idx)) {
            setRegister(idx, leading);
        }
    }

    int getRegister(int idx) {
        int startBit = idx * 6;
        int shift = startBit & 0X7;
        int indexLeft = startBit >>> 3;
        int indexRight = (startBit + 5) >>> 3;
        if (indexLeft == indexRight) {
            int word = regs[indexLeft];
            return (word >>> shift) & 0X3F;
        } else {
            int word = ((regs[indexLeft] & 0XFF) | ((regs[indexLeft + 1] & 0XFF) << 8));
            return (word >>> shift) & 0X3F;
        }
    }

    void setRegister(int idx, int val) {
        //int odlVal = val;
        int startBit = idx * 6;
        int shift = startBit & 0X7;
        int indexLeft = startBit >>> 3;
        int indexRight = (startBit + 5) >>> 3;
        if (indexLeft == indexRight) {
            int word = regs[indexLeft];
            val = val << shift;
            int valMask = 0X3F << shift; // 所在bit范围mask
            regs[indexLeft] = (byte) ((word & ~valMask) | val);
            //Preconditions.checkArgument(getRegister(idx) == odlVal);
        } else {
//            int odlVal1 = getRegister(idx - 1) ;
//            int odlVal2 = getRegister(idx + 1) ;
            int word = ((regs[indexLeft] & 0XFF) | ((regs[indexLeft + 1] & 0XFF) << 8));
            val = val << shift;
            int valMask = 0X3F << shift; // 所在bit范围mask
            int insert = (word & ~valMask) | val;
            regs[indexLeft] = (byte) (insert);
            regs[indexLeft + 1] = (byte) (insert >>> 8);

//            Preconditions.checkArgument(getRegister(idx) == odlVal);
//            Preconditions.checkArgument(getRegister(idx - 1) == odlVal1);
//            Preconditions.checkArgument(getRegister(idx + 1) == odlVal2);
        }
    }

    int getPrecision(){
        return precision;
    }

    public double size() {
        int p = getPrecision();
        int reg = 1 << p;

        RawEstAndNumZeros estAndNumZeros = rawEstimate(p, reg);
        double rawEst = estAndNumZeros.rawEst;
        int numZeros = estAndNumZeros.numZeros;

        // Check if we need to apply bias correction
        if (rawEst <= 5 * reg) {
            //rawEst -= biasEstimate(rawEst);
            rawEst -= estimateBias(rawEst, p);
        }

        // Check if linear counting should be used
        double altEst;
        if (numZeros != 0) {
            altEst = linearCount(reg, numZeros);
        } else {
            altEst = rawEst;
        }

        // Determine which estimate to use
        if (altEst <= thresholdData[p-4]) {
            return altEst;
        } else {
            return rawEst;
        }
    }

    private double alpha(int p, int reg) {
        switch (p) {
            case 4:
                return 0.673;
            case 5:
                return 0.697;
            case 6:
                return 0.709;
            default:
                return 0.7213 / (1 + 1.079 / reg);
        }
    }

    private RawEstAndNumZeros rawEstimate(int p, int reg) {
        double multi = alpha(p, reg) * reg * reg;

        int numZeros = 0;
        double sum = 0;
        int regVal;
        for (int i = 0; i < reg; i++) {
            regVal = getRegister(i);
            sum += inversePow2Data[regVal];
            if (regVal == 0) {
                numZeros++;
            }
        }
        return new RawEstAndNumZeros(multi / sum, numZeros);
    }

    private long estimateBias(double rawEst, int p) {
        double[] rawEstForP = rawEstimateData[p - 4];

        // compute distance and store it in sorted map
        Double2IntSortedMap estIndexMap = new Double2IntAVLTreeMap();
        double distance = 0;
        for (int i = 0; i < rawEstForP.length; i++) {
            distance = Math.pow(rawEst - rawEstForP[i], 2);
            estIndexMap.put(distance, i);
        }

        // take top-k closest neighbors and compute the bias corrected cardinality
        long result = 0;
        double[] biasForP = biasData[p - 4];
        double biasSum = 0;
        int kNeighbors = K_NEAREST_NEIGHBOR;
        for (Map.Entry<Double, Integer> entry : estIndexMap.entrySet()) {
            biasSum += biasForP[entry.getValue()];
            kNeighbors--;
            if (kNeighbors <= 0) {
                break;
            }
        }

        // 0.5 added for rounding off
        result = (long) ((biasSum / K_NEAREST_NEIGHBOR) + 0.5);
        return result;
    }

    long linearCount(int reg, long numZeros) {
        return Math.round(reg * Math.log(reg / ((double) numZeros)));
    }

    static class RawEstAndNumZeros {
        double rawEst;
        int numZeros;

        public RawEstAndNumZeros(double rawEst, int numZeros) {
            this.rawEst = rawEst;
            this.numZeros = numZeros;
        }
    }
}
