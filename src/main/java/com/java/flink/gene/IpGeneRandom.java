package com.java.flink.gene;

import java.util.concurrent.ThreadLocalRandom;

public class IpGeneRandom extends AbstractFieldGene<String>{
    private long start;
    private long end;
    private double nullRatio;
    private boolean nullAble;
    private StringBuilder sb = new StringBuilder(16);

    public IpGeneRandom(String fieldName, long start, long end, double nullRatio) {
        super(fieldName);
        this.start = start;
        this.end = end;
        this.nullRatio = nullRatio;
        this.nullAble = nullRatio > 0D;
    }

    @Override
    public String geneValue() throws Exception {
        if(nullAble && ThreadLocalRandom.current().nextDouble() < nullRatio){
            return null;
        }else{
            return longToIp(ThreadLocalRandom.current().nextLong(start, end));
        }
    }

    private String longToIp(long ipLong) {
        //sb.delete(0, sb.length());
        sb.setLength(0);

        sb.append(ipLong >>> 24).append(".");
        sb.append((ipLong >>> 16) & 0xFF).append(".");
        sb.append((ipLong >>> 8) & 0xFF).append(".");
        sb.append(ipLong & 0xFF);

        return sb.toString();
    }
}
