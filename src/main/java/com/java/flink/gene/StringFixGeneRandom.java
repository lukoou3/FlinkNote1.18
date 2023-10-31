package com.java.flink.gene;

import java.util.concurrent.ThreadLocalRandom;

public class StringFixGeneRandom extends AbstractFieldGene<String>{
    static final char[] charArray;
    private int len;
    private char[] buf;

    static {
        charArray = new char[26 * 2 + 10 + 4];
        int idx = 0;
        for (int i = 48; i <= 57; i++) {
            charArray[idx++] = (char) i;
        }
        for (int i = 65; i <= 90; i++) {
            charArray[idx++] = (char) i;
        }
        for (int i = 97; i <= 122; i++) {
            charArray[idx++] = (char) i;
        }

        charArray[idx++] = '-';
        charArray[idx++] = '_';
        charArray[idx++] = '#';
        charArray[idx++] = '*';
    }

    public StringFixGeneRandom(String fieldName, int len) {
        super(fieldName);
        this.len = len;
        this.buf = new char[len];
    }

    @Override
    public String geneValue() throws Exception {
        if(len == 0 ){
            return "";
        }
        for (int i = 0; i < len; i++) {
            buf[i] = charArray[ThreadLocalRandom.current().nextInt(charArray.length)];
        }
        return new String(buf);
    }

    public static void main(String[] args) throws Exception {
        StringFixGeneRandom gene = new StringFixGeneRandom("name", 10);
        for (int i = 0; i < 20; i++) {
            System.out.println(gene.geneValue());
        }
        System.out.println(new String(charArray));

        gene = new StringFixGeneRandom("name", 0);
        for (int i = 0; i < 10; i++) {
            System.out.println(gene.geneValue());
        }
    }
}
