package com.xxxx.kafkastream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @author 朱佳睿
 * @date 2020/4/19
 */
public class LogProcessor implements Processor<byte[],byte[]> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public void process(byte[] dummy, byte[] line) {
        String input = new String(line);
        String perfix = "PRODUCT_RATING_PREFIX:";
        // 根据前缀过滤日志信息，提取后面的内容
        if(input.contains(perfix)){
            input = input.split(perfix)[1].trim();
            System.out.print(input);
            context.forward("logProcessor".getBytes(), input.getBytes());
        }

    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}

