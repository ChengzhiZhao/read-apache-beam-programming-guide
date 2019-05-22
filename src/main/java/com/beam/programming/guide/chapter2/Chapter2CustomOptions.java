package com.beam.programming.guide.chapter2;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class Chapter2CustomOptions {
    public interface MyFirstCustomOption extends PipelineOptions {
        @Description("The signature of the pipeline")
        @Default.String("Hulk")
        String getSignature();
        void setSignature(String signature);
    }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(MyFirstCustomOption.class);
        MyFirstCustomOption option = PipelineOptionsFactory.fromArgs(args).as(MyFirstCustomOption.class);
        System.out.println(option.getSignature());
        System.out.println(option.getRunner().getName());
    }
}
