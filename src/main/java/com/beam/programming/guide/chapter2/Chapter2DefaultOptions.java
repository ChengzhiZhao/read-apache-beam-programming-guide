package com.beam.programming.guide.chapter2;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class Chapter2DefaultOptions {
    public static void main(String[] args) {
        //let's check the default options
        PipelineOptions options = PipelineOptionsFactory.create();
        System.out.println("Runner: " + options.getRunner().getName());
        System.out.println("JobName: " + options.getJobName());
        System.out.println("OptionID: " + options.getOptionsId());
        System.out.println("StableUniqueName: " + options.getStableUniqueNames());
        System.out.println("TempLocation: " + options.getTempLocation());
        System.out.println("UserAgent: " + options.getUserAgent());
    }
}
