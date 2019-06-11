package com.beam.programming.guide.chapter3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class Chapter3PCollectionsInMemory {
    public static void main(String[] args) {
        final List<String> LINES = Arrays.asList(
                "To be, or not to be: that is the question: ",
                "Whether 'tis nobler in the mind to suffer ",
                "The slings and arrows of outrageous fortune, ",
                "Or to take arms against a sea of troubles, ");

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> input = pipeline.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());
        String outputPath = new File("src/main/resources/beam_output/").getPath();
        //You can control the number of output by specify the using .withNumShards(2)
        input.apply(TextIO.write().to(outputPath + "/output-"));
        pipeline.run().waitUntilFinish();

        System.out.println(input.isBounded());
    }
}
