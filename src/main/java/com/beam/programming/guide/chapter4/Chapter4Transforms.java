package com.beam.programming.guide.chapter4;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

import java.util.Arrays;
import java.util.List;

public class Chapter4Transforms {
    public static void main(String[] args) {
        Instant baseTime = new Instant(0);
        final List<String> LINES = Arrays.asList(
                "To be, or not to be: that is the question: ",
                "Whether 'tis nobler in the mind to suffer ",
                "The slings and arrows of outrageous fortune, ",
                "Or to take arms against a sea of troubles, ");

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);


//        PCollection<String> lines = pipeline.apply("ReadMyFile",
//                TextIO.read().from("/Users/chengzhi/Documents/Github/comchengzhibeam/src/main/resources/beam_output/output-*"));

//        PCollection<String> lines = pipeline.apply("ReadMyFile",
//                TextIO.read().from("/Users/chengzhi/Documents/Github/comchengzhibeam/src/main/resources/beam_output/output-*")
//                .watchForNewFiles(Duration.standardSeconds(30), Watch.Growth.<String>never()));

//        System.out.println(lines.isBounded());

        PCollection output = pipeline.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());
        output.apply(TextIO.write().to("/Users/chengzhi/Documents/Github/comchengzhibeam/src/main/resources/beam_output/output").withWindowedWrites().withNumShards(3));

        pipeline.run().waitUntilFinish();
    }
}
