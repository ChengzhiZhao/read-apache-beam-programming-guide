package com.beam.programming.guide.chapter3;

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

public class Chapter3PCollections {
    public static void main(String[] args) {
        Instant baseTime = new Instant(0);
        final List<String> LINES = Arrays.asList(
                "To be, or not to be: that is the question: ",
                "Whether 'tis nobler in the mind to suffer ",
                "The slings and arrows of outrageous fortune, ",
                "Or to take arms against a sea of troubles, ");

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection output = pipeline.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());
        output.apply(TextIO.write().to("src/main/resources/beam_output/output").withWindowedWrites().withNumShards(3));

        pipeline.run().waitUntilFinish();
    }
}
