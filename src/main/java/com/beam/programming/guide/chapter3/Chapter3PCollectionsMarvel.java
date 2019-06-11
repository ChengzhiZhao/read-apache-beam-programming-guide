package com.beam.programming.guide.chapter3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.io.File;

public class Chapter3PCollectionsMarvel {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        String sourcePath = new File("src/main/resources/marvel_data/fights").getPath();

        PCollection<String> lines = pipeline.apply("readMarvelFights",
                TextIO.read().from(sourcePath + "/fight-*")
                        .watchForNewFiles(Duration.standardSeconds(5), Watch.Growth.<String>never()));

        PCollection<String> fixedWindowedLines=lines.apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(10))));

        String outputPath = new File("src/main/resources/beam_output/").getPath();
        fixedWindowedLines.apply(TextIO.write().to(outputPath+"/output").withWindowedWrites().withNumShards(3));

        pipeline.run().waitUntilFinish();
    }
}
