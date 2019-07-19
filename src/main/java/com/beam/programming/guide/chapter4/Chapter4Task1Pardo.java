package com.beam.programming.guide.chapter4;

import com.beam.programming.guide.marvelsource.Fight;
import com.google.gson.Gson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.io.File;

public class Chapter4Task1Pardo {
    static Gson gson = new Gson();

    static class FilterOddIDFn extends DoFn<Fight, Fight>{
        @ProcessElement
        public void processElement(@Element Fight fight, OutputReceiver<Fight> out){
            if (fight.getPlayer2Id() % 2 == 1){
                out.output(fight);
            }
        }
    }

    static class ParseJSONStringToFightFn extends DoFn<String, Fight>{
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<Fight> out){
            Fight fight = gson.fromJson(line, Fight.class);
            out.output(fight);
        }
    }

    static class ParseFightToJSONStringFn extends DoFn<Fight, String>{
        @ProcessElement
        public void processElement(@Element Fight fight, OutputReceiver<String> out){
            String json = gson.toJson(fight);
            out.output(json);
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        String sourcePath = new File("src/main/resources/marvel_data/fights").getPath();

        PCollection<String> fightsData = pipeline.apply("readMarvelFights",
                TextIO.read().from(sourcePath + "/fight-*")
                        .watchForNewFiles(Duration.standardSeconds(5), Watch.Growth.<String>never()));

        PCollection<Fight> fights = fightsData.apply("ParseJSONStringToFightFn", ParDo.of(new ParseJSONStringToFightFn()));

        PCollection<Fight> fightsOddPlayer2 = fights.apply("FilterOddPlayer2", ParDo.of(new FilterOddIDFn()));

        PCollection<String> fightsJson = fightsOddPlayer2.apply("ParseFightToJSONStringFn", ParDo.of(new ParseFightToJSONStringFn())).setCoder(StringUtf8Coder.of());

        PCollection<String> fixedWindowedLines=fightsJson.apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(10))));

        String outputPath = new File("src/main/resources/beam_output/").getPath();
        fixedWindowedLines.apply(TextIO.write().to(outputPath+"/output").withWindowedWrites().withNumShards(3));

        pipeline.run().waitUntilFinish();
    }
}
