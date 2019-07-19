package com.beam.programming.guide.chapter4;

import com.beam.programming.guide.marvelsource.Fight;
import com.google.gson.Gson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;

import java.io.File;

public class Chapter4Task6Partition {
    static Gson gson = new Gson();

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

        PCollection<Fight> fights = fightsData
                .apply("ParseJSONStringToFightFn", ParDo.of(new ParseJSONStringToFightFn()))
                .apply(Window.<Fight>into(FixedWindows.of(Duration.standardSeconds(5))));

        PCollectionList<Fight> topFights = fights.apply(Partition.of(5, new Partition.PartitionFn<Fight>() {
            public int partitionFor(Fight fight, int numPartitions){ return (int)(fight.getPlayer1SkillRate() * 100/2)*numPartitions/100;}
        }));

        PCollection<String> topFightsOutput = topFights.get(4).apply("ParseFightToJSONStringFn",ParDo.of(new ParseFightToJSONStringFn()));

        String outputPath = new File("src/main/resources/beam_output/").getPath();
        topFightsOutput.apply(TextIO.write().to(outputPath+"/output").withWindowedWrites().withNumShards(3));

        pipeline.run().waitUntilFinish();
    }
}
