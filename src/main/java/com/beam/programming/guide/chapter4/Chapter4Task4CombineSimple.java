package com.beam.programming.guide.chapter4;

import com.beam.programming.guide.marvelsource.Fight;
import com.google.gson.Gson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.io.File;

public class Chapter4Task4CombineSimple {
    static Gson gson = new Gson();

    static class SumDoubles implements SerializableFunction<Iterable<Double>, Double> {
        @Override
        public Double apply(Iterable<Double> fights) {
            double sum = 0.0;
            for (Double fightPoint: fights){
                sum += fightPoint;
            }
            return sum;
        }
    }

    static class ParseJSONToKVFightFn extends DoFn<String, Double> {
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<Double> out){
            Fight fight = gson.fromJson(line, Fight.class);
            out.output((fight.getPlayer1SkillRate()+fight.getPlayer2SkillRate())/2);
        }
    }

    static class ParseFightToJSONStringFn extends DoFn<Double, String>{
        @ProcessElement
        public void processElement(@Element Double fight, OutputReceiver<String> out){
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

        PCollection<Double> fights = fightsData.apply("ParseJSONStringToFightFn", ParDo.of(new ParseJSONToKVFightFn()))
                .apply(Window.<Double>into(FixedWindows.of(Duration.standardSeconds(10))));

        PCollection<String> fightsSum = fights.apply(Combine.globally(new SumDoubles()).withoutDefaults())
                                              .apply(ParDo.of(new ParseFightToJSONStringFn()));

        String outputPath = new File("src/main/resources/beam_output/").getPath();
        fightsSum.apply(TextIO.write().to(outputPath+"/output").withWindowedWrites().withNumShards(3));

        pipeline.run().waitUntilFinish();
    }
}
