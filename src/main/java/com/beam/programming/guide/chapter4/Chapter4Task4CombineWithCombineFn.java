package com.beam.programming.guide.chapter4;

import com.beam.programming.guide.marvelsource.Fight;
import com.google.gson.Gson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.io.File;
import java.io.Serializable;

public class Chapter4Task4CombineWithCombineFn {
    static Gson gson = new Gson();

    static class MeanFn extends Combine.CombineFn<Double, MeanFn.Accum, Double> {
        public static class Accum implements Serializable {
            double sum = 0;
            int count = 0;
        }

        @Override
        public Accum createAccumulator() { return new Accum(); }

        @Override
        public Accum addInput(Accum accum, Double input) {
            accum.sum += input;
            accum.count++;
            return accum;
        }

        @Override
        public Accum mergeAccumulators(Iterable<Accum> accumulators) {
            Accum merged = createAccumulator();
            for (Accum accum : accumulators) {
                merged.sum += accum.sum;
                merged.count += accum.count;
            }
            return merged;
        }

        @Override
        public Double extractOutput(Accum accumulator) {
            return accumulator.sum/accumulator.count;
        }

    }

    static class ParseJSONToKVFightFn extends DoFn<String, KV<Integer, Double>> {
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<KV<Integer, Double>> out){
            Fight fight = gson.fromJson(line, Fight.class);
            out.output(KV.of(fight.getPlayer1Id(),fight.getPlayer1SkillRate()));
        }
    }

    static class ParseFightSkillRateToJSONStringFn extends DoFn<KV<Integer, Double>, String>{
        @ProcessElement
        public void processElement(@Element KV<Integer, Double> fight, OutputReceiver<String> out){
            out.output(fight.getKey().toString() + " | " + fight.getValue().toString());
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        String sourcePath = new File("src/main/resources/marvel_data/fights").getPath();

        PCollection<String> fightsData = pipeline.apply("readMarvelFights",
                TextIO.read().from(sourcePath + "/fight-*")
                        .watchForNewFiles(Duration.standardSeconds(5), Watch.Growth.<String>never()));

        PCollection<KV<Integer, Double>> fights = fightsData
                .apply("ParseJSONStringToFightFn", ParDo.of(new ParseJSONToKVFightFn()))
                .apply(Window.<KV<Integer,Double>>into(FixedWindows.of(Duration.standardSeconds(5))));

        PCollection<KV<Integer, Double>> fightsGroup = fights
                .apply("MeanFn", Combine.<Integer, Double, Double>perKey(new MeanFn()));

        PCollection<String> fightSums = fightsGroup
                .apply("ParseFightSkillRateToJSONStringFn", ParDo.of(new ParseFightSkillRateToJSONStringFn()));

        String outputPath = new File("src/main/resources/beam_output/").getPath();
        fightSums.apply(TextIO.write().to(outputPath+"/output").withWindowedWrites().withNumShards(3));

        pipeline.run().waitUntilFinish();
    }
}
