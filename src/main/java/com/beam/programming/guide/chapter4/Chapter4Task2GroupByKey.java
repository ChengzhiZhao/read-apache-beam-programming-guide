package com.beam.programming.guide.chapter4;

import com.beam.programming.guide.marvelsource.Fight;
import com.google.gson.Gson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.io.File;

public class Chapter4Task2GroupByKey {
    static Gson gson = new Gson();

    static class GetOpponentsList extends DoFn<KV<Integer,Iterable<Fight>>, String>{
        @ProcessElement
        public void processElement(@Element KV<Integer,Iterable<Fight>> fightsList, OutputReceiver<String> out){
            String opponentsList = "";
            for (Fight fight: fightsList.getValue()){
                opponentsList += Integer.toString(fight.getPlayer2Id()) + ",";
            }
            String re = Integer.toString(fightsList.getKey()) + " | " + opponentsList.substring(0, opponentsList.length()-1);
            out.output(re);
        }
    }

    static class ParseJSONToKVFightFn extends DoFn<String, KV<Integer, Fight>> {
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<KV<Integer, Fight>> out){
            Fight fight = gson.fromJson(line, Fight.class);
            out.output(KV.of(fight.getPlayer1Id(), fight));
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        String sourcePath = new File("src/main/resources/marvel_data/fights").getPath();

        PCollection<String> fightsData = pipeline.apply("readMarvelFights",
                TextIO.read().from(sourcePath + "/fight-*")
                        .watchForNewFiles(Duration.standardSeconds(5), Watch.Growth.<String>never()));

        PCollection<KV<Integer,Fight>> fights = fightsData
                .apply("ParseJSONStringToFightFn", ParDo.of(new ParseJSONToKVFightFn()))
                .apply(Window.<KV<Integer,Fight>>into(FixedWindows.of(Duration.standardSeconds(5))));


        PCollection<KV<Integer,Iterable<Fight>>> fightsGroupByKey = fights.apply("FightsGroupByKey", GroupByKey.<Integer, Fight>create());

        PCollection<String> playerWithOpponentsList = fightsGroupByKey.apply("GetOpponentsList", ParDo.of(new GetOpponentsList()));

        PCollection<String> fixedWindowedLines=playerWithOpponentsList.apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(10))));

        String outputPath = new File("src/main/resources/beam_output/").getPath();
        fixedWindowedLines.apply(TextIO.write().to(outputPath+"/output").withWindowedWrites().withNumShards(3));

        pipeline.run().waitUntilFinish();
    }
}
