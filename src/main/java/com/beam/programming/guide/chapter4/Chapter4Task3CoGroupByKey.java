package com.beam.programming.guide.chapter4;

import com.beam.programming.guide.marvelsource.Character;
import com.beam.programming.guide.marvelsource.EnrichFight;
import com.beam.programming.guide.marvelsource.Fight;
import com.beam.programming.guide.utils.Tuple2;
import com.google.gson.Gson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;

import java.io.File;

public class Chapter4Task3CoGroupByKey {
    static Gson gson = new Gson();
    final static TupleTag<Character> characterTag = new TupleTag<>();
    final static TupleTag<Tuple2<Integer,Fight>> fightTag = new TupleTag<>();

    static class EnrichFightFn extends DoFn<KV<Integer,CoGbkResult>, KV<String, EnrichFight>>{
        @ProcessElement
        public void processElement(ProcessContext c){
            KV<Integer,CoGbkResult> element = c.element();
            Iterable<Character> characterIter = element.getValue().getAll(characterTag);
            Iterable<Tuple2<Integer,Fight>> fightIter = element.getValue().getAll(fightTag);
            Character currentCharacter = characterIter.iterator().next();

            for (Tuple2<Integer,Fight> f: fightIter){
                EnrichFight fight = gson.fromJson(gson.toJson(f.b), EnrichFight.class);
                if (f.a == 1){
                    fight.setPlayer1CharacterName(currentCharacter.getName());
                }
                else {
                    fight.setPlayer2CharacterName(currentCharacter.getName());
                }
                c.output(KV.of(fight.getFightUUID(), fight));
            }
        }
    }

    static class ParseJSONToKVFightFn extends DoFn<String, KV<Integer, Tuple2<Integer,Fight>>> {
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<KV<Integer, Tuple2<Integer,Fight>>> out) {
            Fight fight = gson.fromJson(line, Fight.class);
            out.output(KV.of(fight.getPlayer1CharacterId(), new Tuple2<>(1, fight)));
            out.output(KV.of(fight.getPlayer2CharacterId(), new Tuple2<>(2, fight)));
        }
    }

    static class ParseJSONToKVCharacterFn extends DoFn<String, KV<Integer, Character>> {
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<KV<Integer, Character>> out){
            Character character = gson.fromJson(line, Character.class);
            out.output(KV.of(character.getId(), character));
        }
    }

    static class MergeAndFormatFight extends DoFn<KV<String, Iterable<EnrichFight>>, String>{
        @ProcessElement
        public void processElement(@Element KV<String, Iterable<EnrichFight>> fights, OutputReceiver<String> out){
             String player1CharacterName = "";
             String player2CharacterName = "";
             EnrichFight enrichfight = null;
             for (EnrichFight fight: fights.getValue()){
                 if (fight.getPlayer1CharacterName() != null){
                     enrichfight = fight;
                     player1CharacterName = fight.getPlayer1CharacterName();
                 }
                 else {
                     enrichfight = fight;
                     player2CharacterName = fight.getPlayer2CharacterName();
                 }
             }
             if (enrichfight != null) {
                 EnrichFight fight = gson.fromJson(gson.toJson(enrichfight), EnrichFight.class);
                 fight.setPlayer1CharacterName(player1CharacterName);
                 fight.setPlayer2CharacterName(player2CharacterName);
                 out.output(gson.toJson(fight));
             }
        }
    }

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        String sourcePath = new File("src/main/resources/marvel_data/fights").getPath();

        PCollection<String> fightsData = pipeline.apply("readMarvelFights",
                TextIO.read().from(sourcePath + "/fight-*")
                        .watchForNewFiles(Duration.standardSeconds(3), Watch.Growth.<String>never()));

        PCollection<KV<Integer,Tuple2<Integer,Fight>>> fights = fightsData
                .apply("ParseJSONStringToFightFn", ParDo.of(new ParseJSONToKVFightFn()))
                .apply(Window.<KV<Integer,Tuple2<Integer,Fight>>>into(FixedWindows.of(Duration.standardSeconds(5))));

        String marvelCharactersPath = new File("src/main/resources/marvel_data/").getPath();
        PCollection<String> CharactersData = pipeline.apply("readMarvelCharacters",
                TextIO.read().from(marvelCharactersPath + "/marvel_characters.json")
                .watchForNewFiles(Duration.standardSeconds(3), Watch.Growth.<String>never()));

        PCollection<KV<Integer,Character>> characters = CharactersData
                .apply("ParseJSONToKVCharacterFn", ParDo.of(new ParseJSONToKVCharacterFn()))
                .apply(Window.<KV<Integer,Character>>into(FixedWindows.of(Duration.standardSeconds(5))));

        PCollection<KV<Integer, CoGbkResult>> enrichedResult = KeyedPCollectionTuple
                .of(fightTag, fights)
                .and(characterTag, characters)
                .apply(CoGroupByKey.<Integer>create());

        //Transform to UUID, Fight
        PCollection<KV<String, EnrichFight>> enrichedFights = enrichedResult.apply("EnrichFightFn", ParDo.of(new EnrichFightFn()));

        PCollection<KV<String, Iterable<EnrichFight>>> enrichedFinalFights = enrichedFights
                .apply("GroupByKey", GroupByKey.<String, EnrichFight>create());

        PCollection<String> output = enrichedFinalFights
                .apply("MergeAndFormatFight", ParDo.of(new MergeAndFormatFight()))
                .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(5))));

        String outputPath = new File("src/main/resources/beam_output/").getPath();
        output.apply(TextIO.write().to(outputPath+"/output").withWindowedWrites().withNumShards(3));

        pipeline.run().waitUntilFinish();
    }
}
