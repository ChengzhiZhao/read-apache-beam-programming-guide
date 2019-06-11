package com.beam.programming.guide.marvelsource;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class MarvelSource {
    final static int minPlayerID = 1;
    final static int maxPlayerID = 100;
    final static int minPlayerCharacterID = 1;
    final static int maxPlayerCharacterID = 25;
    final static double minPlayerSkillRate = 0.1;
    final static double maxPlayerSkillRate = 2.0;
    static Gson gson = new GsonBuilder().create();


    public static Fight generateFight(){
        String fightUUID = UUID.randomUUID().toString();
        int player1Id = ThreadLocalRandom.current().nextInt(minPlayerID, maxPlayerID + 1);
        int player2Id = ThreadLocalRandom.current().nextInt(minPlayerID, maxPlayerID + 1);
        int player1CharacterId = ThreadLocalRandom.current().nextInt(minPlayerCharacterID, maxPlayerCharacterID + 1);
        int player2CharacterId = ThreadLocalRandom.current().nextInt(minPlayerCharacterID, maxPlayerCharacterID + 1);
        double player1SkillRate = ThreadLocalRandom.current().nextDouble(minPlayerSkillRate, maxPlayerSkillRate);
        double player2SkillRate = ThreadLocalRandom.current().nextDouble(minPlayerSkillRate, maxPlayerSkillRate);
        long endTimestamp = System.currentTimeMillis();

        Fight fight = new Fight(fightUUID, player1Id, player2Id, player1CharacterId, player2CharacterId, player1SkillRate, player2SkillRate, endTimestamp);

        return fight;
    }

    public static List<Fight> generateFights(int minNumberFights, int maxNumberFights){
        int numberOfFights = ThreadLocalRandom.current().nextInt(minNumberFights, maxNumberFights + 1);
        List<Fight> fights = new ArrayList<Fight>();
        for (int i=1; i<numberOfFights; i++){
            fights.add(generateFight());
        }
        return fights;
    }

    public static void generateFightsJSONByInterval(int interval, int duration, String outputPath, int minNumberFights, int maxNumberFights){
        String prefix = "/fight-";
        long currentTime = 0;
        try {
            while (duration == -1 || currentTime<duration){
                List<Fight> fights = generateFights(minNumberFights, maxNumberFights);
                Writer write = new FileWriter(outputPath + prefix + UUID.randomUUID());
                for (Fight fight: fights){
                    gson.toJson(fight, write);
                    write.write("\n");
                }
                write.flush();
                write.close();
                currentTime += 1000;
                Thread.sleep(interval);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String outputPath = new File("src/main/resources/marvel_data/fights/").getPath();
        int minNumberFights = 50;
        int maxNumberFights = 100;
        int interval = 1000;
        int duration = 5000;
        for (String arg: args){
            if (arg.startsWith("--min_fights")) {
                minNumberFights = Integer.parseInt(arg.split("=")[1]);
            }
            if (arg.startsWith("--max_fights")) {
                maxNumberFights = Integer.parseInt(arg.split("=")[1]);
            }
            if (arg.startsWith("--interval")) {
                interval = Integer.parseInt(arg.split("=")[1]);
            }
            if (arg.startsWith("--duration")) {
                duration = Integer.parseInt(arg.split("=")[1]);
            }
        }

        generateFightsJSONByInterval(interval, duration, outputPath, minNumberFights, maxNumberFights);
    }
}
