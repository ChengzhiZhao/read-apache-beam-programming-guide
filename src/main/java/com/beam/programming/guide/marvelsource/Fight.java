package com.beam.programming.guide.marvelsource;

import java.io.Serializable;

public class Fight implements Serializable {
    private String fightUUID;
    private int player1Id;
    private int player2Id;
    private int player1CharacterId;
    private int player2CharacterId;
    private String player1CharacterName;
    private String player2CharacterName;
    private double player1SkillRate;
    private double player2SkillRate;
    private long endTimestamp;


    public String getFightUUID() {
        return fightUUID;
    }

    public String getPlayer1CharacterName() {
        return player1CharacterName;
    }

    public String getPlayer2CharacterName() {
        return player2CharacterName;
    }

    public int getPlayer1Id() {
        return player1Id;
    }

    public int getPlayer2Id() {
        return player2Id;
    }

    public int getPlayer1CharacterId() {
        return player1CharacterId;
    }

    public int getPlayer2CharacterId() {
        return player2CharacterId;
    }

    public double getPlayer1SkillRate() {
        return player1SkillRate;
    }

    public double getPlayer2SkillRate() {
        return player2SkillRate;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public Fight(String fightUUID, int player1Id, int player2Id, int player1CharacterId, int player2CharacterId, double player1SkillRate, double player2SkillRate, long endTimestamp) {
        this.fightUUID = fightUUID;
        this.player1Id = player1Id;
        this.player2Id = player2Id;
        this.player1CharacterId = player1CharacterId;
        this.player2CharacterId = player2CharacterId;
        this.player1SkillRate = player1SkillRate;
        this.player2SkillRate = player2SkillRate;
        this.endTimestamp = endTimestamp;
    }
}
