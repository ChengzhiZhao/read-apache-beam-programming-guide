package com.beam.programming.guide.marvelsource;

import java.io.Serializable;

public class EnrichFight implements Serializable {
    private String fightUUID;
    private int player1Id;

    public void setPlayer1CharacterName(String player1CharacterName) {
        this.player1CharacterName = player1CharacterName;
    }

    public void setPlayer2CharacterName(String player2CharacterName) {
        this.player2CharacterName = player2CharacterName;
    }

    public EnrichFight(String fightUUID, int player1Id, int player2Id, int player1CharacterId, int player2CharacterId, String player1CharacterName, String player2CharacterName, double player1SkillRate, double player2SkillRate, long endTimestamp) {
        this.fightUUID = fightUUID;
        this.player1Id = player1Id;
        this.player2Id = player2Id;
        this.player1CharacterId = player1CharacterId;

        this.player2CharacterId = player2CharacterId;
        this.player1CharacterName = player1CharacterName;
        this.player2CharacterName = player2CharacterName;
        this.player1SkillRate = player1SkillRate;
        this.player2SkillRate = player2SkillRate;
        this.endTimestamp = endTimestamp;
    }

    public String getFightUUID() {
        return fightUUID;
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

    public String getPlayer1CharacterName() {
        return player1CharacterName;
    }

    public String getPlayer2CharacterName() {
        return player2CharacterName;
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

    private int player2Id;
    private int player1CharacterId;
    private int player2CharacterId;
    private String player1CharacterName;
    private String player2CharacterName;
    private double player1SkillRate;
    private double player2SkillRate;
    private long endTimestamp;
}
