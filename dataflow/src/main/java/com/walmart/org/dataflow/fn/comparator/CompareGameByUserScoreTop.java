package com.walmart.org.dataflow.fn.comparator;

import com.walmart.org.dataengchallenge.GameRecord;
import org.apache.beam.sdk.transforms.SerializableComparator;

/**
 * This class is used to get the N Top Games
 * by userScore.
 */
public class CompareGameByUserScoreTop implements SerializableComparator<GameRecord> {
    @Override
    public int compare(GameRecord a, GameRecord b) {
        return a.getUserScore().compareTo(b.getUserScore());
    }
}
