package com.walmart.org.dataflow.fn.comparator;

import com.walmart.org.dataengchallenge.GameRecord;
import org.apache.beam.sdk.transforms.SerializableComparator;

/**
 * This class is used to get the N Worst Games
 * by userScore.
 */
public class CompareGameByUserScoreWorst implements SerializableComparator<GameRecord> {
    @Override
    public int compare(GameRecord a, GameRecord b) {
        return b.getUserScore().compareTo(a.getUserScore());
    }
}
