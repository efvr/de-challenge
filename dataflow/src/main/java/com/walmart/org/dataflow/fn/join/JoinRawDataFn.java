package com.walmart.org.dataflow.fn.join;

import com.walmart.org.dataengchallenge.GameRecord;
import com.walmart.org.dataflow.objects.Console;
import com.walmart.org.dataflow.objects.Result;
import com.walmart.org.dataflow.utils.Utils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.List;
import java.util.Objects;

/**
 * This class is used to join the input
 * from Result data and Console data.
 * This class returns a new GameRecord object.
 */
public class JoinRawDataFn extends DoFn<Result, GameRecord> {

    final PCollectionView<List<Console>> view;

    /**
     *
     * @param view This view corresponds to the console data.
     */
    public JoinRawDataFn(PCollectionView<List<Console>> view) {
        this.view = view;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        List<Console> sideInput = c.sideInput(this.view);
        GameRecord gameRecord = new GameRecord();

        try {
            if (sideInput.size() > 0) {
                gameRecord.setConsole(Objects.requireNonNull(c.element()).getConsole().trim());
                if(!gameRecord.getConsole().isEmpty()){
                    gameRecord.setCompany(Utils.getCompanyName(sideInput, gameRecord.getConsole()));
                }
                gameRecord.setDate(Objects.requireNonNull(c.element()).getDate());
                gameRecord.setMetaScore(Objects.requireNonNull(c.element()).getMetascore());
                gameRecord.setName(Objects.requireNonNull(c.element()).getName());
                gameRecord.setUserScore(Objects.requireNonNull(c.element()).getUserscore());

                c.output(gameRecord);
            }
        } catch (Exception e){
            throw new Exception("Please check the record of this game: ["+ c.element().getName()+"]");
        }
    }
}
