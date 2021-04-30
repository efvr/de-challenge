package com.walmart.org.dataflow.fn;

import com.walmart.org.dataflow.objects.Console;
import com.walmart.org.dataflow.objects.GameRecord;
import com.walmart.org.dataflow.objects.Result;
import com.walmart.org.dataflow.utils.Utils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.List;
import java.util.Objects;

public class JoinDataFn extends DoFn<Result, GameRecord> {

    final PCollectionView<List<Console>> view;

    public JoinDataFn(PCollectionView<List<Console>> view) {
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
                gameRecord.setMetascore(Objects.requireNonNull(c.element()).getMetascore());
                gameRecord.setName(Objects.requireNonNull(c.element()).getName());
                gameRecord.setUserscore(Objects.requireNonNull(c.element()).getUserscore());

                c.output(gameRecord);
            }
        } catch (Exception e){
            throw new Exception("Please check the record of this game: ["+ c.element().getName()+"]");
        }
    }
}
