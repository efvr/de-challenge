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
    public void processElement(ProcessContext c) {
        List<Console> sideInput = c.sideInput(this.view);
        GameRecord gameRecord = new GameRecord();

        if(sideInput != null){
            gameRecord.setConsole(Objects.requireNonNull(c.element()).getConsole());
            gameRecord.setCompany(Utils.getCompanyName(sideInput,Objects.requireNonNull(c.element()).getConsole()));
            gameRecord.setDate(Objects.requireNonNull(c.element()).getDate());
            gameRecord.setMetascore(Objects.requireNonNull(c.element()).getMetascore());
            gameRecord.setName(Objects.requireNonNull(c.element()).getName());
            gameRecord.setUserscore(Objects.requireNonNull(c.element()).getUserscore());
        }

        c.output(gameRecord);
    }



}
