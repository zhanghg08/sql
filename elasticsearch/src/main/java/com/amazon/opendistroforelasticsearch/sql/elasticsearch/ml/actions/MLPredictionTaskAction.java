package com.amazon.opendistroforelasticsearch.sql.elasticsearch.ml.actions;

import org.elasticsearch.action.ActionType;

public class MLPredictionTaskAction extends ActionType<MLPredictionTaskResponse> {
    public static final MLPredictionTaskAction INSTANCE = new MLPredictionTaskAction();
    public static final String NAME = "cluster:admin/odfe-ml/predict";

    private MLPredictionTaskAction() {
        super(NAME, MLPredictionTaskResponse::new);
    }
}
