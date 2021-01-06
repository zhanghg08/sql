package com.amazon.opendistroforelasticsearch.sql.elasticsearch.ml.actions;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import com.amazon.opendistroforelasticsearch.sql.elasticsearch.ml.JsonUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import lombok.extern.log4j.Log4j2;

@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@ToString
public class MLPredictionTaskResponse  extends ActionResponse {
    String taskId;

    String status;

    List<Map<String, Object>>  predictionResult;

    @Builder
    public MLPredictionTaskResponse(String taskId, String status, List<Map<String, Object>> predictionResult) {
        this.taskId = taskId;
        this.status = status;
        this.predictionResult = predictionResult;
    }

    public MLPredictionTaskResponse(StreamInput in) throws IOException {
        super(in);
        this.taskId = in.readString();
        this.status = in.readString();
        this.predictionResult =
                JsonUtil.deserialize(in.readString(), new TypeReference<List<Map<String, Object>>>(){});
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(taskId);
        out.writeString(status);
        out.writeString(JsonUtil.serialize(predictionResult));
    }
}
