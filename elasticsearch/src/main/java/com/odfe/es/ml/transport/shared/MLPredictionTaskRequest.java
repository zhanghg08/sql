package com.odfe.es.ml.transport.shared;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
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
public class MLPredictionTaskRequest extends ActionRequest {

    String algorithm;

    String mlParameter;

    @ToString.Exclude
    List<Map<String, Object>> inputDataFrame;

    @Builder
    public MLPredictionTaskRequest(String algorithm, String mlParameter,
                                   List<Map<String, Object>> inputDataFrame) {
        this.algorithm = algorithm;
        this.mlParameter = mlParameter;
        this.inputDataFrame = inputDataFrame;
    }

    public MLPredictionTaskRequest(StreamInput in) throws IOException {
        super(in);
        this.algorithm = in.readString();
        this.mlParameter = in.readOptionalString();
        this.inputDataFrame = JsonUtil.deserialize(in.readString(), new TypeReference<List<Map<String, Object>>>(){});

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(algorithm);
        out.writeOptionalString(mlParameter);
        out.writeString(JsonUtil.serialize(inputDataFrame));
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }


}
