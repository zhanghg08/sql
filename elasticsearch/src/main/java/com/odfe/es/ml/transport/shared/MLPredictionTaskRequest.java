package com.odfe.es.ml.transport.shared;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import com.amazon.opendistroforelasticsearch.sql.elasticsearch.ml.JsonUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;

@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@ToString
public class MLPredictionTaskRequest extends ActionRequest {

    String algorithm;

    String mlParameter;

    String modelId;

    @ToString.Exclude
    List<Map<String, Object>> inputDataFrame;

    @Builder
    public MLPredictionTaskRequest(String algorithm, String mlParameter,
                                   String modelId, List<Map<String, Object>> inputDataFrame) {
        this.algorithm = algorithm;
        this.mlParameter = mlParameter;
        this.modelId = modelId;
        this.inputDataFrame = inputDataFrame;
    }

    public MLPredictionTaskRequest(StreamInput in) throws IOException {
        super(in);
        this.algorithm = in.readString();
        this.mlParameter = in.readOptionalString();
        this.modelId = in.readOptionalString();
        this.inputDataFrame = JsonUtil.deserialize(in.readString(), new TypeReference<List<Map<String, Object>>>(){});

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(algorithm);
        out.writeOptionalString(mlParameter);
        out.writeOptionalString(modelId);
        out.writeString(JsonUtil.serialize(inputDataFrame));
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }


    public static MLPredictionTaskRequest fromActionRequest(ActionRequest actionRequest) {
         if (actionRequest instanceof MLPredictionTaskRequest) {
             return (MLPredictionTaskRequest) actionRequest;
         }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionRequest.writeTo(osso);
            try (StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                return new MLPredictionTaskRequest(input);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to parse ActionRequest into MLPredictionTaskRequest", e);
        }

    }
}
