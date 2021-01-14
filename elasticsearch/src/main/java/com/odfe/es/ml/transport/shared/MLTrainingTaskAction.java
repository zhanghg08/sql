package com.odfe.es.ml.transport.shared;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
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

public class MLTrainingTaskAction extends ActionType<MLTrainingTaskAction.MLTrainingTaskResponse> {
    public static MLTrainingTaskAction INSTANCE = new MLTrainingTaskAction();
    public static final String NAME = "cluster:admin/odfe-ml/training";

    private MLTrainingTaskAction() {
        super(NAME, MLTrainingTaskResponse::new);
    }

    @Getter
    @FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
    @ToString
    public static class MLTrainingTaskRequest extends ActionRequest {
        String algorithm;

        String mlParameter;

        @ToString.Exclude
        List<Map<String, Object>> inputDataFrame;

        @Builder
        public MLTrainingTaskRequest(String algorithm, String mlParameter, List<Map<String, Object>> inputDataFrame) {
            this.algorithm = algorithm;
            this.mlParameter = mlParameter;
            this.inputDataFrame = inputDataFrame;
        }

        public MLTrainingTaskRequest(StreamInput in) throws IOException {
            super(in);
            this.algorithm = in.readString();
            this.mlParameter = in.readOptionalString();
            this.inputDataFrame = JsonUtil.deserialize(in.readString(), new TypeReference<List<Map<String, Object>>>(){});
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(algorithm);
            out.writeOptionalString(mlParameter);
            out.writeString(JsonUtil.serialize(inputDataFrame));
        }

        public static MLTrainingTaskRequest fromActionRequest(ActionRequest actionRequest) {
            if (actionRequest instanceof MLTrainingTaskRequest) {
                return (MLTrainingTaskRequest) actionRequest;
            }

            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
                actionRequest.writeTo(osso);
                try (StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                    return new MLTrainingTaskRequest(input);
                }
            } catch (IOException e) {
                throw new UncheckedIOException("failed to parse ActionRequest into MLTrainingTaskRequest", e);
            }

        }
    }

    @Getter
    @FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
    @ToString
    public static class MLTrainingTaskResponse extends ActionResponse {
        String status;
        String taskId;

        @Builder
        public MLTrainingTaskResponse(String status, String taskId) {
            this.status = status;
            this.taskId = taskId;
        }

        public MLTrainingTaskResponse(StreamInput in) throws IOException {
            this.status = in.readString();
            this.taskId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(status);
            out.writeString(taskId);
        }

        public static MLTrainingTaskResponse fromActionResponse(ActionResponse actionResponse) {
            if (actionResponse instanceof MLTrainingTaskResponse) {
                return (MLTrainingTaskResponse) actionResponse;
            }

            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
                actionResponse.writeTo(osso);
                try (StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                    return new MLTrainingTaskResponse(input);
                }
            } catch (IOException e) {
                throw new UncheckedIOException("failed to parse ActionRequest into MLTrainingTaskResponse", e);
            }
        }
    }
}
