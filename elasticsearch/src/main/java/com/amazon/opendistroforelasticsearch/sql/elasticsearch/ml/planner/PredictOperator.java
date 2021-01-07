/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.elasticsearch.ml.planner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprDoubleValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprIntegerValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprLongValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprStringValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprTupleValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.client.ElasticsearchClient;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.ml.JsonUtil;
import com.odfe.es.ml.transport.shared.MLPredictionTaskRequest;
import com.amazon.opendistroforelasticsearch.sql.planner.physical.PhysicalPlan;
import com.amazon.opendistroforelasticsearch.sql.planner.physical.PhysicalPlanNodeVisitor;
import com.google.common.collect.ImmutableMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

@Getter
@EqualsAndHashCode
public class PredictOperator extends PhysicalPlan {

  @Getter
  private final PhysicalPlan input;
  @Getter
  private final String algo;
  @Getter
  private final String args;

  private final ElasticsearchClient elasticsearchClient;

  @EqualsAndHashCode.Exclude
  private Iterator<ExprValue> iterator;

  @NonNull
  public PredictOperator(PhysicalPlan input, String algo, String args, ElasticsearchClient elasticsearchClient) {
    this.input = input;
    this.algo = algo;
    this.args = args;
    this.elasticsearchClient = elasticsearchClient;
  }

  @Override
  public void open() {
    super.open();
    List<Map<String, Object>> inputDataFrame = new LinkedList<>();
    while (input.hasNext()) {
      Map<String, Object> items = new HashMap<>();
      input.next().tupleValue().forEach((key, value) -> items.put(key, value.value()));
      inputDataFrame.add(items);
    }
    Map<String, Object> argsMap = new HashMap<>();
    for(String arg: args.split(";")) {
      String[] splits = arg.split("=");
      String key = splits[0];
      String value = splits[1];

      if(StringUtils.isNumeric(splits[1])) {
        argsMap.put(key, Integer.valueOf(value));
      } else if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
        argsMap.put(key, Boolean.valueOf(value.toLowerCase()));
      } else {
        argsMap.put(key, value);
      }
    }
    MLPredictionTaskRequest request = MLPredictionTaskRequest.builder().algorithm(algo)
            .inputDataFrame(inputDataFrame)
            .mlParameter(JsonUtil.serialize(argsMap)).build();
    List<Map<String, Object>>  predictionResult = this.elasticsearchClient.predict(request).getPredictionResult();

    iterator =  predictionResult.stream().map(result -> {
      ImmutableMap.Builder<String, ExprValue> resultBuilder = new ImmutableMap.Builder<>();

      for (Map.Entry<String, Object> entry : result.entrySet()) {
        Object value = entry.getValue();
        if( value instanceof Integer) {
          resultBuilder.put(entry.getKey(), new ExprIntegerValue((Integer)value));
        } else if (value instanceof Long) {
          resultBuilder.put(entry.getKey(), new ExprLongValue((Long)value));
        } else if (value instanceof Double) {
          resultBuilder.put(entry.getKey(), new ExprDoubleValue((Double)value));
        }  else if (value instanceof String) {
          resultBuilder.put(entry.getKey(), new ExprStringValue((String)value));
        }
      }
      return (ExprValue) ExprTupleValue.fromExprValueMap(resultBuilder.build());
    }).iterator();
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitPredict(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.singletonList(input);
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }
}
