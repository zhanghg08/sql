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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import com.amazon.opendistroforelasticsearch.ml.client.MachineLearningClient;
import com.amazon.opendistroforelasticsearch.ml.common.dataframe.ColumnMeta;
import com.amazon.opendistroforelasticsearch.ml.common.dataframe.ColumnValue;
import com.amazon.opendistroforelasticsearch.ml.common.dataframe.DataFrame;
import com.amazon.opendistroforelasticsearch.ml.common.dataframe.DataFrameBuilder;
import com.amazon.opendistroforelasticsearch.ml.common.dataframe.Row;
import com.amazon.opendistroforelasticsearch.ml.common.parameter.Parameter;
import com.amazon.opendistroforelasticsearch.ml.common.parameter.ParameterBuilder;
import com.amazon.opendistroforelasticsearch.sql.analysis.TypeEnvironment;
import com.amazon.opendistroforelasticsearch.sql.analysis.symbol.Namespace;
import com.amazon.opendistroforelasticsearch.sql.analysis.symbol.Symbol;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprDoubleValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprIntegerValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprLongValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprStringValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprTupleValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType;
import com.amazon.opendistroforelasticsearch.sql.data.type.ExprType;
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

  private final MachineLearningClient machineLearningClient;

  @EqualsAndHashCode.Exclude
  private Iterator<ExprValue> iterator;

  @NonNull
  public PredictOperator(PhysicalPlan input, String algo, String args, MachineLearningClient machineLearningClient) {
    this.input = input;
    this.algo = algo;
    this.args = args;
    this.machineLearningClient = machineLearningClient;
  }

  @Override
  public void open() {
    super.open();
    List<Map<String, Object>> inputDataMapList = new LinkedList<>();
    Map<String, ExprType> fieldTypes = new HashMap<>();
    while (input.hasNext()) {
      Map<String, Object> items = new HashMap<>();
      input.next().tupleValue().forEach((key, value) -> {
        items.put(key, value.value());
        fieldTypes.put(key, value.type());
      });
      inputDataMapList.add(items);
    }

    DataFrame dataFrame = DataFrameBuilder.load(inputDataMapList);
    List<Parameter> parameters = new LinkedList<>();
    String modelId = null;
    for(String arg: args.split(",")) {
      String[] splits = arg.split("=");
      String key = splits[0];
      String value = splits[1];
      if(key.equalsIgnoreCase("modelId")) {
        modelId = value;
        continue;
      }
      if(StringUtils.isNumeric(splits[1])) {
        parameters.add(ParameterBuilder.parameter(key, Integer.parseInt(value)));
      } else if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
        parameters.add(ParameterBuilder.parameter(key, Boolean.parseBoolean(value.toLowerCase())));
      } else if(value.contains("-")) {
        int[] list = Arrays.stream(value.split("-")).map(Integer::parseInt).mapToInt(x->x).toArray();
        parameters.add(ParameterBuilder.parameter(key, list));
      } else  {
        parameters.add(ParameterBuilder.parameter(key, value));
      }
    }

    DataFrame predictionResult = this.machineLearningClient.predict(algo, parameters, dataFrame, modelId);
    ColumnMeta[] columnMetas = predictionResult.columnMetas();

    String resultKeyName = getKeyName();
    this.iterator = StreamSupport.stream(predictionResult.spliterator(), false).map(row -> {
      ImmutableMap.Builder<String, ExprValue> resultBuilder = new ImmutableMap.Builder<>();

      for (int i =0 ; i < columnMetas.length; i++) {
        ColumnValue columnValue = row.getValue(i);
        switch(columnValue.columnType()){
          case INTEGER:
            resultBuilder.put(resultKeyName, new ExprStringValue(String.valueOf(columnValue.intValue())));
            break;
          case STRING:
            resultBuilder.put(resultKeyName, new ExprStringValue(columnValue.stringValue()));
            break;
          case DOUBLE:
            resultBuilder.put(resultKeyName, new ExprStringValue(String.valueOf(columnValue.doubleValue())));
            break;
        }
      }
      return (ExprValue) ExprTupleValue.fromExprValueMap(resultBuilder.build());
    }).iterator();
  }

  private String getKeyName() {
    if(args.contains("target")) {
      for(String split: args.split(",")) {
        if(split.trim().contains("target")) {
          String[] targets = split.trim().split("=");
          return targets[1];
        }
      }
    }

    return "result";

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
