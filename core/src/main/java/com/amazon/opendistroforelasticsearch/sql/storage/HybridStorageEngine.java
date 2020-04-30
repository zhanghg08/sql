/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.storage;

import com.amazon.opendistroforelasticsearch.sql.data.model.ExprType;

import java.util.Arrays;
import java.util.List;

public class HybridStorageEngine implements StorageEngine {

    private final List<StorageEngine> engines;

    public HybridStorageEngine(StorageEngine... engines) {
        this.engines = Arrays.asList(engines);
    }

    @Override
    public ExprType getType(String name) {
        return null;
    }

    @Override
    public Table getTable(String name) {
        for (StorageEngine engine : engines) {
            Table table = engine.getTable(name);
            if (table != Table.NONE) {
                return table;
            }
        }
        return Table.NONE;
    }

}
