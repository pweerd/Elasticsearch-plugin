/*
 * Licensed to De Bitmanager under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. De Bitmanager licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package nl.bitmanager.elasticsearch.extensions.aggregations;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource.Bytes.WithOrdinals;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import nl.bitmanager.elasticsearch.extensions.aggregations.ParentsAggregatorBuilder.AggregatorMode;

public class ParentsAggregatorFactory extends AggregatorFactory<ParentsAggregatorFactory> {
    public final String types[];
    public final Query typeFilters[];
    public final ValuesSourceConfig<WithOrdinals> valuesSourceConfigs[];
    public final int levels;
    public final AggregatorMode mode;

    public ParentsAggregatorFactory(ParentsAggregatorBuilder bldr, ValuesSourceConfig<WithOrdinals> valuesSourceConfigs[] 
            , SearchContext context, AggregatorFactory<?> parent, Builder subFactoriesBuilder
            , Map<String, Object> metaData) throws IOException {
        super(bldr.getName(), context, parent, subFactoriesBuilder, metaData);
        this.types = bldr.types;
        this.typeFilters = bldr.typeFilters;
        this.levels = bldr.levels;
        this.valuesSourceConfigs = valuesSourceConfigs;
        this.mode = bldr.mode;
        if (ParentsAggregatorBuilder.DEBUG) System.out.printf("Created UndupByParentAggregatorFactory[name=%s type=%s, parent=%s]\n", name, types[0], types[1]);
    }

    @Override
    public Aggregator createInternal(Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        
        WithOrdinals valuesSources[] = new WithOrdinals[levels+1];
        QueryShardContext shardCtx = context.getQueryShardContext();
        for (int i=1; i<=levels; i++) {
            valuesSources[i] = valuesSourceConfigs[i].toValuesSource(shardCtx);
            if (valuesSources[i] == null) {
                if (ParentsAggregatorBuilder.DEBUG) System.out.printf("-- createUnmapped because type[%s] does not have a source\n", types[i]);
                return new NonCollectingAggregator(name, context, parent, pipelineAggregators, metaData) {

                    @Override
                    public InternalAggregation buildEmptyAggregation() {
                        return new InternalParentsAggregation(name, 0, buildEmptySubAggregations(), pipelineAggregators(), metaData());
                    }
                };
            }
        }
        
        if (ParentsAggregatorBuilder.DEBUG) System.out.printf("-- Create aggregation from factory\n");
        return new ParentsAggregator(this, name, factories, context, parent, pipelineAggregators, metaData, valuesSources);
    }

}
