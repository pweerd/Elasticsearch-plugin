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

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource.Bytes.WithOrdinals;
import org.elasticsearch.search.internal.SearchContext;

public class UndupByParentsAggregatorFactory extends AggregatorFactory<UndupByParentsAggregatorFactory> {
    public final String parentPaths[];
    public final ParentValueSourceConfig valuesSourceConfigs[];
    public final boolean cache_bitsets;

    public UndupByParentsAggregatorFactory(UndupByParentsAggregatorBuilder bldr, 
            ParentValueSourceConfig valuesSourceConfigs[], 
            SearchContext context, 
            AggregatorFactory<?> parent, 
            Builder subFactoriesBuilder,
            Map<String, Object> metaData) throws IOException {
        super(bldr.getName(), context, parent, subFactoriesBuilder, metaData);
        this.parentPaths = bldr.parentPaths;
        this.cache_bitsets = bldr.cache_bitsets;
        this.valuesSourceConfigs = valuesSourceConfigs;
        if (UndupByParentsAggregatorBuilder.DEBUG) System.out.printf("Created %s[name=%s parent_paths=%s]\n", getClass().getSimpleName(), name, UndupByParentsAggregatorBuilder.parentPathsAsString(parentPaths));
    }

    
    private Aggregator createEmpty (Aggregator parent, 
            List<PipelineAggregator> pipelineAggregators, 
            Map<String, Object> metaData,
            String why) throws IOException {
        if (UndupByParentsAggregatorBuilder.DEBUG) System.out.printf("-- createUnmapped because %s", why);
        return new NonCollectingAggregator(name, context, parent, pipelineAggregators, metaData) {

            @Override
            public InternalAggregation buildEmptyAggregation() {
                return new UndupByParentsInternal (name, 0, pipelineAggregators(), metaData());
            }

        };
        
    }
    @Override
    public Aggregator createInternal(Aggregator parent, 
            boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, 
            Map<String, Object> metaData) throws IOException {
        
        if (valuesSourceConfigs==null) {
            return createEmpty(parent, pipelineAggregators, metaData, "valuesSourceConfigs==null");
        }
        
        QueryShardContext shardCtx = context.getQueryShardContext();
        WithOrdinals valuesSources[] = new WithOrdinals[parentPaths.length];
        for (int i=0; i<valuesSources.length; i++) {
            if (valuesSourceConfigs[i] == null) continue;
            valuesSources[i] = valuesSourceConfigs[i].toValuesSource(shardCtx);
            if (valuesSources[i] == null) {
                return createEmpty(parent, pipelineAggregators, metaData, "valuesSource==null for type=" + parentPaths[i]);
            }
        }
        
        if (UndupByParentsAggregatorBuilder.DEBUG) System.out.printf("-- Create aggregation from factory\n");
        return new UndupByParentsAggregator(this, name, context, parent, pipelineAggregators, metaData, valuesSources);
    }

}
