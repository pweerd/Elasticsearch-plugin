/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
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
import java.util.Objects;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.plugins.SearchPlugin.AggregationSpec;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValuesSource.Bytes.ParentChild;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;

public class UndupByParentAggregatorBuilder extends AbstractAggregationBuilder<UndupByParentAggregatorBuilder> {
    public final static boolean DEBUG = false;
    public static final String NAME = "undup-by-parent";
    
    public final String types[];
    public final Query typeFilters[];
    public final ValuesSourceConfig<ParentChild> valuesSourceConfigs[];
    public final int levels;


    public UndupByParentAggregatorBuilder(String name, String childType, int levels) {
        super(name); //, ValuesSourceType.BYTES, ValueType.STRING);
        if (childType == null) {
            throw new IllegalArgumentException("[childType] must not be null: [" + name + "]");
        }
        this.levels = levels;
        this.types = new String[levels+1];
        this.typeFilters = new Query[levels+1];
        this.valuesSourceConfigs = createConfigArr(levels+1);
        this.types[0] = childType;
    }
    
    @SuppressWarnings("unchecked")
    protected static ValuesSourceConfig<ParentChild>[] createConfigArr(int dim) {
       return new ValuesSourceConfig[dim];
    }

    /**
     * Read from a stream.
     */
    public UndupByParentAggregatorBuilder(StreamInput in) throws IOException {
        super(in);
        levels = in.readVInt();
        this.types = new String[levels+1];
        this.typeFilters = new Query[levels+1];
        this.valuesSourceConfigs = createConfigArr(levels+1);
        this.types[0] = in.readString();
    }
    

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(levels);
        out.writeString(types[0]);
    }

    
    public static AggregationSpec createAggregationSpec() {
        AggregationSpec x = new AggregationSpec(NAME, UndupByParentAggregatorBuilder::new, UndupByParentAggregatorBuilder::parse);
        return x.addResultReader(InternalParentsAggregation::new);
    }
    

    protected ValuesSourceConfig<ParentChild>[] resolveConfigs (SearchContext context) {
        ValuesSourceConfig<ParentChild>[] configs = createConfigArr(levels+1);
        MapperService mapperService = context.mapperService();
        for (int lvl=0; lvl <= levels; lvl++) {
            DocumentMapper docMapper = mapperService.documentMapper(types[lvl]);
            if (docMapper == null) 
                throw new IllegalArgumentException(String.format("[%s]: type [%s] is not defined.", NAME, types[lvl]));
            typeFilters[lvl] = docMapper.typeFilter();
            
            if (lvl >= levels) continue;
            
            ParentFieldMapper parentFieldMapper = docMapper.parentFieldMapper();
            if (!parentFieldMapper.active()) {
                throw new IllegalArgumentException(String.format("[%s]: type [%s] is not defined with a parent type.", NAME, types[lvl]));
            }
            
            types[lvl+1] = parentFieldMapper.type();
            
            ParentChildIndexFieldData parentChildIndexFieldData = context.fieldData().getForField(parentFieldMapper.fieldType());
            configs[lvl+1] = new ValuesSourceConfig<>(ValuesSourceType.BYTES);
            configs[lvl+1].fieldContext(new FieldContext(parentFieldMapper.fieldType().name(), parentChildIndexFieldData,
                    parentFieldMapper.fieldType()));
        }
        return configs;
    }

    public static UndupByParentAggregatorBuilder parse(String aggregationName, QueryParseContext context) throws IOException {
        String childType = null;
        int levels = 1;

        XContentParser.Token token;
        String currentFieldName = null;
        XContentParser parser = context.parser();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            switch (token) {
            case FIELD_NAME: 
                currentFieldName = parser.currentName();
                continue;
            case VALUE_STRING:
                if ("type".equals(currentFieldName)) {
                    childType = parser.text();
                    continue;
                }
                break;
            case VALUE_NUMBER:
                if ("levels".equals(currentFieldName)) {
                    levels = parser.intValue();
                    continue;
                }
                break;
            default:
                throw new ParsingException(parser.getTokenLocation(), "Unexpected token " + token + " in [" + aggregationName + "].");
            }
            throw new ParsingException(parser.getTokenLocation(),
                         "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
        }

        if (childType == null) {
            throw new ParsingException(parser.getTokenLocation(),
                    "Missing [type] field for [" + aggregationName + "] aggregation.");
        }
        if (levels <= 0) {
            throw new ParsingException(parser.getTokenLocation(),
                    "Field [levels] should be > 0 for [" + aggregationName + "] aggregation.");
        }

        return new UndupByParentAggregatorBuilder(aggregationName, childType, levels);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected AggregatorFactory<?> doBuild(SearchContext context, AggregatorFactory<?> parent, Builder subfactoriesBuilder) throws IOException {
        ValuesSourceConfig<ParentChild>[] configs = resolveConfigs (context);
        return new UndupByParentAggregatorFactory(this, configs, context, parent, subfactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", types[0]);
        builder.field("levels", levels);
        builder.endObject();
        return builder;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(types[0]) ^ (8941*levels);
    }

    @Override
    protected boolean doEquals(Object obj) {
        UndupByParentAggregatorBuilder other = (UndupByParentAggregatorBuilder) obj;
        return levels == other.levels && Objects.equals(types[0], other.types[0]);
    }
}
