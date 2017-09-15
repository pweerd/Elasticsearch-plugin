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
import java.util.Objects;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.plain.SortedSetDVOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.plugins.SearchPlugin.AggregationSpec;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValuesSource.Bytes.WithOrdinals;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;

public class ParentsAggregatorBuilder extends AbstractAggregationBuilder<ParentsAggregatorBuilder> {
    public enum AggregatorMode {Undup, MapToParent};
    public final static boolean DEBUG = false;
    public static final String NAME = "bm_parent";
    
    public final String types[];
    public final Query typeFilters[];
    public final ValuesSourceConfig<WithOrdinals> valuesSourceConfigs[];
    public final int levels;
    public final AggregatorMode mode;


    public ParentsAggregatorBuilder(String name, String childType, int levels, AggregatorMode mode) {
        super(name); 
        if (childType == null) {
            throw new IllegalArgumentException("[childType] must not be null: [" + name + "]");
        }
        this.levels = levels;
        this.types = new String[levels+1];
        this.typeFilters = new Query[levels+1];
        this.valuesSourceConfigs = createConfigArr(levels+1);
        this.types[0] = childType;
        this.mode = mode;
    }
    
    @SuppressWarnings("unchecked")
    protected static ValuesSourceConfig<WithOrdinals>[] createConfigArr(int dim) {
       return new ValuesSourceConfig[dim];
    }

    /**
     * Read from a stream.
     */
    public ParentsAggregatorBuilder(StreamInput in) throws IOException {
        super(in);
        levels = in.readVInt();
        mode = AggregatorMode.values()[in.readVInt()];
        this.types = new String[levels+1];
        this.typeFilters = new Query[levels+1];
        this.valuesSourceConfigs = createConfigArr(levels+1);
        this.types[0] = in.readString();
    }
    

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(levels);
        out.writeVInt(mode.ordinal());
        out.writeString(types[0]);
    }

    
    public static AggregationSpec createAggregationSpec() {
        AggregationSpec x = new AggregationSpec(NAME, ParentsAggregatorBuilder::new, ParentsAggregatorBuilder::parse);
        return x.addResultReader(InternalParentsAggregation::new);
    }
    

    protected ValuesSourceConfig<WithOrdinals>[] resolveConfigs (SearchContext context) {
        ValuesSourceConfig<WithOrdinals>[] configs = createConfigArr(levels+1);
        MapperService mapperService = context.mapperService();
        for (int lvl=0; lvl <= levels; lvl++) {
            DocumentMapper docMapper = mapperService.documentMapper(types[lvl]);
            if (docMapper == null) 
                throw new IllegalArgumentException(String.format("[%s]: type [%s] is not defined.", NAME, types[lvl]));
            typeFilters[lvl] = docMapper.typeFilter(context.getQueryShardContext());
            
            if (lvl >= levels) continue;
            
            ParentFieldMapper parentFieldMapper = docMapper.parentFieldMapper();
            if (!parentFieldMapper.active()) {
                throw new IllegalArgumentException(String.format("[%s]: type [%s] is not defined with a parent type.", NAME, types[lvl]));
            }
            
            types[lvl+1] = parentFieldMapper.type();
            
            SortedSetDVOrdinalsIndexFieldData parentChildIndexFieldData = context.fieldData().getForField(parentFieldMapper.fieldType());
            configs[lvl+1] = new ValuesSourceConfig<>(ValuesSourceType.BYTES);
            configs[lvl+1].fieldContext(new FieldContext(parentFieldMapper.fieldType().name(), parentChildIndexFieldData,
                    parentFieldMapper.fieldType()));
        }
        return configs;
    }

    public static ParentsAggregatorBuilder parse(String aggregationName, QueryParseContext context) throws IOException {
        String childType = null;
        int levels = 1;
        AggregatorMode mode = AggregatorMode.Undup;

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
                if ("mode".equals(currentFieldName)) {
                    switch (parser.text().toLowerCase()) {
                        case "undup": mode = AggregatorMode.Undup; break;
                        case "maptoparent": mode = AggregatorMode.MapToParent; break;
                        default: throwParsingException (parser, aggregationName, "Invalid mode [%s]. Possible values: Undup, MapToParent.",  parser.text()); break;
                    }
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
                throwParsingException (parser, aggregationName, "Unexpected token [%s]", token);
            }
            throwParsingException (parser, aggregationName, "Unknown key for a %s. Field: [%s]", token, currentFieldName);
        }

        if (childType == null) 
            throwParsingException (parser, aggregationName, "Missing [type] field");
        if (levels <= 0) 
            throwParsingException (parser, aggregationName, "Field [levels] should be > 0");

        return new ParentsAggregatorBuilder(aggregationName, childType, levels, mode);
    }
    
    private static void throwParsingException (XContentParser parser, String name, String msg) {
        msg = String.format("Aggregation [%s]: %s", name, msg);
        throw new ParsingException(parser.getTokenLocation(), msg);
    }
    private static void throwParsingException (XContentParser parser, String name, String fmt, Object... args) {
        throwParsingException (parser, name, String.format(fmt, args));
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected AggregatorFactory<?> doBuild(SearchContext context, AggregatorFactory<?> parent, Builder subfactoriesBuilder) throws IOException {
        ValuesSourceConfig<WithOrdinals>[] configs = resolveConfigs (context);
        return new ParentsAggregatorFactory(this, configs, context, parent, subfactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", types[0]);
        builder.field("levels", levels);
        builder.field("mode", mode.toString());
        builder.endObject();
        return builder;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(types[0]) ^ (8941*levels) ^ mode.hashCode();
    }

    @Override
    protected boolean doEquals(Object obj) {
        ParentsAggregatorBuilder other = (ParentsAggregatorBuilder) obj;
        return levels == other.levels && mode == other.mode && Objects.equals(types[0], other.types[0]);
    }
}
