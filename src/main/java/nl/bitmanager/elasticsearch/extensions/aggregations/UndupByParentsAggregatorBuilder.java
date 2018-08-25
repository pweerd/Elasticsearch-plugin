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
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Objects;

import org.apache.lucene.search.Query;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.plugins.SearchPlugin.AggregationSpec;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.internal.SearchContext;

public class UndupByParentsAggregatorBuilder extends AbstractAggregationBuilder<UndupByParentsAggregatorBuilder> {
    public final static boolean DEBUG = false;
    public static final String NAME = "bm_undup_by_parents";
    
    public final String[] parentPaths;
    public final boolean resilient;
    public final boolean cache_bitsets;


    public UndupByParentsAggregatorBuilder(String name, String path, boolean resilient, boolean cache_bitsets) {
        super(name); 
        this.resilient = resilient;
        this.cache_bitsets = cache_bitsets;
        if (path == null) {
            throw new IllegalArgumentException("[parent_path] must not be null: [" + name + "]");
        }
        parentPaths = parsePath (path);
        if (parentPaths.length == 0) {
            throw new IllegalArgumentException("[parent_path] should not be empty: [" + name + "]");
        }
    }
    
    private String[] parsePath (String path) {
        String[] x = path.split("[,;]+");
        int j=0;
        for (int i=0; i<x.length; i++) {
            x[i] = x[i].trim();
            if (x[i].length() == 0) continue;
            x[j] = x[i];
            j++;
        }
        
        return (j < x.length) ? Arrays.copyOf (x, j) : x;
    }
    
    
    
    /**
     * Read from a stream.
     */
    public UndupByParentsAggregatorBuilder(StreamInput in) throws IOException {
        super(in);
        int levels = in.readVInt();
        this.parentPaths = new String[levels];
        for (int i=0; i<parentPaths.length; i++)
            parentPaths[i] = in.readString();
        resilient = in.readBoolean();
        cache_bitsets = in.readBoolean();
    }
    

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(parentPaths.length);
        for (int i=0; i<parentPaths.length; i++)
            out.writeString(parentPaths[i]);
        out.writeBoolean(resilient);
        out.writeBoolean(cache_bitsets);
    }

    
    public static AggregationSpec createAggregationSpec() {
        AggregationSpec x = new AggregationSpec(NAME, UndupByParentsAggregatorBuilder::new, UndupByParentsAggregatorBuilder::parse);
        return x.addResultReader(UndupByParentsInternal::new);
    }
    
    
    protected ParentValueSourceConfig[] resolveConfigs (SearchContext context) {
        final int levels = parentPaths.length;
        ParentValueSourceConfig[] configs = new ParentValueSourceConfig[levels];
        MapperService mapperService = context.mapperService();
        _ParentJoinGetter x = new _ParentJoinGetter(mapperService);
        
        FieldMapper parentJoinFieldMapper = x.getJoinFieldMapper();
        
        for (int lvl=0; lvl < levels; lvl++) {
            String type = parentPaths[lvl];
            if ("_nested_".equals(type)) {
                configs[lvl] = null;
                continue;
            }
            
            if (parentJoinFieldMapper==null) {
                if (!resilient)
                    throw new RuntimeException ("Mapping has no join field.");
                return null;
            }
            
            FieldMapper parentIdFieldMapper = (FieldMapper) x.getIdMapper(type, true);
            if (parentIdFieldMapper==null) {
                if (!resilient)
                    throw new RuntimeException ("Parent type [" + type + "] not found.");
                return null;
            }
            
            configs[lvl] = new ParentValueSourceConfig (context, parentIdFieldMapper, x.getParentFilter());
        }
        return configs;
    }

    public static UndupByParentsAggregatorBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        String parent_paths = null;
        XContentParser.Token token;
        String currentFieldName = null;
        boolean resilient = true;
        boolean cache = true; 
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            switch (token) {
            case FIELD_NAME: 
                currentFieldName = parser.currentName();
                continue;
            case VALUE_STRING:
                if ("parent_paths".equals(currentFieldName)) {
                    parent_paths = parser.text();
                    continue;
                }
                break;
            case VALUE_BOOLEAN:
                if ("resilient".equals(currentFieldName)) {
                    resilient = parser.booleanValue();
                    continue;
                }
                if ("cache_bitsets".equals(currentFieldName)) {
                    cache = parser.booleanValue();
                    continue;
                }
                break;
            default:
                throwParsingException (parser, aggregationName, "Unexpected token [%s]", token);
            }
            throwParsingException (parser, aggregationName, "Unknown key for a %s. Field: [%s]", token, currentFieldName);
        }

        if (parent_paths == null) 
            throwParsingException (parser, aggregationName, "Missing [parent_paths] field");
        return new UndupByParentsAggregatorBuilder(aggregationName, parent_paths, resilient, cache);
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
        ParentValueSourceConfig[] configs = resolveConfigs (context);
        return new UndupByParentsAggregatorFactory(this, configs, context, parent, subfactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("parent_paths", parentPathsAsString(parentPaths));
        builder.field("resilient", resilient);
        builder.field("cache_bitsets", cache_bitsets);
        builder.endObject();
        return builder;
    }
    
    public static String parentPathsAsString (String[] parentPaths) {
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<parentPaths.length; i++) {
            if (i>0) sb.append(',');
            sb.append(parentPaths[i]);
        }
        return sb.toString();
    }

    @Override
    protected int doHashCode() {
        return Objects.hash((Object[])parentPaths);
    }

    @Override
    protected boolean doEquals(Object obj) {
        if (obj==null || obj.getClass() != getClass()) return false;
        UndupByParentsAggregatorBuilder other = (UndupByParentsAggregatorBuilder) obj;
        return Arrays.deepEquals(parentPaths,  other.parentPaths);
    }

    
    /** Helper class to get info from the join mapper
     *  The join mapper lives in a module and is not normally accessible by us :-( 
     */
    static class _ParentJoinGetter implements PrivilegedAction<Object> {
        public final MapperService service;
        public final MappedFieldType joinFieldType;
        public FieldMapper parentJoinFieldMapper;
        public FieldMapper parentIdFieldMapper;
        
        private volatile Throwable error;
        private String cmd;
        private String type;
        private boolean isParent;
        
        public _ParentJoinGetter (MapperService service) {
            SpecialPermission.check();
            this.service = service;
            this.joinFieldType = service.fullName("_parent_join");
        }
        
        public Query getParentFilter () {
            cmd = "get_parent_filter";
            return (Query) doRequest();
        }
        public FieldMapper getIdMapper (String type, boolean parent) {
            cmd = "get_id_mapper";
            this.type = type;
            this.isParent = parent;
            return (FieldMapper) doRequest();
        }
        public FieldMapper getJoinFieldMapper () {
            this.cmd = "get_join_mapper";
            return (FieldMapper) doRequest();
        }
        private Object doRequest () {
            this.error = null;
            Object ret = AccessController.doPrivileged (this);
            if (error != null) 
                throw new RuntimeException (error.toString() + "\nIs the correct plugin-security.policy in place?", error);
            return ret;
        }

        private Object run(String cmd) throws Exception {
            if (cmd == "get_join_mapper") {
                parentJoinFieldMapper = null;
                Class<? extends Object> c = joinFieldType.getClass();
                Method m = c.getDeclaredMethod("getMapper");
                return parentJoinFieldMapper = (FieldMapper) m.invoke(joinFieldType);
            }

            if (cmd == "get_id_mapper") {
                parentIdFieldMapper = null;
                if (parentJoinFieldMapper == null) return null;
                Class<? extends Object> c = parentJoinFieldMapper.getClass();
                Method m = c.getDeclaredMethod("getParentIdFieldMapper", String.class, boolean.class);
                return parentIdFieldMapper = (FieldMapper) m.invoke(parentJoinFieldMapper, type, isParent);
            }

            if (cmd == "get_parent_filter") {
                if (parentIdFieldMapper == null) return null;
                Class<? extends Object> c = parentIdFieldMapper.getClass();
                Method m = c.getDeclaredMethod("getParentFilter");
                return m.invoke(parentIdFieldMapper);
            }

            
            throw new RuntimeException ("Unexpected cmd=[" + cmd + "]");
        }
        
        
        @Override
        public Object run() {
            try {
                return run(cmd);
            } catch (Exception e) {
                error = e;
            }
            return null;
        }
    }


    
}
