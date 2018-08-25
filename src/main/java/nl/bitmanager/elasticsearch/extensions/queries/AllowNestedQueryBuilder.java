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

package nl.bitmanager.elasticsearch.extensions.queries;

import static nl.bitmanager.elasticsearch.extensions.queries.QueryBuilderHelper.throwUnexpectedToken;
import static nl.bitmanager.elasticsearch.extensions.queries.QueryBuilderHelper.throwUnsupportedField;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;

public class AllowNestedQueryBuilder extends AbstractQueryBuilder<AllowNestedQueryBuilder> {
    public static final String NAME = "bm_allow_nested";

    private final QueryBuilder query;

    private AllowNestedQueryBuilder(QueryBuilder query) {
        this.query = requireValue(query, "[" + NAME + "] requires 'query' field");
    }

    /**
     * Read from a stream.
     */
    public AllowNestedQueryBuilder(StreamInput in) throws IOException {
        super(in);
        query = in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(query);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("query");
        query.toXContent(builder, params);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static AllowNestedQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        QueryBuilder query = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    query = parseInnerQueryBuilder(parser);
                } else {
                    throwUnsupportedField (parser, NAME);
                }
            } else if (token.isValue()) {
//                if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName)) {
//                    boost = parser.floatValue();
//                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName)) {
//                    queryName = parser.text();
//                } else {
                    throwUnsupportedField (parser, NAME);
//                }
            } else
                throwUnexpectedToken (parser, NAME);

        }
        AllowNestedQueryBuilder queryBuilder =  new AllowNestedQueryBuilder(query)
            .queryName(queryName)
            .boost(boost);
        return queryBuilder;
    }

    @Override
    public final String getWriteableName() {
        return NAME;
    }

    @Override
    protected boolean doEquals(AllowNestedQueryBuilder that) {
        return Objects.equals(query, that.query);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query innerQuery = this.query.toQuery(context);
        return new AllowNestedQuery(innerQuery);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder rewrittenQuery = query.rewrite(queryRewriteContext);
        if (rewrittenQuery != query) {
            return new AllowNestedQueryBuilder(rewrittenQuery);
        }
        return this;
    }

}
