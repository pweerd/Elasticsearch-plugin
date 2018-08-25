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

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;

public class MatchNestedQueryBuilder extends AbstractQueryBuilder<MatchNestedQueryBuilder> {
    public static final String NAME = "bm_match_nested";

    private final QueryBuilder subBuilder;

    private MatchNestedQueryBuilder(QueryBuilder query) {
        this.subBuilder = query;
    }

    /**
     * Read from a stream.
     */
    public MatchNestedQueryBuilder(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        subBuilder = size==0 ? null : in.readNamedWriteable(QueryBuilder.class);

    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (subBuilder == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(1);
            out.writeNamedWriteable(subBuilder);
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("query");
        subBuilder.toXContent(builder, params);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static MatchNestedQueryBuilder fromXContent(XContentParser parser) throws IOException {
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
        MatchNestedQueryBuilder queryBuilder =  new MatchNestedQueryBuilder(query)
            .queryName(queryName)
            .boost(boost);
        return queryBuilder;
    }

    @Override
    public final String getWriteableName() {
        return NAME;
    }

    @Override
    protected boolean doEquals(MatchNestedQueryBuilder that) {
        return Objects.equals(subBuilder, that.subBuilder);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(subBuilder);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        return new MatchNestedQuery (subBuilder==null ? null : subBuilder.toQuery(context));
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (subBuilder!=null) {
            QueryBuilder rewrittenQuery = subBuilder.rewrite(queryRewriteContext);
            if (rewrittenQuery != subBuilder) {
                return new MatchNestedQueryBuilder(rewrittenQuery);
            }
        }
        
        return this;
    }

}
