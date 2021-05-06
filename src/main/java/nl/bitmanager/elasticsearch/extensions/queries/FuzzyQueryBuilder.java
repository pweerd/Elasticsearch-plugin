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

import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;

public class FuzzyQueryBuilder extends AbstractQueryBuilder<FuzzyQueryBuilder> {
    public static final String NAME = "bm_fuzzy";
    private final QueryBuilder subBuilder;

    public FuzzyQueryBuilder(QueryBuilder sub) {
        subBuilder = sub;
    }

    /**
     * Read from a stream.
     */
    public FuzzyQueryBuilder(StreamInput in) throws IOException {
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

    public static FuzzyQueryBuilder fromXContent(XContentParser parser) throws IOException {
        QueryBuilder sub = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            //System.out.printf("Token1: %s\n", token);
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                //System.out.printf("FName1: %s\n", currentFieldName);
                if (!"query".equals(currentFieldName))
                    throwUnsupportedField (parser,NAME);
            } else if (token == XContentParser.Token.START_OBJECT) {
                sub = parseInnerQueryBuilder(parser);
            } else {
                throwUnexpectedToken (parser, NAME);
            }
        }

        return new FuzzyQueryBuilder(sub);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query sub = null;

        if (subBuilder != null) sub = subBuilder.toQuery(context);
        return new FuzzyQuery (sub);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        if (subBuilder != null) {
            builder.field("query");
            subBuilder.toXContent(builder, params);
        }
        builder.endObject();
    }

    @Override
    protected boolean doEquals(FuzzyQueryBuilder other) {
        if (subBuilder==null)
            return other.subBuilder==null;
        return subBuilder.equals (other.subBuilder);
    }

    @Override
    protected int doHashCode() {
        return subBuilder == null ? 0 : subBuilder.hashCode();
    }
}