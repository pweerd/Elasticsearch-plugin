package nl.bitmanager.elasticsearch.extensions.queries;

import java.io.IOException;
import java.util.Optional;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;

public class MatchDeletedQueryBuilder extends AbstractQueryBuilder<MatchDeletedQueryBuilder> {
        private final QueryBuilder subBuilder;

        public MatchDeletedQueryBuilder() {
            subBuilder = null;
        }
        public MatchDeletedQueryBuilder(QueryBuilder sub) {
            subBuilder = sub;
        }
        
        /**
         * Read from a stream.
         */
        public MatchDeletedQueryBuilder(StreamInput in) throws IOException {
            super(in);
            int size = in.readVInt();
            QueryBuilder q = null;
            switch (size) {
                default: throw new RuntimeException (String.format("unexpected #sub queries: %d, expected 0 or 1.", size));
                case 1:
                    q = in.readNamedWriteable(QueryBuilder.class);
                    break;
                case 0:
                    q = null;
            }
            
            subBuilder = q;
        }

        public static Optional<MatchDeletedQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
            
            QueryBuilder sub = null;
            XContentParser parser = parseContext.parser();

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                //System.out.printf("Token1: %s\n", token);
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    //System.out.printf("FName1: %s\n", currentFieldName);
                    if (!"query".equals(currentFieldName))
                        throw new ParsingException(parser.getTokenLocation(),
                                String.format("[%s] query does not support [%s]", MatchDeletedQuery.NAME, currentFieldName));
                } else if (token == XContentParser.Token.START_OBJECT) {
                    Optional<QueryBuilder> tmp = parseContext.parseInnerQueryBuilder();
                    if (tmp.isPresent()) sub = tmp.get();
                } else {    
                    throw new ParsingException(parser.getTokenLocation(),
                            String.format("Unexpected token %s in [%s] query.", token, MatchDeletedQuery.NAME));
                }
            }

            MatchDeletedQueryBuilder q = new MatchDeletedQueryBuilder(sub);
            return Optional.of(q);
        }
        
        @Override
        protected Query doToQuery(QueryShardContext context) throws IOException {
            Query sub = null;
            
            if (subBuilder != null) sub = subBuilder.toQuery(context);
            return new MatchDeletedQuery (sub);
        }

        @Override
        public String getWriteableName() {
            return MatchDeletedQuery.NAME;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            if (subBuilder == null) {
                out.writeVInt(0);
            } else {
                out.writeVInt(0);
                out.writeNamedWriteable(subBuilder);
            }
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(MatchDeletedQuery.NAME);
            if (subBuilder != null) {
                builder.field("query");
                subBuilder.toXContent(builder, params);
            }
            builder.endObject();
        }

        @Override
        protected boolean doEquals(MatchDeletedQueryBuilder other) {
            if (subBuilder==null)
                return other.subBuilder==null;
            return subBuilder.equals (other.subBuilder);
        }

        @Override
        protected int doHashCode() {
            return subBuilder == null ? 0 : subBuilder.hashCode();
        }
    }