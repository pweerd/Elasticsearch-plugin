package nl.bitmanager.elasticsearch.search;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchExtBuilder;

public class SearchParmDocValues extends SearchExtBuilder {
    public static final String WRITEABLENAME = "_bm";

    @Override
    public String getWriteableName() {
        return WRITEABLENAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

}
