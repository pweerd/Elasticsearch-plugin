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
import java.util.Objects;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

/**
 * Result of the {@link UndupByParentsAggregator}.
 */
public class UndupByParentsInternal extends InternalNumericMetricsAggregation.SingleValue implements UndupByParents {
    private final double count;
    
    public UndupByParentsInternal(String name, double count, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        
        super(name, pipelineAggregators, metaData);
        this.count = count;
    }

    /**
     * Read from a stream.
     */
    public UndupByParentsInternal(StreamInput in) throws IOException {
        super(in);
        count = in.readDouble();

    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(count);
    }

    @Override
    public String getWriteableName() {
        return UndupByParentsAggregatorBuilder.NAME;
    }

    @Override
    public UndupByParentsInternal doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        double count = 0;
        for (InternalAggregation aggregation : aggregations) {
            count += ((UndupByParentsInternal) aggregation).count;
        }
        return new UndupByParentsInternal(name, count, pipelineAggregators(), getMetaData());
    }

    @Override
    public double value() {
        return count;
    }

    @Override
    public long getValue() {
        return (long)count;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        boolean hasValue = !Double.isInfinite(count);
        builder.field(CommonFields.DOC_COUNT.getPreferredName(), hasValue ? count : null);
        return builder;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(count);
    }

    @Override
    protected boolean doEquals(Object obj) {
        UndupByParentsInternal other = (UndupByParentsInternal) obj;
        return Objects.equals(count, other.count);
    }
}
