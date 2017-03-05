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

package nl.bitmanager.elasticsearch.extensions.view;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import nl.bitmanager.elasticsearch.transport.ShardRequest;
import nl.bitmanager.elasticsearch.transport.ShardTransportActionBase;
import nl.bitmanager.elasticsearch.transport.TransportItemBase;

public class TransportAction extends ShardTransportActionBase {
    private final IndicesService indicesService;

    @Inject
    public TransportAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
            IndicesService indicesService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ActionDefinition.INSTANCE, settings, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver);
        this.indicesService = indicesService;
    }

    @Override
    protected TransportItemBase handleShardRequest(ShardRequest request) throws Exception {
        ViewTransportItem rawRequest = (ViewTransportItem) request.getTransportItem();

        ViewTransportItem transportItem = new ViewTransportItem(rawRequest);
        IndexShard indexShard = super.getShard(indicesService, request);

        transportItem.processShard(indexShard);
        return transportItem;
    }

}