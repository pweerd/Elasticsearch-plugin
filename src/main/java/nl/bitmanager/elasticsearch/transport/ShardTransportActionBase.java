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

package nl.bitmanager.elasticsearch.transport;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import nl.bitmanager.elasticsearch.support.Utils;

/**
 * Base class for shard related actions
 */
public abstract class ShardTransportActionBase
        extends TransportBroadcastAction<ShardBroadcastRequest, ShardBroadcastResponse, ShardRequest, ShardResponse> {
    protected final ShardActionDefinitionBase definition;

    protected ShardTransportActionBase(ShardActionDefinitionBase definition, Settings settings, ThreadPool threadPool,
            ClusterService clusterService, TransportService transportService, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, definition.name(), threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                new ShardBroadcastRequest.Factory(definition), new ShardRequest.Factory(definition), ThreadPool.Names.GENERIC);
        this.definition = definition;
    }

    protected IndexShard getShard(IndicesService indicesService, ShardRequest request) {
        ShardId shardId = request.shardId();
        return indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.getId());
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, ShardBroadcastRequest request, String[] concreteIndices) {
        RoutingTable rt = clusterState.routingTable();
        switch (definition.targets) {
        default:
            throw new RuntimeException("Unexpected ShardsEnum value [" + definition.targets + "]");
        case ALL:
            return rt.allAssignedShardsGrouped(concreteIndices, definition.includeEmptyShards);
        case PRIMARY:
            return rt.activePrimaryShardsGrouped(concreteIndices, definition.includeEmptyShards);
        case ACTIVE:
            return rt.allActiveShardsGrouped(concreteIndices, definition.includeEmptyShards);
        case ASSIGNED:
            return rt.allAssignedShardsGrouped(concreteIndices, definition.includeEmptyShards);
        }
    }

    // PW
    // @Override
    // protected String executor() {
    // return ThreadPool.Names.GENERIC;
    // }
    //
    // @Override
    // protected ShardBroadcastRequest newRequest() {
    // return new ShardBroadcastRequest(actionDefinition.createTransportItem());
    // }

    // protected abstract void consolidateResponse (TransportItemBase
    // consolidatedItem, TransportItemBase shardItem);

    @SuppressWarnings("rawtypes")
    @Override
    protected ShardBroadcastResponse newResponse(ShardBroadcastRequest request, AtomicReferenceArray shardsResponses,
            ClusterState clusterState) {
        if (definition.debug)
            System.out.printf("[%s]: newBroadcastShardResponse()\n", definition.id);
        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
        TransportItemBase consolidatedResponse = request.getTransportItem();

        List<ShardResponse> okResponses = null;
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);

            if (shardResponse == null)
                continue; // a non active shard, ignore...
            if (shardResponse instanceof BroadcastShardOperationFailedException) {
                BroadcastShardOperationFailedException shardException = (BroadcastShardOperationFailedException) shardResponse;
                Throwable cause = shardException.getCause();
                if (cause == null)
                    cause = shardException;

                logger.warn(shardException.getMessage(), cause);
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = new ArrayList<ShardOperationFailedException>();
                }
                shardFailures.add(new DefaultShardOperationFailedException(shardException));
                continue;
            }
            successfulShards++;
            if (shardResponse instanceof ShardResponse) {
                if (okResponses == null)
                    okResponses = new ArrayList<ShardResponse>();
                ShardResponse resp = (ShardResponse) shardResponse;
                okResponses.add(resp);
                consolidatedResponse.consolidateResponse(resp.getTransportItem());
                continue;
            }
            logger.info("Got unexpected shardResponse: " + Utils.getTrimmedClass(shardResponse));
        }

        // logger.info("TransportAction::newResponse returning
        // BroadcastResponse...");
        return new ShardBroadcastResponse(consolidatedResponse, shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ShardResponse newShardResponse() {
        if (definition.debug)
            System.out.printf("[%s]: newShardResponse()\n", definition.id);
        return new ShardResponse(definition);
    }

    @Override
    protected ShardRequest newShardRequest(int numShards, ShardRouting shard, ShardBroadcastRequest request) {
        if (definition.debug)
            System.out.printf("[%s]: newShardRequest()\n", definition.id);
        return new ShardRequest(shard.shardId(), request);
    }

    protected abstract TransportItemBase handleShardRequest(ShardRequest request) throws Exception;

    @Override
    protected ShardResponse shardOperation(ShardRequest request) throws ElasticsearchException {
        try {
            return new ShardResponse(definition, request.shardId(), handleShardRequest(request));
        } catch (Throwable ex) {
            logger.error("Node ransport error: " + ex.getMessage(), ex);
            throw new ElasticsearchException(ex.getMessage(), ex);
        }
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ShardBroadcastRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ShardBroadcastRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

}
