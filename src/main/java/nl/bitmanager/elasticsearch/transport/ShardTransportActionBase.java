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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
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
        super(definition.name,
                clusterService,
                transportService,
                actionFilters,
                indexNameExpressionResolver,
                (StreamInput in)->definition.createBroadcastRequest(in),
                (StreamInput in)->definition.createShardRequest(in),
                ThreadPool.Names.GENERIC);
        this.definition = definition;
    }

    protected IndexShard getShard(IndicesService indicesService, ShardRequest request)  throws IOException {
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

    @SuppressWarnings("rawtypes")
    @Override
    protected ShardBroadcastResponse newResponse(ShardBroadcastRequest request, AtomicReferenceArray shardsResponses,
            ClusterState clusterState) {
        if (definition.debug)
            definition.logger.info ("[%s]: newBroadcastShardResponse()\n", definition.id);
        int successfulShards = 0;
        int failedShards = 0;
        List<DefaultShardOperationFailedException> shardFailures = null;
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

                definition.logger.warn(shardException.getMessage(), cause);
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = new ArrayList<DefaultShardOperationFailedException>();
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
            definition.logger.info("Got unexpected shardResponse: " + Utils.getTrimmedClass(shardResponse));
        }

        // logger.info("TransportAction::newResponse returning
        // BroadcastResponse...");
        return new ShardBroadcastResponse(definition, consolidatedResponse, shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ShardRequest newShardRequest(int numShards, ShardRouting shard, ShardBroadcastRequest request) {
        if (definition.debug)
            definition.logger.info ("[%s]: new ShardRequest()\n", definition.id);
        return new ShardRequest(shard.shardId(), request);
    }

    @Override
    protected ShardResponse readShardResponse(StreamInput in) throws IOException {
        if (definition.debug)
            definition.logger.info ("[%s]: read ShardResponse()\n", definition.id);
        return new ShardResponse(definition, in);
    }

    protected abstract TransportItemBase handleShardRequest(ShardRequest request) throws Exception;

    @Override
    protected ShardResponse shardOperation(ShardRequest request, Task task) throws IOException {
        try {
            return new ShardResponse(definition, request.shardId(), handleShardRequest(request));
        } catch (Throwable ex) {
            definition.logger.error("Node ransport error: " + ex.getMessage(), ex);
            throw new IOException(ex.getMessage(), ex);
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
