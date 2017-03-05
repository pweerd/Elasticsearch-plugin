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

import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import nl.bitmanager.elasticsearch.support.Utils;

public abstract class NodeTransportActionBase
        extends TransportNodesAction<NodeBroadcastRequest, NodeBroadcastResponse, NodeRequest, NodeResponse> {
    public final boolean debug;
    protected final NodeActionDefinitionBase definition;

    protected NodeTransportActionBase(NodeActionDefinitionBase definition, Settings settings, ThreadPool threadPool,
            ClusterService clusterService, TransportService transportService, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, definition.name(), threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                new NodeBroadcastRequest.Factory(definition), new NodeRequest.Factory(definition), ThreadPool.Names.GENERIC,
                NodeResponse.class);
        this.definition = definition;
        this.debug = definition.debug;
        if (definition.debug) System.out.printf("Creating node action %s\n", Utils.getTrimmedClass(this));
    }

    @Override
    protected NodeRequest newNodeRequest(String nodeId, NodeBroadcastRequest request) {
        if (debug)
            System.out.printf("[%s]: newNodeRequest()\n", definition.id);
        return new NodeRequest(request, nodeId);
    }

    @Override
    protected NodeBroadcastResponse newResponse(NodeBroadcastRequest request, List<NodeResponse> responses,
            List<FailedNodeException> failures) {
        if (debug)
            System.out.printf("[%s]: newResponse()\n", definition.id);
        return new NodeBroadcastResponse(definition, clusterService.getClusterName(), responses, failures);
    }

    protected void dumpResponses(AtomicReferenceArray responses) {
        System.out.printf("[%s]: received %d responses\n", definition.id, responses.length());
        for (int k = 0; k < responses.length(); k++) {
            Object resp = responses.get(k);
            System.out.printf("-- resp[%d]: type=%s\n", k, (resp == null ? "NULL" : Utils.getTrimmedClass(resp)));
        }
    }

    protected abstract TransportItemBase handleNodeRequest(NodeRequest request) throws Exception;

    @Override
    protected NodeResponse nodeOperation(NodeRequest request) throws ElasticsearchException {
        if (debug)
            System.out.printf("[%s]: nodeOperation()\n", definition.id);
        DiscoveryNode node = null;
        try {
            node = clusterService.localNode();
            return new NodeResponse(node, handleNodeRequest(request));
        } catch (Throwable e) {
            logger.error("Node ransport error: " + e.getMessage(), e);
            return new NodeResponse(node, e);
        }
    }

    @Override
    protected NodeResponse newNodeResponse() {
        if (debug)
            System.out.printf("[%s]: newNodeResponse()\n", definition.id);
        return new NodeResponse(definition.createTransportItem());
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

}
