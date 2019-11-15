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

package nl.bitmanager.elasticsearch.extensions.version;

import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import nl.bitmanager.elasticsearch.extensions.Plugin;
import nl.bitmanager.elasticsearch.transport.NodeRequest;
import nl.bitmanager.elasticsearch.transport.NodeTransportActionBase;
import nl.bitmanager.elasticsearch.transport.TransportItemBase;
//ActionType<Response extends ActionResponse>
public class TransportAction extends NodeTransportActionBase {

	@Inject
	public TransportAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
			TransportService transportService, ActionFilters actionFilters,
			IndexNameExpressionResolver indexNameExpressionResolver) {
		super(ActionDefinition.INSTANCE, settings, threadPool, clusterService, transportService, actionFilters,
				indexNameExpressionResolver);
	}

	@Override
	protected TransportItemBase handleNodeRequest(NodeRequest request) throws Exception {
		VersionTransportItem ret = new VersionTransportItem(ActionDefinition.INSTANCE);
		if (debug)
			System.out.println("VersionTransportAction:handleNodeRequest");
		ret.addNodeVersion(clusterService.localNode().toString(), Plugin.version, getLocation());
		return ret;
	}

	private URL getLocation() {
		ProtectionDomain pd = getClass().getProtectionDomain();
		if (pd == null)
			return null;
		CodeSource cs = pd.getCodeSource();
		if (cs == null)
			return null;
		return cs.getLocation();
	}

}