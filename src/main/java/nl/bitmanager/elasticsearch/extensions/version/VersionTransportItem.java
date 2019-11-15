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

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import nl.bitmanager.elasticsearch.extensions.Plugin;
import nl.bitmanager.elasticsearch.transport.ActionDefinition;
import nl.bitmanager.elasticsearch.transport.TransportItemBase;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class VersionTransportItem extends TransportItemBase {
	protected TreeMap<String, NodeVersion> nodeVersions;

    public VersionTransportItem(ActionDefinition definition) {
        super(definition);
    }

    public VersionTransportItem(ActionDefinition definition, StreamInput in) throws IOException {
        super(definition, in);
        int n = in.readInt();
        if (n == 0)
            return;

        nodeVersions = createMap();
        for (int i = 0; i < n; i++) {
            NodeVersion tmp = new NodeVersion(definition, in);
            nodeVersions.put(tmp.getNode(), tmp);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        int n = nodeVersions == null ? 0 : nodeVersions.size();
        out.writeInt(n);
        if (n == 0)
            return;

        for (Entry<String, NodeVersion> kvp : nodeVersions.entrySet()) {
            kvp.getValue().writeTo(out);
        }
    }

    public Map<String, NodeVersion> getNodeVersions() {
		return nodeVersions;
	}

	private static TreeMap<String, NodeVersion> createMap() {
		return new TreeMap<String, NodeVersion>();
	}

	public void addNodeVersion(String node, String version, URL location) {
		if (nodeVersions == null) {
			nodeVersions = createMap();
		}
		if (nodeVersions.containsKey(node))
			return;
		nodeVersions.put(node, new NodeVersion(definition, node, version, location));
	}

	@Override
	public void consolidateResponse(TransportItemBase _other) {
		VersionTransportItem other = (VersionTransportItem) _other;
		if (other.nodeVersions == null)
			return;

		if (nodeVersions == null) {
			nodeVersions = createMap();
		}
		for (Entry<String, NodeVersion> kvp : other.nodeVersions.entrySet()) {
			NodeVersion nv = kvp.getValue();
			String node = nv.getNode();
			if (nodeVersions.containsKey(node))
				continue;
			nodeVersions.put(node, nv);
		}
	}

	public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
		builder.field("plugin-version", Plugin.version);

		// Create a map to collect all keys of the settings that differ
		Map<String, String> settingDifferences = new TreeMap<String, String>();

		// Check version differences
		boolean allVersionsOK = true; // response.getFailedShards() == 0;
		NodeVersion first = null;
		NodeVersion firstDiff = null;
		String diffReason = null;
		int activeNodes = 0;
		if (nodeVersions != null) {
			activeNodes = nodeVersions.size();
			for (Entry<String, NodeVersion> kvp : nodeVersions.entrySet()) {
				NodeVersion nv = kvp.getValue();
				if (first == null) {
					first = nv;
					continue;
				}
				if (firstDiff != null)
					continue;

				diffReason = first.getDifference(settingDifferences, nv);
				if (diffReason == null)
					continue; // no diff...

				allVersionsOK = false;
				firstDiff = nv;
				break;
			}
		}
		if (first == null)
			allVersionsOK = false;

		// export node versions
		builder.field("all-versions-ok", allVersionsOK);
		if (settingDifferences.size() > 0)
			builder.field("settings-differences", getDifferencesAsString(settingDifferences));
		if (firstDiff != null) {
			builder.startObject("version-diff");
			builder.field("node", firstDiff.getNode());
			builder.field("reason", diffReason);
			builder.endObject();
		}

		builder.field("activeNodes", activeNodes);
		builder.startArray("nodes");
		if (nodeVersions != null) {
			first = null;
			for (Entry<String, NodeVersion> kvp : nodeVersions.entrySet()) {
				NodeVersion nv = kvp.getValue();
				builder.startObject();
				nv.exportToJson(builder, first);
				builder.endObject();
				if (first == null)
					first = nv;
			}
		}
		builder.endArray();
		return builder;
	}

	private static String getDifferencesAsString(Map<String, String> differences) {
		if (differences == null || differences.size() == 0)
			return null;

		StringBuilder bldr = new StringBuilder();
		for (Map.Entry<String, String> entry : differences.entrySet()) {
			if (bldr.length() > 0)
				bldr.append(", ");
			bldr.append(entry.getKey());
		}
		return bldr.toString();
	}

}
