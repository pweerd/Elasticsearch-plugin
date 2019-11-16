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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import nl.bitmanager.elasticsearch.extensions.Plugin;
import nl.bitmanager.elasticsearch.transport.ActionDefinition;
import nl.bitmanager.elasticsearch.transport.TransportItemBase;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class NodeVersion extends TransportItemBase {
    private final static Map<String, String> EMPTY_MAP = new HashMap<String, String>();

    private String node;
    private String version;
    private String location;
    private long fileSize;
    private long fileDate;
    private org.elasticsearch.Version esVersion;
    private Map<String, String> esSettings;

    NodeVersion(ActionDefinition definition) {
        super(definition);
        esVersion = org.elasticsearch.Version.CURRENT;
        esSettings = asMap (Plugin.ESSettings);
    }

    public NodeVersion(ActionDefinition definition, String node, String version, URL location) {
        super(definition);
        esSettings = asMap (Plugin.ESSettings);
        esVersion = org.elasticsearch.Version.CURRENT;
        this.node = node;
        this.version = version;
        if (location != null) {
            this.location = location.toString();
            String proto = location.getProtocol();
            if (proto != null && proto.startsWith("file")) {
                try {
                    File file = new File(location.getFile());
                    fileSize = file.length();
                    fileDate = file.lastModified();
                } catch (Exception e) {
                }
            }
        }
    }

    public NodeVersion(ActionDefinition definition, StreamInput in) throws IOException {
        super(definition, in);
        node = readStr(in);
        version = readStr(in);
        location = readStr(in);
        fileSize = in.readLong();
        fileDate = in.readLong();
        esVersion = org.elasticsearch.Version.readVersion(in);
        esSettings = readMap(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeStr(out, node);
        writeStr(out, version);
        writeStr(out, location);
        out.writeLong(fileSize);
        out.writeLong(fileDate);
        org.elasticsearch.Version.writeVersion(esVersion, out);
        writeMap(out, esSettings);
    }

    private static Map<String,String> asMap (Settings x) {
        Map<String,String> ret = new HashMap<String,String>(x.size());
        for (String key: x.keySet()) {
            ret.put(key, x.get(key));
        }
        return ret;
    }

    public String getVersion() {
        return version;
    }

    public String getNode() {
        return node;
    }

    public String getLocation() {
        return location;
    }

    public long getFileSize() {
        return fileSize;
    }

    public Date getFileDate() {
        return new Date(fileDate);
    }

    public void exportToJson(XContentBuilder builder) throws IOException {
        builder.field("node", node);
        builder.field("version", version);
        builder.field("location", location);
        builder.field("fileSize", fileSize);
        builder.field("fileDate", getFileDate());
        builder.field("esVersion", esVersion.toString());
        if (esSettings != null) {
            builder.startObject("settings");
            for (Map.Entry<String, String> entry : esSettings.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }
    }

    private static void writeDifference(XContentBuilder builder, String label, String str1, String str2)
            throws IOException {
        if (str1 != null && str1.equals(str2))
            return;
        builder.field(label, str1);
    }

    private static void writeDifference(XContentBuilder builder, String label, long n1, long n2) throws IOException {
        if (n1 == n2)
            return;
        builder.field(label, n1);
    }

    private static void writeDifference(XContentBuilder builder, String label, Date n1, Date n2) throws IOException {
        if (n1.getTime() / 1000 == n2.getTime() / 1000)
            return;
        builder.field(label, n1);
    }

    private static void writeDifference(XContentBuilder builder, String label, org.elasticsearch.Version n1,
            org.elasticsearch.Version n2) throws IOException {
        if (n1 != null && n1.equals(n2))
            return;
        builder.field(label, n1.toString());
    }

    public void exportToJson(XContentBuilder builder, NodeVersion other) throws IOException {
        if (other == null) {
            exportToJson(builder);
            return;
        }
        builder.field("node", node);
        builder.startObject("differences");
        writeDifference(builder, "version", version, other.version);
        writeDifference(builder, "location", location, other.location);
        writeDifference(builder, "fileSize", fileSize, other.fileSize);
        writeDifference(builder, "fileDate", getFileDate(), other.getFileDate());
        writeDifference(builder, "esVersion", esVersion, other.esVersion);

        Map<String, String> diff = new TreeMap<String, String>();
        getNotEqualKeys(diff, esSettings, other.esSettings);
        if (diff.size() > 0) {
            builder.startObject("settings");
            for (Map.Entry<String, String> entry : diff.entrySet()) {
                builder.field(entry.getKey(), esSettings.get(entry.getKey()));
            }
            builder.endObject();
        }
        builder.endObject();
    }

    private static Map<String, String> appendDifference(Map<String, String> differences, String key) {
        if (differences == null)
            differences = new TreeMap<String, String>();
        differences.put(key, null);
        return differences;
    }

    private static Map<String, String> getNotEqualKeys(Map<String, String> differences, Map<String, String> settings,
            Map<String, String> other) {
        if (settings == null)
            settings = EMPTY_MAP;
        if (other == null)
            other = EMPTY_MAP;
        for (Map.Entry<String, String> entry : settings.entrySet()) {
            String key = entry.getKey();
            if (key.equals("name"))
                continue;
            if (key.equals("node.name"))
                continue;
            if (key.equals("node.id"))
                continue;
            if (key.equals("node.tag"))
                continue;
            if (key.equals("node.data"))
                continue;
            if (key.equals("http.enabled"))
                continue;
            String valOther = other.get(key);
            if (valOther == null) {
                differences = appendDifference(differences, key);
                continue;
            }
            if (valOther.equals(entry.getValue()))
                continue;
            differences = appendDifference(differences, key);
        }
        return differences;
    }

    public String getDifference(Map<String, String> settingsDifferences, NodeVersion other) {
        getNotEqualKeys(settingsDifferences, esSettings, other.esSettings);

        if (esVersion == null)
            return "ES-Version";
        if (!esVersion.equals(other.esVersion))
            return "ES-Version";

        if (location == null)
            return "location";
        if (!location.equals(other.location))
            return "location";

        if (fileSize == 0 || fileSize != other.fileSize)
            return "size";

        // if (fileDate == 0 || fileDate != other.fileDate) return "date";

        return null; // no differencees
    }

    @Override
    protected void consolidateResponse(TransportItemBase other) {
        // Nothing to do here
    }
}
