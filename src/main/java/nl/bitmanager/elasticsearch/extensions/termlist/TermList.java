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

package nl.bitmanager.elasticsearch.extensions.termlist;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import nl.bitmanager.elasticsearch.support.BytesHelper;
import nl.bitmanager.elasticsearch.support.IntRange;
import nl.bitmanager.elasticsearch.support.RegexReplacers;
import nl.bitmanager.elasticsearch.transport.TransportItemBase;
import nl.bitmanager.elasticsearch.typehandlers.TypeHandler;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

@SuppressWarnings("serial")
public class TermList extends TreeMap<byte[], TermElt> {
    private BytesRef rawBytes;
    private HashMap<String, FieldInfoItem> fields;
    private String es_type;
    
    public TermList(SortType sort) {
        super((sort.order == (SortType.SORT_REVERSE | SortType.SORT_TERM))  ? BytesHelper.bytesComparer : BytesHelper.bytesComparerRev);
        fields = new HashMap<String, FieldInfoItem>();
    }

    public String getType() {
        return es_type;
    }

    public void setType(String type) {
        es_type = type;
    }

    public void addField(FieldInfoItem fld) {
        FieldInfoItem existing = fields.get(fld.key);
        if (existing != null)
            existing.consolidate(fld);
        else
            fields.put(fld.key, fld);
    }

    public TermElt add(byte[] term, boolean owned, int cnt) {
        TermElt elt = get(term);
        if (elt == null) {
            if (!owned)
                term = Arrays.copyOf(term, term.length);
            elt = new TermElt(term, cnt);
            put(term, elt);
            return elt;
        }
        elt.addCount(cnt);
        return elt;
    }

    public TermElt add(TermElt elt) {
        TermElt other = get(elt.term);
        if (other == null) {
            put(elt.term, elt);
            return elt;
        }
        other.addCount(elt.count);
        return other;
    }

    public void combine(TermList other) {
        if (es_type == null)
            es_type = other.es_type;
        for (Entry<byte[], TermElt> kvp : other.entrySet()) {
            TermElt elt = kvp.getValue();
            add(elt.term, true, elt.count);
        }
        for (Entry<String, FieldInfoItem> kvp : other.fields.entrySet()) {
            addField(kvp.getValue());
        }
    }

    public BytesRef getRawBytes() {
        return rawBytes;
    }

    public void setRawBytes(BytesRef bytes) {
        rawBytes = bytes;
    }

    public int getRawBytesLength() {
        return rawBytes == null ? 0 : rawBytes.length;
    }

    public void saveToStream(StreamOutput out) throws IOException {
        TransportItemBase.writeStr(out, es_type);
        out.writeInt(fields.size());
        for (java.util.Map.Entry<String, FieldInfoItem> kvp : fields.entrySet()) {
            kvp.getValue().writeTo(out);
        }

        out.writeInt(size());
        for (java.util.Map.Entry<byte[], TermElt> kvp : entrySet()) {
            kvp.getValue().saveToStream(out);
        }
        System.out.println("TermList::saveToStream saved " + size() + " items");
        out.writeBytesRef(rawBytes);
    }

    public void loadFromStream(StreamInput in) throws IOException {
        es_type = TransportItemBase.readStr(in);
        int n = in.readInt();
        for (int i = 0; i < n; i++) {
            addField(new FieldInfoItem(in));
        }

        n = in.readInt();
        for (int i = 0; i < n; i++) {
            add(new TermElt(in));
        }
        System.out.println("TermList::loadFromStream loaded " + size() + " items, n=" + n);
        rawBytes = in.readBytesRef();
    }

    private List<TermElt> createSortedTerms(SortType sort, TermListLimits limits) {
        List<TermElt> arr = new ArrayList<TermElt>(size());
        IntRange range = limits.countRange;
        for (Entry<byte[], TermElt> kvp : entrySet()) {
            TermElt te = kvp.getValue();
            if (range==null || range.isInRange(te.count))
                arr.add(te);
        }

        Collections.sort(arr, (sort.order & SortType.SORT_REVERSE) == 0 ? new AscCountComparator() : new DescCountComparator());
        return arr;
    }

    private int getMaxCount() {
        int cnt = 0;
        for (Entry<byte[], TermElt> kvp : entrySet()) {
            int tmp = kvp.getValue().count;
            if (tmp > cnt)
                cnt = tmp;
        }
        return cnt;
    }

    private int getFilteredCount(IntRange range) {
        if (range==null) return size();
        int cnt = 0;
        for (Entry<byte[], TermElt> kvp : entrySet()) {
            if (range.isInRange(kvp.getValue().count))
                cnt++;
        }
        return cnt;
    }

    public Collisions buildCollisions(TermlistTransportItem request) {
        Collisions list = new Collisions();

        RegexReplacers replacers = request.getReplacers();
        if (replacers == null)
            return list;

        boolean collisionsOnly = request.getCollisionsOnly();
        for (Entry<byte[], TermElt> kvp : entrySet()) {
            TermElt term = kvp.getValue();
            String replaced = replacers.replace(term.termAsString());
            // System.out.println(String.format("REPL '%s'-->'%s'", term.term,
            // replaced));
            if (replaced == null)
                continue;

            TermElt collision = this.get(replaced);
            if (collision == null) {
                if (collisionsOnly)
                    continue;
                collision = new TermElt(replaced);
            }

            list.add(new CollisionElt(term, collision));
        }
        return list;
    }

    public void exportToJson(XContentBuilder builder, TermlistTransportItem request, TypeHandler th) throws IOException {
        exportToJson(builder, request, new TermListLimits(this, request), th);
        // SortType sort = request.getSortType();
        // int resultLimit = request.getResultLimit();
        //
        // //Interpret supplied counts
        // // - values between 0 and 1 are interpreted as a percentage of the
        // maxOccCount
        // // - a negative value for minCount means that it is computed from the
        // end of the counts interval
        // // - other values are interpreted as is.
        // // A value of zero means a No-OP
        // int maxOccCount = 0;//this.getMaxCount();
        // double _minC = request.getMinCount();
        // double _maxC = request.getMaxCount();
        // if (_minC < 0.0 || (_minC > 0 && _minC < 1.0) || (_maxC > 0 && _maxC
        // <
        // 1.0))
        // maxOccCount = this.getMaxCount();
        //
        // int minCount = doubleToCount (_minC, maxOccCount);
        // int maxCount = doubleToCount (_maxC, maxOccCount);
        // if (minCount < 0) minCount += maxOccCount;
        // System.out.println(String.format("After interpret: minCount=%d,
        // maxCount=%d, maxOcc=%d",
        // minCount, maxCount, maxOccCount));
        //
        // if (sort==SortType.sortString) {
        // for (Entry<String, TermElt> kvp: entrySet()) {
        // kvp.getValue().exportToJson(builder, minCount, maxCount);
        // if (resultLimit>0 && --resultLimit==0) break;
        // }
        // return;
        // }
        //
        // TermElt[] arr = createSortedTerms (sort);
        // for (int i=0; i<arr.length; i++) {
        // arr[i].exportToJson(builder, minCount, maxCount);
        // if (resultLimit>0 && --resultLimit==0) break;
        // }
    }

    public void exportFieldsToJson(XContentBuilder builder) throws IOException {
        TreeMap<String, List<FieldInfoItem>> map = new TreeMap<String, List<FieldInfoItem>>();
        for (Entry<String, FieldInfoItem> kvp : fields.entrySet()) {
            FieldInfoItem item = kvp.getValue();
            List<FieldInfoItem> list = map.get(item.index);
            if (list == null) {
                list = new ArrayList<FieldInfoItem>();
                map.put(item.index, list);
            }
            list.add(item);
        }

        for (java.util.Map.Entry<String, List<FieldInfoItem>> kvp : map.entrySet()) {
            List<FieldInfoItem> list = kvp.getValue();
            Collections.sort(list, cbSortCaseInsensitive);
            builder.startObject(kvp.getKey());
            for (FieldInfoItem item : list) {
                item.toXContent(builder);
            }
            builder.endObject();
        }
    }

    public static final Comparator<FieldInfoItem> cbSortCaseInsensitive = new Comparator<FieldInfoItem>() {
        public int compare(FieldInfoItem a, FieldInfoItem b) {
            return a.name.compareToIgnoreCase(b.name);
        }
    };

    public void exportToJson(XContentBuilder builder, TermlistTransportItem request, TermListLimits limits, TypeHandler th) throws IOException {
        SortType sort = request.getSortType();
        int resultLimit = request.getResultLimit();

        IntRange range = limits.countRange;
        if ((sort.order & SortType.SORT_TERM) != 0) {
            for (Entry<byte[], TermElt> kvp : entrySet()) {
                TermElt v = kvp.getValue();
                if (range != null && !range.isInRange(v.count))
                    continue;

                v.exportToJson(builder, th);
                if (resultLimit > 0 && --resultLimit == 0)
                    break;
            }
            return;
        }

        List<TermElt> arr = createSortedTerms(sort, limits);
        int N = arr.size();
        if (resultLimit > 0 && resultLimit < N) N = resultLimit;
        for (int i = 0; i < N; i++) {
            arr.get(i).exportToJson(builder, th);
        }
    }

    public void writeRawBytes(OutputStream out) throws IOException {
        if (rawBytes == null)
            return;
        out.write(rawBytes.bytes, 0, rawBytes.length);
    }

    public BytesRef exportToText(SortType sort) throws IOException {
        return rawBytes != null ? rawBytes : new BytesRef(BytesRef.EMPTY_BYTES);
        // StringBuilder bldr = new StringBuilder (16*1024);
        // if (sort==SortType.sortString) {
        // for (Entry<String, TermElt> kvp: entrySet()) {
        // kvp.getValue().exportToText(bldr);
        // }
        // return new BytesRef (bldr);
        // }
        //
        // TermElt[] arr = createSortedTerms (sort);
        // for (int i=0; i<arr.length; i++) {
        // arr[i].exportToText(bldr);
        // }
        // return new BytesRef (bldr);
    }

    private static class AscCountComparator implements Comparator<TermElt> {

        @Override
        public int compare(TermElt arg0, TermElt arg1) {
            int rc = arg0.count - arg1.count;
            return rc != 0 ? rc : BytesHelper.bytesComparer.compare(arg0.term, arg1.term);
        }
    }

    private static class DescCountComparator implements Comparator<TermElt> {

        @Override
        public int compare(TermElt arg0, TermElt arg1) {
            int rc = arg1.count - arg0.count;
            return rc != 0 ? rc : BytesHelper.bytesComparer.compare(arg0.term, arg1.term);
        }
    }

    @Override
    public String toString() {
        return String.format("Termlist with %d items, rawBytes=%d bytes.", size(), getRawBytesLength());
    }

    public TermListLimits getLimits(TermlistTransportItem request) {
        return new TermListLimits(this, request);
    }

    public static class TermListLimits {
        public final int maxOccCount;
        public final IntRange countRange;//, lengthRange;
        public final int itemCount;
        public final int filteredItemCount;

        public TermListLimits(TermList list, TermlistTransportItem request) {
            maxOccCount = list.getMaxCount();
            countRange = request.getCountRange();
            filteredItemCount = list.getFilteredCount(countRange);
            itemCount = list.size();
        }

        public void exportToJson(XContentBuilder builder) throws IOException {
            builder.field("count", countRange==null ? "" : countRange.toString());
            builder.field("maxOccCount", maxOccCount);
            builder.field("itemCount", itemCount);
            builder.field("filteredItemCount", filteredItemCount);
        }
    }


}
