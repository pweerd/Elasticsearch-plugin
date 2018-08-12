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

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource.Bytes.WithOrdinals;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;

public class UndupByParentsAggregator extends NumericMetricsAggregator.SingleValue {
    private final WithOrdinals[] valuesSources;
    private final ParentValueSourceConfig valuesSourceConfigs[];
    
    //Unnested docs administration
    private final Query mainDocsFilter;
    private final BitSetProducer mainDocsBitsetProducer;
    
    /** final counts that are outputted */ 
    private LongArray counts;
    
    /** final counts that are only used when doing nested to unnested undup. Will be converted into counts */ 
    private ObjectArray<DocCount> docCounts; // a count per bucket
    

    /** cached docvalues for the first level of to-parent undups */ 
    private SortedSetDocValues[]  firstLevelDocValues;

    /** holds bitset per ordinal. Only used in parent updups */
    private ObjectArray<FixedBitSet> bitsetPerBucket; // administrate all parent ords per bucket
    private int maxBucket;
    private final boolean cache_bitsets;


    protected UndupByParentsAggregator(UndupByParentsAggregatorFactory factory,
            String name, 
            SearchContext context, 
            Aggregator parent, 
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData,
            WithOrdinals[] valuesSources) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.valuesSources = valuesSources;
        this.valuesSourceConfigs = factory.valuesSourceConfigs;
        this.cache_bitsets = factory.cache_bitsets;
        
        if (valuesSources[0] == null) { //1st level is reverseNested?
            mainDocsFilter = Queries.newNonNestedFilter();
            mainDocsBitsetProducer = context.bitsetFilterCache().getBitSetProducer(mainDocsFilter);
            
            if (valuesSources.length == 1)
                this.docCounts = context.bigArrays().newObjectArray(64);
                
        } else {
            mainDocsFilter = null;
            mainDocsBitsetProducer = null;
        }
        
        maxBucket = 64; //allocate 64 bits by default and let it grow in steps of 64
        int parentLevels = valuesSources.length;
        if (valuesSources[0] == null) parentLevels--;
        
        if (parentLevels > 0) {  //we need the bucket bitsets?
            WithOrdinals first = valuesSources[0] == null ? valuesSources[1] : valuesSources[0];
            firstLevelDocValues = getDocvaluesForAllSegments (first);

            bitsetPerBucket = context.bigArrays().newObjectArray(maxBucket);
            if (UndupByParentsAggregatorBuilder.DEBUG)
                System.out.printf("Allocated %d bucket bitsets in advance for the 1st level\n", bitsetPerBucket.size());
        }

    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        LeafBucketCollector ret;
        if (valuesSources[0] == null) {  //First lvl is reverse-nested?
            ReverseNestedCollector frcCollector = valuesSources.length==1 ? new FinalReverseNestedCollector(this, ctx)
                                                                          : new IntermediateReverseNestedParentCollector(this, ctx);
            ret = (frcCollector.mainDocs == null) ? LeafBucketCollector.NO_OP_COLLECTOR : frcCollector;
        }
        else
            ret = new IntermediateParentCollector (this, ctx);
        
        if (UndupByParentsAggregatorBuilder.DEBUG)
            System.out.printf("return leafColl: %s\n", ret.getClass().getSimpleName());
        return ret;
    }

    @Override
    public double metric(long owningBucketOrd) {
        return ((int)owningBucketOrd) >= counts.size() ? 0 : counts.get(owningBucketOrd); 
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        int cnt = ((int)bucket) >= counts.size() ? 0 : (int)counts.get(bucket); 
        return new UndupByParentsInternal(name, cnt, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        if (UndupByParentsAggregatorBuilder.DEBUG) System.out.printf("-- buildEmptyAggregation\n");
        return new UndupByParentsInternal (name, 0, pipelineAggregators(), metaData());
    }

    @Override
    protected void doPostCollection() throws IOException {
        if (UndupByParentsAggregatorBuilder.DEBUG) System.out.println("POST");
        //Reverse nested only?
        if (docCounts != null) {
            if (UndupByParentsAggregatorBuilder.DEBUG) System.out.printf("POST: only revrse: converting %d counts\n", docCounts.size());
            this.counts = context.bigArrays().newLongArray(docCounts.size());
            for (int i=0; i<docCounts.size(); i++) {
                DocCount dc = docCounts.get(i);
                if (dc != null) counts.set(i, dc.count);
            }
            docCounts.close();
            return;
        }
        
        //We don't need these any more
        firstLevelDocValues = null; 
        
        int first = valuesSources[0]==null ? 1 : 0;
        
        //Check special case: we had only 1 level of parent-childs
        //In which case we can simply return the bit counts
        if (valuesSources.length - first == 1) {
            this.counts = context.bigArrays().newLongArray(bitsetPerBucket.size());
            for (int i=0; i<bitsetPerBucket.size(); i++) {
                FixedBitSet bitset = bitsetPerBucket.get(i);
                if (bitset!=null)
                    counts.set(i, bitset.cardinality());
            }
            bitsetPerBucket.close();
            return;
        }
        
        
        //OK, we had at least 1 to-parent updup operation
        // 1) process all next levels that were requested
        //    This involves looping though the lvl-1 docs
        //    check if we had the global ord for that doc, and collect the bitset to the parent-ord
        // 2) Calculate the counts based on the collected bitsets (1 bit for each bucket)
        ContextIndexSearcher searcher = context.searcher();
        IndexReaderContext rootContext = searcher.getTopReaderContext();
        int bucketNum = 1 + getMaxBucket();
        int lvl = first+1;
        for (; lvl < valuesSources.length; lvl++) {
            SortedSetDocValues[] docValuesArr = getDocvaluesForAllSegments (valuesSources[lvl]);
            int maxOrd = getMaxOrd (docValuesArr);
            
            //Allocate next level of bitset per bucket
            ObjectArray<FixedBitSet> nextBitsetPerBucket = context.bigArrays().newObjectArray(bucketNum);
            int dim = mod64(maxOrd+1);
            for (int i = 0; i<bucketNum; i++)
                if (bitsetPerBucket.get(i) != null)
                    nextBitsetPerBucket.set(i,  new FixedBitSet(dim));
                
            
            FixedBitSet combinedOrdinals = getCombinedOrdinals();
            
            Weight w = valuesSourceConfigs[lvl-1].parentFilter.createWeight(context.searcher(),  false,  1.0f);
            
            for (LeafReaderContext leaf : rootContext.leaves()) {
                final SortedSetDocValues globalOrdinals = valuesSources[lvl-1].globalOrdinalsValues(leaf);
                final SortedSetDocValues globalOrdinalsParent = docValuesArr [leaf.ord];

                final Bits liveDocs = leaf.reader().getLiveDocs();
                if (this.cache_bitsets) {
                    if (UndupByParentsAggregatorBuilder.DEBUG) System.out.printf("POST: undup via cache\n");
                    BitSet bits = context.bitsetFilterCache().getBitSetProducer(valuesSourceConfigs[lvl-1].parentFilter).getBitSet(leaf);
                    if (bits == null) continue;
                    undupSegment(bits, liveDocs, combinedOrdinals, globalOrdinals,  globalOrdinalsParent, nextBitsetPerBucket);
                } else {
                    if (UndupByParentsAggregatorBuilder.DEBUG) System.out.printf("POST: undup via doc iter\n");
                    Scorer parentScorer = w.scorer(leaf);
                    if (parentScorer == null) continue;
                    undupSegment(parentScorer.iterator(), liveDocs, combinedOrdinals, globalOrdinals, globalOrdinalsParent, nextBitsetPerBucket);
                }
            }
            this.bitsetPerBucket.close();
            this.bitsetPerBucket = nextBitsetPerBucket;
        }
        this.counts = convertBitsetPerBucketIntoCounts (bitsetPerBucket);
    }
    
    protected static int mod64 (int x) {
        return 64 * ((x + 64) / 64);
    }
    


    private int getMaxBucket () {
        int maxBucket = 0;
        for (int i=0; i<bitsetPerBucket.size(); i++) {
            if (null != bitsetPerBucket.get(i)) maxBucket = i;
        }
        return maxBucket;
    }
    private FixedBitSet getCombinedOrdinals() {
        int maxLen = 0;
        for (int i=0; i<bitsetPerBucket.size(); i++) {
            FixedBitSet b = bitsetPerBucket.get(i);
            if (b != null && b.length() > maxLen) maxLen = b.length(); 
        }
        
        FixedBitSet ret = new FixedBitSet(maxLen);
        for (int i=0; i<bitsetPerBucket.size(); i++) {
            FixedBitSet b = bitsetPerBucket.get(i);
            if (b != null) ret.or(b);;
        }
        return ret;
    }

    private void undupSegment(DocIdSetIterator iter, final Bits liveDocs,
            FixedBitSet combinedOrdinals,
            final SortedSetDocValues globalOrdinals,
            final SortedSetDocValues globalOrdinalsParent, 
            ObjectArray<FixedBitSet> nextBitsetPerBucket) throws IOException {
        while (true) {
            int docId = iter.nextDoc();
            System.out.printf ("doc=%d\n", docId); 

            if (docId == DocIdSetIterator.NO_MORE_DOCS) break;
            if (liveDocs != null && liveDocs.get(docId) == false) {
                continue;
            }

            
            if (!globalOrdinals.advanceExact(docId)) continue;
            int globalOrdinal = (int)globalOrdinals.nextOrd();
            if (globalOrdinal >= combinedOrdinals.length() || !combinedOrdinals.get(globalOrdinal)) continue;

            if (!globalOrdinalsParent.advanceExact(docId)) continue;
            int globalOrdinalParent = (int)globalOrdinalsParent.nextOrd();
            
            for (int bucket=0; bucket<this.bitsetPerBucket.size(); bucket++) {
                FixedBitSet bitset = bitsetPerBucket.get(bucket);
                if (bitset==null) continue;
                
                if (!bitset.get(globalOrdinal)) continue;
                nextBitsetPerBucket.get(bucket).set(globalOrdinalParent);
            }
        }
    }
    
    private void undupSegment(BitSet iter, final Bits liveDocs,
            final FixedBitSet combinedOrdinals,
            final SortedSetDocValues globalOrdinals,
            final SortedSetDocValues globalOrdinalsParent, 
            ObjectArray<FixedBitSet> nextBitsetPerBucket) throws IOException {
        int docId = -1;
        final int N = iter.length() -1;
        while (docId < N) {
            docId = iter.nextSetBit(docId+1);

            if (docId == DocIdSetIterator.NO_MORE_DOCS) break;
            if (liveDocs != null && liveDocs.get(docId) == false) {
                continue;
            }

            if (!globalOrdinals.advanceExact(docId)) continue;
            int globalOrdinal = (int)globalOrdinals.nextOrd();
            if (globalOrdinal >= combinedOrdinals.length() || !combinedOrdinals.get(globalOrdinal)) continue;

            if (!globalOrdinalsParent.advanceExact(docId)) continue;
            int globalOrdinalParent = (int)globalOrdinalsParent.nextOrd();
            
            for (int bucket=0; bucket<this.bitsetPerBucket.size(); bucket++) {
                FixedBitSet bitset = bitsetPerBucket.get(bucket);
                if (bitset==null) continue;
                
                if (!bitset.get(globalOrdinal)) continue;
                nextBitsetPerBucket.get(bucket).set(globalOrdinalParent);
            }
        }
    }

    private LongArray convertBitsetPerBucketIntoCounts(final ObjectArray<FixedBitSet> bitsetPerBucket) {
        LongArray ret = context.bigArrays().newLongArray(bitsetPerBucket.size());
        for (int i=0; i<bitsetPerBucket.size(); i++) {
            FixedBitSet bitset = bitsetPerBucket.get(i);
            if (bitset!=null)
                ret.set(i, bitset.cardinality());
        }
        bitsetPerBucket.close();
        return ret;
    }

    private SortedSetDocValues[] getDocvaluesForAllSegments (WithOrdinals source) throws IOException {
        List<LeafReaderContext> leaves = context.searcher().getTopReaderContext().leaves();
        SortedSetDocValues[] ret = new SortedSetDocValues[leaves.size()];
        for (int i=0; i<leaves.size(); i++) {
            LeafReaderContext leaf = leaves.get(i);
            ret[leaf.ord] = source.globalOrdinalsValues(leaf);
        }
        return ret;
    }

    private int getMaxOrd (SortedSetDocValues[] dvs) {
        int max = 0;
        for (SortedSetDocValues dv : dvs) {
            int cnt = (int) dv.getValueCount();
            if (cnt > max) max = cnt;
        }
        return max;
    }
    /**
     * Base class for all leaf collectors.
     */
    protected static abstract class CollectorBase extends LeafBucketCollector {
        final protected static boolean DEBUG=false;
        final protected UndupByParentsAggregator aggregator;
        final protected BigArrays bigArrays;
        
        /** used when undupping over a parent relation */
        protected ObjectArray<FixedBitSet> bitsetPerBucket; 
        final private int maxOrd;

        /** used when undupping over NESTED docs only */
        protected ObjectArray<DocCount> cached_counts; // a count per bucket

        protected CollectorBase (UndupByParentsAggregator aggregator, LeafReaderContext ctx) {
            this.aggregator = aggregator;
            this.cached_counts = aggregator.docCounts;
            this.bigArrays = aggregator.context.bigArrays();
            this.bitsetPerBucket = aggregator.bitsetPerBucket;
            
            this.maxOrd = aggregator.firstLevelDocValues == null ? 0 : aggregator.getMaxOrd(aggregator.firstLevelDocValues);
        }
        
        protected void incrementNestedCount (long bucket, int doc) {
            if (bucket >= cached_counts.size()) {
                cached_counts = aggregator.docCounts = bigArrays.grow(cached_counts, bucket + 1);
            }
            DocCount docCount = cached_counts.get(bucket); 
            if (docCount == null) 
                cached_counts.set(bucket, new DocCount(doc));
            else
                docCount.increment(doc);
        }
        
        protected void clearLastDocs () {
            for (int i=0; i<cached_counts.size(); i++) {
                DocCount docCount = cached_counts.get(i); 
                if (docCount != null) docCount.clearLastDoc();
            }
        }
        
        protected void administrateOrdinalInBucket (int bucket, int ord) {
            if (bucket >= bitsetPerBucket.size()) {
                aggregator.bitsetPerBucket = bitsetPerBucket = aggregator.context.bigArrays().grow(bitsetPerBucket, bucket+1);
                aggregator.maxBucket = (int)bitsetPerBucket.size();
            }
            FixedBitSet bitset = bitsetPerBucket.get(bucket);
            if (bitset == null || bitset.length() <= ord) {
                int cnt = ord<maxOrd ? maxOrd : mod64(ord+1);
                FixedBitSet tmp = new FixedBitSet (cnt);
                if (bitset != null) tmp.or(bitset);
                bitsetPerBucket.set(bucket, bitset = tmp);
            }
            bitset.set(ord);
        }
        
        protected void dumpCounts () {
            System.out.println ("Dumping counts");
            for (int i=0; i<cached_counts.size(); i++) {
                DocCount docCount = cached_counts.get(i); 
                System.out.printf("-- [%s]: %s\n", i, docCount==null ? -1: docCount.count);
            }
        }
        
    }

    /**
     * Base class for NESTED doc collectors.
     */
    protected static abstract class ReverseNestedCollector extends CollectorBase {
        protected final BitSet mainDocs;
        protected int lastDoc;

        public ReverseNestedCollector (UndupByParentsAggregator aggregator, LeafReaderContext ctx) throws IOException {
            super (aggregator, ctx);
            mainDocs = aggregator.mainDocsBitsetProducer.getBitSet(ctx);
            lastDoc = -1;
            if (DEBUG) System.out.printf("FRN COLLECT (%s): new segment, mainDocs=%s \n", getClass().getSimpleName(), mainDocs);
        }
    }

    /**
     * This leafCollector finds the docid of the unnested main-doc and
     * update the count for the bucket accordinly.
     * This collector is used only when we have ONLY undupping over nested docs.
     */
    protected static class FinalReverseNestedCollector extends ReverseNestedCollector {

        public FinalReverseNestedCollector (UndupByParentsAggregator aggregator, LeafReaderContext ctx) throws IOException {
            super (aggregator, ctx);
            clearLastDocs();
        }
        
        @Override
        public void collect(int docId, long bucket) throws IOException {
            final int mainDoc = mainDocs.nextSetBit(docId);
            assert docId <= mainDoc && mainDoc != DocIdSetIterator.NO_MORE_DOCS;
            
            if (DEBUG) System.out.printf("-- FRN COLLECT doc=%d, buck=%d, p=%d\n", docId, bucket, mainDoc);
            incrementNestedCount(bucket, mainDoc);
        }
    }

    /**
     * This leafCollector finds the docid of the unnested main-doc and
     * administrates the bucket as a bit in the parent-ord's bitset of the maindoc
     * So, the collector basically undups two levels at once.
     */
    protected static class IntermediateReverseNestedParentCollector extends ReverseNestedCollector {
        protected final SortedSetDocValues globalOrdinals;
        private int lastDoc;
        private int lastOrd;

        public IntermediateReverseNestedParentCollector (UndupByParentsAggregator aggregator, LeafReaderContext ctx) throws IOException {
            super (aggregator, ctx);
            globalOrdinals = aggregator.firstLevelDocValues[ctx.ord]; //aggregator.valuesSources[1].globalOrdinalsValues(ctx);
            lastDoc = -1; 
        }
        
        @Override
        public void collect(int docId, long bucket) throws IOException {
            try {
            final int mainDoc = mainDocs.nextSetBit(docId);
            assert docId <= mainDoc && mainDoc != DocIdSetIterator.NO_MORE_DOCS;
            
            if (DEBUG) System.out.printf("-- FRN COLLECT doc=%d, buck=%d, p=%d\n", docId, bucket, mainDoc);
            if (lastDoc != mainDoc) {
                lastDoc = mainDoc;
                if (!globalOrdinals.advanceExact(mainDoc)) 
                    lastOrd = -1;
                else {
                    lastOrd = (int)globalOrdinals.nextOrd();
                    administrateOrdinalInBucket((int) bucket, lastOrd);
                }
                return;
            }
            if (lastOrd >= 0) administrateOrdinalInBucket((int) bucket, lastOrd);
            } catch (Exception e) {
                System.out.println("bitmanager");
                e.printStackTrace();
                throw new RuntimeException (e);
            }
        }
    }

    
    
    /**
     * This leafCollector administrates the bucket as a bit in the parent-ord's bitset
     * If it was not administrated before, the doc is collected into the sub-collector
     */
    protected static class IntermediateParentCollector extends CollectorBase {
        protected final SortedSetDocValues globalOrdinals;
        private int lastDoc;
        private int lastOrd;

        protected IntermediateParentCollector (UndupByParentsAggregator aggregator, LeafReaderContext ctx) throws IOException {
            super (aggregator, ctx);
            globalOrdinals = aggregator.firstLevelDocValues[ctx.ord]; //aggregator.valuesSources[0].globalOrdinalsValues(ctx);
            lastDoc = -1; 
            if (DEBUG) System.out.printf("PAR COLLECT (%s): new segment \n", getClass().getSimpleName());
        }

        @Override
        public void collect(int docId, long bucket) throws IOException {
            if (lastDoc != docId) {
                lastDoc = docId;
                if (!globalOrdinals.advanceExact(docId)) 
                    lastOrd = -1;
                else {
                    lastOrd = (int)globalOrdinals.nextOrd();
                    administrateOrdinalInBucket((int) bucket, lastOrd);
                }
                return;
            }
            if (lastOrd >= 0) administrateOrdinalInBucket((int) bucket, lastOrd);
        }
    }


    /** Helper class to hold counts and the last docid
     *  It is used when undupping over unnested documents only
     */
    static class DocCount {
        private int lastDoc;
        public int count;
        
        public DocCount() {
            lastDoc = -1;
        }
        public DocCount(int doc) {
            lastDoc = doc;
            count = 1;
        }
        public void increment (int doc) {
            if (doc != lastDoc) {
                count++;
                lastDoc = doc;
            }
        }
        public void clearLastDoc() {
            lastDoc = -1;
        }
    }
}
