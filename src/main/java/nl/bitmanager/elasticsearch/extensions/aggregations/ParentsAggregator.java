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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource.Bytes.ParentChild;
import org.elasticsearch.search.internal.SearchContext;

import nl.bitmanager.elasticsearch.extensions.aggregations.ParentsAggregatorBuilder.AggregatorMode;

/**
 * This aggregator supports 2 modi:
 * - updup: we undup the buckets by their parent. (No guarantee that the docid passed to the next lvl are parents!)
 * - map:   we map the docid's to their parents, and output them to the next level. (slower, but docids are guaranteed to be parents)
 * 
 * The aggregator can work on 1 or more levels.
 * If the levels are 1, an optimized path is choosen. We don't need to enumerate all the parent records (unless mode=map).
 * However, for multiple levels, we need to enumerate parents at all levels to match their ordinals. (which are used for the matching)
 *   In this case the highest level will be directly outputted to the sub-aggregators.
 *   It will save us enumerating parants for the top-level.
 *   
 * The mapping to the parent docids is done in the post processing  
 */
public class ParentsAggregator extends SingleBucketAggregator {
    private final String types[];
    private final Query typeFilters[];
    private final Weight typeWeights[];
    private final ParentChild valuesSources[];
    private final int levels;
    public final AggregatorMode mode;
    private final boolean needParentDocs;

    
    private HashMap<Long,FixedBitSet> curBuckets;


    public ParentsAggregator(ParentsAggregatorFactory factory, String name, AggregatorFactories factories, 
            SearchContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData,
            ParentChild[] valuesSources)
            throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);
        this.types = factory.types;
        this.typeFilters = factory.typeFilters;
        this.valuesSources = valuesSources;
        this.typeWeights = new Weight[this.typeFilters.length];
        this.mode = factory.mode;
        this.needParentDocs = factory.mode == AggregatorMode.MapToParent;
//        for (int i=0; i<this.typeFilters.length; i++) {
//            System.out.printf("\n[%d]: type=%s, filter=%s, src=%s\n", i, types[i], typeFilters[i], valuesSources[i]);
//        }
        for (int i=0; i<this.typeFilters.length; i++) {
            typeWeights[i] = context.searcher().createNormalizedWeight(this.typeFilters[i],  false); 
        }
        this.levels = factory.levels;
        this.curBuckets = new HashMap<Long,FixedBitSet>();
    }
    
    @Override
    public boolean needsScores() {
        return false;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        if (valuesSources[1] == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        final SortedDocValues globalOrdinals = valuesSources[1].globalOrdinalsValues(types[1], ctx);
        assert globalOrdinals != null;
        final Bits childDocs = context.bitsetFilterCache().getBitSetProducer(typeFilters[0]).getBitSet (ctx);

        //System.out.printf("Create leafCollector docbase=%d\n", ctx.docBase);
        return this.levels > 1 || needParentDocs  ? new NonCollectingBucketCollector (this, globalOrdinals, childDocs)
                                                  : new CollectingBucketCollector (this, globalOrdinals, childDocs, sub);
    }
    
    /**
     * This leafCollector only administrates a collected document in the curBuckets of the parent aggregator
     */
    protected static class NonCollectingBucketCollector extends LeafBucketCollector {
        final protected static boolean DEBUG=false;
        
        final protected Bits childDocs;
        final protected SortedDocValues globalOrdinals;
        final protected HashMap<Long,FixedBitSet> curBuckets;
        final protected ParentsAggregator aggregator;

        final protected int maxOrd;
        
        public NonCollectingBucketCollector (ParentsAggregator aggregator, SortedDocValues globalOrdinals, Bits childDocs) {
            this.childDocs = childDocs;
            this.globalOrdinals = globalOrdinals;
            this.aggregator = aggregator;
            this.curBuckets = aggregator.curBuckets;
            this.maxOrd = 1+globalOrdinals.getValueCount();
        }
        
        @Override
        public void collect(int docId, long bucket) throws IOException {
            if (DEBUG) System.out.printf("-- NC: doc %d. IsChild=%s bucket=%d\n", docId, childDocs.get(docId), bucket);
            if (childDocs.get(docId)) {
                long globalOrdinal = globalOrdinals.getOrd(docId);
                if (DEBUG) System.out.printf("-- NC: doc %s -> %d\n", docId, globalOrdinal);
                Long b = bucket;
                FixedBitSet bitset = curBuckets.get(b);
                if (bitset==null) {
                    bitset = new FixedBitSet (maxOrd);
                    curBuckets.put(b, bitset);
                }
                bitset.set ((int)globalOrdinal);
            }
        }
    }

    /**
     * This leafCollector administrates a collected document in the curBuckets of the parent aggregator.
     * If it was not administrated before, the doc is collected into the sub-collector
     */
    protected static class CollectingBucketCollector extends NonCollectingBucketCollector {
        protected final LeafBucketCollector sub;
        public CollectingBucketCollector (ParentsAggregator aggregator, SortedDocValues globalOrdinals, Bits childDocs, LeafBucketCollector sub) {
            super (aggregator, globalOrdinals, childDocs);
            this.sub = sub;
        }
        
        @Override
        public void collect(int docId, long bucket) throws IOException {
            if (DEBUG) System.out.printf("-- C: doc %d. IsChild=%s bucket=%d\n", docId, childDocs.get(docId), bucket);
            if (childDocs.get(docId)) {
                long globalOrdinal = globalOrdinals.getOrd(docId);
                if (DEBUG) System.out.printf("-- C: doc %s -> %d\n", docId, globalOrdinal);
                Long b = bucket;
                FixedBitSet bitset = curBuckets.get(b);
                if (bitset==null) {
                    bitset = new FixedBitSet (maxOrd);
                    curBuckets.put(b, bitset);
                }
                if (!bitset.get((int)globalOrdinal)) {
                    bitset.set ((int)globalOrdinal);
                    aggregator.collectBucket(sub, docId, bucket);
                }
            }
        }
    }
    
    /**
     * The curBuckets are organized as a bitset of docs(=ordinals) per bucket.
     * However, for our purpose we need the inversed matrix: buckets per doc (well, ordinal)
     */
    protected long invertDocsAndBuckets (LongObjectPagedHashMap<FixedBitSet> dst, HashMap<Long,FixedBitSet> src) {
        long maxBucket = -1;
        Set<Long> buckets = src.keySet();
        for (long k: buckets) if (k>maxBucket) maxBucket = k;
        for (Long b: buckets) {
            long bucket = b;
            FixedBitSet bitset = src.get(b);
            int docbit = -1;
            final int maxbit = bitset.length()-1;
            while (true) {
                if (docbit >= maxbit) break;
                docbit = bitset.nextSetBit(docbit+1);
                if (docbit > maxbit) break;
                FixedBitSet bucketBits = dst.get(docbit);
                if (bucketBits==null) {
                    bucketBits = new FixedBitSet ((int) maxBucket+1);
                    dst.put(docbit, bucketBits);
                }
                bucketBits.set((int)bucket);
            }
        }
        return maxBucket;
    }
    
    protected static void dumpDocument (String why, LeafReaderContext ctx, int docid) throws IOException {
        Document d = ctx.reader().document(docid);
        if (d==null) {
            System.out.printf("%s: Doc [%d] in reader [base=%d, ord=%d] not found.\n", why, docid, ctx.docBase, ctx.ord);
            return;
        }
        String uid = d.get("_uid");
        System.out.printf("%s: Doc [%d] in reader [base=%d, ord=%d]: [%s].\n", why, docid, ctx.docBase, ctx.ord, uid);
    }
 
    @Override
    protected void doPostCollection() throws IOException {
        //We only need to do postprocessing when we have a multi-level request, or when we need to fetch the parent docs 
        if (levels <= 1 && !needParentDocs) return;
        
        BigArrays bigArrays = context.bigArrays();
        LongObjectPagedHashMap<FixedBitSet> bucketsPerOrd = new LongObjectPagedHashMap<FixedBitSet> (bigArrays);
        int maxBucket = (int)invertDocsAndBuckets (bucketsPerOrd, curBuckets);
        if (ParentsAggregatorBuilder.DEBUG) System.out.printf("POST levels=%d, types=%d, srcs=%d, maxbucket=%d, curBuckets=%d\n", levels, types.length, valuesSources.length, maxBucket, curBuckets.size());
        curBuckets = null;
        
        if (maxBucket < 0) {
            if (ParentsAggregatorBuilder.DEBUG) System.out.println("Nothing to do here...");
            bucketsPerOrd.close();
            return;
        }
       
        
        IndexReader indexReader = context().searcher().getIndexReader();

        //Loop through all levels to collect the ordinals and administrate them  
        int lvl;
        for (lvl = 1; lvl < levels; lvl++) {
            @SuppressWarnings("resource")
            LongObjectPagedHashMap<FixedBitSet> nextBucketsPerOrd = new LongObjectPagedHashMap<FixedBitSet> (bigArrays);
            Weight w = typeWeights[lvl];
            if (ParentsAggregatorBuilder.DEBUG) System.out.printf("-- Handling child=%s, parent=%s\n", types[lvl], types[lvl+1]);
            
            for (LeafReaderContext ctx : indexReader.leaves()) {
                Scorer parentScorer = w.scorer(ctx);
                if (parentScorer == null) {
                    continue;
                }
                DocIdSetIterator iter = parentScorer.iterator();

                LeafBucketCollector sub = null;
                if (lvl == levels-1 && !needParentDocs) { //Highest level? We should output...
                    sub = collectableSubAggregators.getLeafCollector(ctx);
                    sub.setScorer(new ConstantScoreScorer(null, 1, iter));
                }
                final SortedDocValues globalOrdinals = valuesSources[lvl].globalOrdinalsValues(types[lvl], ctx);
                final SortedDocValues globalOrdinalsParent = valuesSources[lvl+1].globalOrdinalsValues(types[lvl+1], ctx);

                final Bits liveDocs = ctx.reader().getLiveDocs();
                while (true) {
                    int docId = iter.nextDoc();
                    if (docId == DocIdSetIterator.NO_MORE_DOCS) break;
                    if (liveDocs != null && liveDocs.get(docId) == false) {
                        continue;
                    }

                    long globalOrdinal = globalOrdinals.getOrd(docId); 
                    if (globalOrdinal < 0) continue;
                    long globalOrdinalParent = globalOrdinalsParent.getOrd(docId); 
                    if (globalOrdinalParent < 0) continue;
                    
                    FixedBitSet bucketBits = bucketsPerOrd.get(globalOrdinal);
                    if (bucketBits == null) continue;

                    FixedBitSet bucketBitsParent = nextBucketsPerOrd.get(globalOrdinalParent);
                    if (bucketBitsParent == null) {
                        bucketBitsParent = new FixedBitSet (maxBucket+1);
                        nextBucketsPerOrd.put(globalOrdinalParent, bucketBitsParent);
                    }
                    
                    if (sub==null) {
                        //System.out.println("sub==null");
                        bucketBitsParent.or(bucketBits);
                        continue;
                    }
                    
                    int bucket = -1;
                    final int maxbit = bucketBits.length()-1;
                    while (true) {
                        if (bucket >= maxbit) break;
                        bucket = bucketBits.nextSetBit(bucket+1);
                        if (bucket > maxbit) break;
                        
                        if (bucketBitsParent.get(bucket)) continue;
                        
                        bucketBitsParent.set(bucket);
                        collectBucket(sub, docId, bucket);
                    }

                }
            }
            bucketsPerOrd.close();
            bucketsPerOrd = nextBucketsPerOrd;
            nextBucketsPerOrd = null;
        }

        //If needed, do a last scan through the highest level parents to match the ordinals and output the undupped buckets
        if (needParentDocs) {
            Weight w = typeWeights[lvl];
            if (ParentsAggregatorBuilder.DEBUG) System.out.printf("-- Mapping parent=%s\n", types[lvl]);
            
            for (LeafReaderContext ctx : indexReader.leaves()) {
                Scorer parentScorer = w.scorer(ctx);
                if (parentScorer == null) {
                    continue;
                }
                DocIdSetIterator iter = parentScorer.iterator();

                LeafBucketCollector sub = collectableSubAggregators.getLeafCollector(ctx);
                sub.setScorer(new ConstantScoreScorer(null, 1, iter));
                final SortedDocValues globalOrdinals = valuesSources[lvl].globalOrdinalsValues(types[lvl], ctx);

                final Bits liveDocs = ctx.reader().getLiveDocs();
                while (true) {
                    int docId = iter.nextDoc();
                    if (docId == DocIdSetIterator.NO_MORE_DOCS) break;
                    if (liveDocs != null && liveDocs.get(docId) == false) {
                        continue;
                    }
                    //dumpDocument ("parent", ctx, docId);

                    long globalOrdinal = globalOrdinals.getOrd(docId); 
                    if (globalOrdinal < 0) continue;
                    
                    FixedBitSet bucketBits = bucketsPerOrd.get(globalOrdinal);
                    if (bucketBits == null) continue;

                    int bucket = -1;
                    final int maxbit = bucketBits.length()-1;
                    while (true) {
                        if (bucket >= maxbit) break;
                        bucket = bucketBits.nextSetBit(bucket+1);
                        if (bucket > maxbit) break;
                        
                        collectBucket(sub, docId, bucket);
                    }

                }
            }
        }
        
        bucketsPerOrd.close();
        bucketsPerOrd = null;
    }


    @Override
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        if (ParentsAggregatorBuilder.DEBUG) System.out.printf("buildinternal (%d counts %d)\n", bucket, bucketDocCount(bucket));
        return new InternalParentsAggregation(name, bucketDocCount(bucket), bucketAggregations(bucket), pipelineAggregators(),
                    metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        if (ParentsAggregatorBuilder.DEBUG) System.out.printf("-- buildEmptyAggregation\n");
        return new InternalParentsAggregation(name, 0, buildEmptySubAggregations(), pipelineAggregators(), metaData());
    }

}
