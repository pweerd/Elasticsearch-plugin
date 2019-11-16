# <a name="top"></a>Bitmanager's Elasticsearch Plugin

Elasticsearch is a great search engine. I love it. Always surprised how good it is!
This plugin helped me to explore a lot of the internals. It extends Elasticsearch with a few endpoints and supplies a custom similarity.

## Installation
This plugin needs Elasticsearch 7.0 or higher. 
Goto [http://bitmanager.nl/es], download the appropriate version and copy it under the plugin directory of ll nodes in your cluster.
On the above location you can also find older versions of the plugin, but no sourcecode is available for them.

## Introduction

This plugin adds following functionality:  
**Queries**  
* [bm_match_deleted query](#match_deleted)
* [bm\_match\_nested query](#match_nested) 
* [bm\_allow\_nested query](#allow_nested)

**Aggregations**    
* [bm_undup_by_parents aggregation](#bm_parents)

**Similarity** 
* [Similarity plugin](#similarity)


**Diagnostic**  
* [_termlist](#_termlist)
* [_view](#_view)
* [_bm](#_bm)
* [_bm/version](#_bm_version)
* [_bm/cache/dump](#_bm_cache_dump)
* [diagnostics/docvalues fetcher](#diagnostics_fetcher)

<a name="priv"></a>
Some functionality, like the \_bm/cache/dump api and the bm\_undup\_by\_parents aggregation is dependent on the plugin being installed with some privileges, because not everything was open in Elasticsearch. For instance, Elasticsearch's parent-child functionaity is now running in its own module, not accessible from any plugin.  
The following privileges (in the file 'plugin-security.policy') are neededi:

```
grant {
  permission java.lang.RuntimePermission "accessDeclaredMembers";
  permission java.lang.reflect.ReflectPermission "suppressAccessChecks";
};
```
If these privileges are not enabled, you will get runtime errors when using either the \_bm/cache/dump api or the bm\_undup\_by\_parents aggregation. The rest should work as advertised.

## _bm
If the caller expected json, a small json excerpt is show, together with some samples. Otherwise this api will redirect to the github readme of this project. (http://localhost:9200/_bm)

## <a name="bm_version"></a>_bm/version
This entrypoint checks if this plugin can be found on all nodes and it checks if the version and filesize is the same on all nodes.
As a convenience, it dumps all differences in the eleasticsearch.yml over the nodes.



## <a name="match_deleted"></a>bm\_match\_deleted query
The match_deleted query matches by default all deleted documents. It is a way to view the deleted documents in the index.
It supports a sub-query that enables to search within deleted documents.

Example to find all deleted documents:

```javascript
{
    "query": {
        "bm_match_deleted": {}
    }
}
```
Example to find all deleted documents that match a certain term:

```javascript
{
    "query": {
        "bm_match_deleted": {
            "query": {
                "term": {"field": "value"}
            }
        }
    }   
}
```
[Back to the top](#top)

## <a name="match_nested"></a>bm\_match\_nested query
When a query is execute by Elasticsearch, it first checks if the query *can* return nested records. If so, it activates a special filter that matches only non-nested documents. So, doing a term query for a nested field will return 0 hits because of this automatic filter.

The bm\_match_nested query prevents this automatic filter and replaces it by a filter containing all nested documents.
This can be useful for diagnostics, but it is mainly useful when you combine the query with the bm\_undup\_by\_parents aggregation.   It can save you a nested/reverse nested aggregation. 

Example to find all nested documents:

```javascript
    {
        "query": {
            "bm_match_nested": {}
        }
    }
```
Example to find all nested documents that match a certain term (note that this can also be done with the bm\_allow\_nested query):

```javascript
{
    "query": {
        "bm_match_nested": {
            "query": {
                "term": {"field": "value"}
            }
        }
    }   
}
```
Example to aggregate over all nested terms and undup them over a parent 'parent_rel':

```javascript
{
    "query": {
        "bm_match_nested": {}
    }   
    "aggs": {
       "raw": {
           "terms": {
               "field": "nested.other_field",
               "size":10
       },
       "aggs": {
           "undupped": {
               "bm_undup_by_parents": {
                   "parent_paths": "_nested_,parent_rel"
               }
           }
       }
   }
}
```

[Back to the top](#top)

## <a name="allow_nested"></a>bm\_allow_nested query
When a query is execute by Elasticsearch, it first checks if the query *can* return nested records. If so, it activates a special filter that matches only non-nested documents. So, doing a term query for a nested field will return 0 hits because of this automatic filter.

The bm\_allow_nested query prevents this automatic filter and allows you to fetch nested documents.
This is not that useful, but in combination with aggregations it can save you a nested/reverse nested aggregation. Also, the bm\_undup\_by\_parent works nicely together with this query..

Example to find all nested documents:

```javascript
{
    "query": {
        "bm_allow_nested": {
            "query": {
                "term": {"nested.field": "value"}
            }
        }
    }
    "aggs": {
       "raw": {
           "terms": {
               "field": "nested.other_field",
               "size":10
       },
       "aggs": {
           "undupped": {
               "bm_undup_by_parents": {
                   "parent_paths": "_nested_"
               }
           }
       }
   }
}
```
[Back to the top](#top)

## <a name="bm_parents"></a>bm\_undup\_by_parents aggregation
The bm\_undup\_by\_parents aggregation is a metric aggregation that undups bucket counts of either a nested parent , a parent in a join relation or a combination of both.

The aggregator needs the pathes over which to be de-deplicate. Those pathes should be separated by ',' or ';'.
The path '\_nested_' is used to indicate undupping over  nested parent.
If the aggregator encounters a record that does not have a parent in the specified pathes, it is ignored.

In case of the '\_nested\_' you should use the bm\_allow_nested query to enable the matching of nested documents.
The advantage is that a nested query/nested agg/reverse nested agg are no needed.
The disadvantage is that the hits could consist of nested documents, depending on the wrapped query. Be aware of that!

Example.
Suppose we have employee-records that have company-records as their parents. We want to have an aggregation on the age of employees, but counted by their companies. Answering questions like: how many companies have employees with an age of 25.

```javascript
{
    "query": {
        "match_all": {}
        }
    },   
    "aggs": {
       "raw": {
           "terms": {
               "field": "age",
               "size":10
       },
       "aggs": {
           "undupped": {
               "bm_undup_by_parents": {
                   "parent_paths": "employee",
                   "cache_bitsets": true,
                   "resilient": false,
               }
           }
       }
   }
}
```
Parameter | Default  | Meaning  
:------------- | :---- | :-----
parent_paths    |    | Comma separated list of parent definitions. \_nested\_ is a placeholder for a reverse nested aggregation.
cache_bitsets     | true     | Parent bitsets are cached for future usage.
resilient      | false  | if false: throws an exception if some parent path does not exist.
compensate_non_existing | true | compensates for non-existing parents 

You can view the cached bitsets by using the [_bm/cache/dump](#_bm_cache_dump) api.

**Note**: this aggregation only works if you have enabled the privileges as specified [here](#priv).

[Back to the top](#top)

## Similarity

This custom similarity aims to help scoring small fields. Like authors, cities, parts, etc. In these cases Lucene's scoring is somewhat 'explosive' and it is hard to do some relative scoring. This similarity forces scores to be around 1.0, which makes them more predictable where the score is still dependent on the tf and idf. By default idf is switched off. 

The similarity needs to be switched on in the index settings, like: (with default settings shown)

```javascript
   "settings": {
      "similarity": {
         "default": {
            "type": "bounded_similarity",
            "max_idf": 0.0,
            "max_tf", 0.2,
            "force_tf": -1,
            "bias_tf": 0.6,
            "discount_overlaps": true);
         }
      }
   }
```

The meaning of the parameters is:

Parameter | Default  | Meaning  
:------------- | :---- | :-----
max_idf     |  0     | Limits the influence of the idf. 
max_tf      |  0.2   | Limits the influence of the tf.
force_tf    |  -1    | Forces a tf (if >= 0).
bias_tf     |  0.6   | Correction value in the division.
discount_overlaps |  true   | Determines whether overlap tokens (Tokens with 0 position increment) are ignored when computing norm.

The total score is calculated as 1.0 + idf + tf, where 0<=idf<=max_idf and 0<=tf<=max_tf

**Note: bug in Elasticsearch**  
There was a bug in ES that prevented indices to be upgraded to a new version if a custom similarity is used.
Elasticsearch will not even start when this problem is encountered.
See [https://github.com/elastic/elasticsearch/issues/25350]. It should be solved now. 

If you experience problems, I see 2 possible workarounds:
* Remove the custom similarity before upgrading, and set the similarity afterwards.
* Copy the whole index, including the index definition from a cluster running the old version into the new cluster.


[Back to the top](#top)

## _termlist
The termlist api shows information about fields in the index. If a field is supplied as a parameter, all terms that are indexed in that field are shown.
Because the termlist could consist of a lot of terms, an out-of-memory is possible. 
By default the output of the termlist is capped to maximum 10000 items.

The following api is supported:

```
curl -XGET 'http://localhost:9200/_termlist'
curl -XGET 'http://localhost:9200/{index}/_termlist'
curl -XGET 'http://localhost:9200/_termlist/{field}'
curl -XGET 'http://localhost:9200/{index}/_termlist/{field}'
```

{index} can be a ,-separated list of index names to limit the termlist generation.
If no {index} is specified, all indexes will be queried.

{field} can be a ,-separated list of field names to limit the termlist generation.
If no {field} is specified, all fields in the selected indexes will be queried.

The plugin will supply a list of terms (tokens) with their accumilated counts. Like:

```javascript
"terms": [
    {
       "t": "house",
       "c": 2346
    },
    {
       "t": "prairy",
       "c": 23
    },
    etc...
}  
```
Note that the counts are accumilated over fields and indexes, without taking into account the indexes could have records in common.
So the returned counts >= the actual counts.

**Sorting**  
The list is sorted on term by default. The sort can be changed by supplying an extra param on the url:

```
?sort=[count | -count | term | -term]
```

**Limiting the resultset**  
The returned result can be limited by supplying one or more params on the url:

```
   ?filter=<regex>         returns only terms that match the expression
   ?resultLimit=<count>    returns only the top <count> items of the list (after sorting!)
   ?minCount=<min>         returns only items that occur >= min times. If 0<min<1, min is recalculated by multiplying it by the max term count.
   ?maxCount=<max>         returns only items that occur <= max times. If 0<max<1, max is recalculated by multiplying it by the max term count.
```

**Collision detection**  
To check wether terms collide after mapping terms, use the following param:

```
?replExpr=<expr/repl>   forinstance: 'ae/e' replaces a term 'maedchen' into 'madchen'
```
After the replacement a check is done if the resulting term was already in the index.
Output is like:

```javascript
{
   "term": {
      "t": "almhuette",
      "c": 1
   },
   "coll": {
      "t": "almhutte",
      "c": 19
   }
},
etc...   
```

**Term Occurrence**  
To get the fields where a term occurs, apply the ?term=<term> to the url.
If this param is found, a list of fields where the term occurs is returned.


**Warning**  
The term list is built internally for each selected shard as a sorted set of strings/counts.
These sets are streamed back to the node where the requeste came in, and are accumulated on that node.
You should be aware that this will result in a large amount of requested heap memory and may result in out of memory situations that can render your Elasticsearch cluster unusable until it is restarted.
It is best to limit your resultset by supplying resultset limiting params on the url (see above)

[Back to the top](#top)




## _view
The view api shows the contents of a record. It is able to show:
* the stored fields
* the terms that are indexed in a field by this record (slow)
* the supplied doc-values

The following api is supported:

```
curl -XGET 'http://localhost:9200/{index}/{type}/{id}/_view'
```
Following extra parameters are supported at this url:

Parameter | Default  | Meaning  
:------------- | :------------- | :-----
field      | (all) | Limits the output to this list of fields
field_expr |       | Limits the output to the fields that match this regex
output     | (all) | One of stored, indexed, docvalues
output_lvl | 0     | Shows additional debug information
offset     | 0     | Value to be added or subtracted to the found docid. (To view the neighbors)

Viewing all contributions to the indexed fields is a very costly operation. 
It involves enumerating all terms from the index, and per term check if the requested document could be found by this term.

[Back to the top](#top)




## <a name="bm_cache_dump"></a>_bm/cache/dump

This api shows the content of the filter or query cache.
Dumping caches is still under development and experimental, since it relies on internal Elasticsearch and Lucene structures. 
The API uses reflection to get access to private fields. If any of those fields cannot be found or cannot be accessed, it returns an exception.
As a consequence, if you remove the grants from this plugin, this api is not available.

The output will contain the toString() versions of the queries and their space requirements, collected by index name. 
The index name is get by executing a toString() on the cache key, which gives most of the time a reasonable indication of the index.
The raw internal names are always outputted. These names can be customised by using an index_expr parameter.

The following api is supported:

```
curl -XGET 'http://localhost:9200/_bm/cache/dump'
```
Following extra parameters are supported at this url:

Parameter | Default  | Meaning  
:------------- | :---- | :-----
index_expr     |       | Limits the output to indices that match this regex
sort           | size  | Sort the output on query or size.
type           | query | Type of the cache (request, query or bitset)
dump_raw       | false | Dumps the raw extracted values from where the index is calculated

**Note**: this api only works if you have enabled the privileges as specified [here](#priv).

[Back to the top](#top)



## <a name="diagnostics_fetcher"></a>Diagnostics/docvalues fetcher
This is an extension that shows more information about a search hit.
It shows doc-values and information about the shard and segment where the doc is found.

Activate it like:

```javascript
{
    "ext": {
        "_bm": {"diagnostics": true}
    },
    "query": {
        "match_all": {}
        }
    }
}
```
The response is in the fields collection:

```javascript
{
    "_source": {
    ...
    },
    "fields": {
       "_bm": [
          {
             "shard": 3,
             "segment": 0,
             "docid": 88401,
             "docid_rel": 88401,
             "docvalues": {
                "_parent#house": [
                   "ARD1005"
                ],
                "_type": [
                   "price"
                ]
             }
          }
       ]
    }
 }
```

[Back to the top](#top)

