#Bitmanager's Elasticsearch Plugin

This plugin extends Elasticsearch with a few endpoints and supplies a custom similarity.

##Installation
This plugin needs Elasticsearch 5.0 or higher. 
Goto [http://bitmanager.nl/es], download the appropriate version and copy it under the plugin directory of ll nodes in your cluster.
On the above location you can find older versions of the plugin, but no sourcecode is available for them.

##Introduction

The main entry points are:
* [_termlist](#_termlist)
* [_view](#_view)
* [_bm](#_bm)
* [_bm/version](#_bm/_version)
* [_bm/cache/dump](#_bm/_cache/dump)

Besides that, two other classes are provided:
* Similarity plugin
* A string-type that that can be analysed and allows for doc-values.

##_bm/version
This entrypoint checks if this plugin can be found on all nodes and it checks if the version and filesize is the same on all nodes.
As a convenience, it dumps all differences in the eleasticsearch.yml over the nodes.

##_termlist
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

####Sorting
The list is sorted on term by default. The sort can be changed by supplying an extra param on the url:
```
    ?sort=[count | -count]
```

####Limiting the resultset
The returned result can be limited by supplying one or more params on the url:
```
   ?filter=<regex>         returns only terms that match the expression
   ?resultLimit=<count>    returns only the top <count> items of the list (after sorting!)
   ?minCount=<min>         returns only items that occur >= min times. If 0<min<1, min is recalculated by multiplying it by the max term count.
   ?maxCount=<max>         returns only items that occur <= max times. If 0<max<1, max is recalculated by multiplying it by the max term count.
```
   
####Collision detection
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
         
####Term Occurrence
To get the fields where a term occurs, apply the ?term=<term> to the url.
If this param is found, a list of fields where the term occurs is returned.


####Warning
The term list is built internally for each selected shard as a sorted set of strings/counts.
These sets are streamed back to the node where the requeste came in, and are accumulated on that node.
You should be aware that this will result in a large amount of requested heap memory and may result in out of memory situations that can render your Elasticsearch cluster unusable until it is restarted.
It is best to limit your resultset by supplying resultset limiting params on the url (see above)

[Back to the top](#bitmanagers-elasticsearch-plugin)




##_view
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
field_expr      |       | Limits the output to the fields that match this regex
output | (all)      | One of stored, indexed, docvalues
output_lvl | 0      | Shows additional debug information
offset | 0      | Value to be added or subtracted to the found docid. (To view the neighbors)

Viewing all contributions to the indexed fields is a very costly operation. 
It involves enumerating all terms from the index, and per term check if the requested document could be found by this term.

[Back to the top](#bitmanagers-elasticsearch-plugin)




##_bm/cache/dump

This api shows the content of the filter or query cache.
This is an experimental API, since it relies on internal Elasticsearch and Lucene structures. 
The API uses reflection to get access to private fields. If any of those fields cannot be found or cannot be accessed, it returns an exception.
The output will contain the toString() versions of the queries and their space requirements, collected by index name. The index name is get by executing a toString() on the cache key, which gives most of the time a reasonable indication of the index.
The raw names are always outputted. These names can be customised by using an index_expr parameter.

The following api is supported:
```
   curl -XGET 'http://localhost:9200/_bm/cache/dump'
```
Following extra parameters are supported at this url:

Parameter | Default  | Meaning  
:------------- | :------------- | :-----
index_expr      |  | Limits the output to indices that match this regex
sort_query      | false | Sort the output on query.
type            | query | Type of the cache (request or query)

[Back to the top](#bitmanagers-elasticsearch-plugin)




##_bm
Shows the html version of this readme. (http://localhost:9200/_bm)
