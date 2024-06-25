# from elasticsearch import Elasticsearch
# from elasticsearch.helpers import scan
# import logging        
# from elasticsearch import logger as elasticsearch_logger 
# import json
# elasticsearch_logger.setLevel(logging.DEBUG)   

# def get_all_ids(index_name, username, password, host='https://spasv2es8stgext.stage.adobesearch.io', port=443, use_ssl=True):
#     es = Elasticsearch(
#          "https://spasv2es8stgext.stage.adobesearch.io:443",
#          basic_auth=(username, password))

#     # Use the scan helper to iterate through all documents in the index
#     #results = scan(es, query='{"fields": "_id"}', index=index_name, scroll='10s')
#     results = scan(es, index=index_name, query={ 
# 		# "_source": {
# 		# 	"includes": [
# 		# 		"field_string_search_store_keyword_16",
# 		# 		"object_embedding_1024_1",  
# 		# 		"object_embedding_768_1",
# 		# 	]
# 		# },

#     "query": {
#         "bool": {
#             "must": {
#                 "term": {
#                     "_id": "urn:aaid:sc:VA6C2:2260e701-de4f-3c15-9525-8192066f3da0"
#                 }
#             }
#         }
#     }
# 	},scroll="25m",raise_on_error=False)
  
#     li = set()
#     count=25000
#     i = 0
#     final_result = {}
#     for result in results:
#         if not ("nested_1" in result and "object_1" in result['nested_1'] and "field_string_search_store_multifields_keyword_text_1" in result['nested_1']['object_1']):
#             continue
#         asset_id = result['_id']
#         embedding1 = result['_source']['object_embedding_1024_1']
#         # embedding2 = result['_source']['object_embedding_768_1']
#         final_result[asset_id] = {
#             "embedding1": embedding1,
#             # "embedding2": embedding2
#         }
#         i = i+1
#         if i == count:
#             break

#     output_file = "embeddings1.txt"
#     try:
#         with open(output_file, 'w') as file:
#             file.write(json.dumps(final_result))
#     finally:
#         print("done")
#     return li

# index_name = "hz_templates_es_video_19april"

# username = "uss_user"
# password = "DhPmPcb9+ibm"
# all_ids = get_all_ids(index_name, username, password)
# print(all_ids)


from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import logging        
from elasticsearch import logger as elasticsearch_logger 
import json
elasticsearch_logger.setLevel(logging.DEBUG)   

def get_all_ids(index_name, username, password, host='https://spasv2es8stgext.stage.adobesearch.io', port=443, use_ssl=True):
    print("inside the function")
    es = Elasticsearch(
         "https://spasv2es8stgext.stage.adobesearch.io:443",
         basic_auth=(username, password))

    # Use the scan helper to iterate through all documents in the index
    #results = scan(es, query='{"fields": "_id"}', index=index_name, scroll='10s')
    results = scan(es, index=index_name, query={ 
		"_source": {
			"includes": [
				"field_string_search_store_keyword_16",
				# "object_embedding_1024_1",  
				# "object_embedding_768_1",
                "nested_1.object_1"
			]
		},
	"query": {
		"bool": {
			"filter": [
				
				{
					"term": {
						"field_string_search_store_keyword_11": {
							"value": "ACTIVE",
							"boost": 1
						}
					}
				},
				{
					"term": {
						"field_string_search_store_keyword_6": {
							"value": "approved",
							"boost": 1
						}
					}
				}
			],
			"boost": 1
		}}
	},scroll="25m",raise_on_error=False)

    print("executed ES request")
    print(results)
  
    li = set()
    # count=25000
    i = 0
    final_result = {}
    template_type_result = {}
    for result in results:
        # print(result['_id'])
        if "_source" in result and "nested_1" in result['_source']:
            # print("nested 1 present in " + result['_id'])
            if len(result['_source']['nested_1']) > 0 and "object_1" in result['_source']['nested_1'][0]:
                # print("chal ja bahi")
                if "field_string_search_store_multifields_keyword_text_1" in result['_source']["nested_1"][0]["object_1"]:
                    # print("field present!")
                    template_type_result[result['_id']] = result['_source']["nested_1"][0]["object_1"]["field_string_search_store_multifields_keyword_text_1"]
        # if not ("_source" in result and "object_embedding_1024_1" in result['_source'] and "object_dynamic_rankerPositiveScoreImpact_1" in result['_source']['object_embedding_1024_1']):
        #     continue
        # asset_id = result['_id']
        # print(asset_id)
        # embedding1 = result['_source']['object_embedding_1024_1']
        # # embedding2 = result['_source']['object_embedding_768_1']
        # final_result[asset_id] = {
        #     "embedding1": embedding1,
        #     # "embedding2": embedding2
        # }
        i = i+1
        print(i)
        # if i == count:
        #     break

    # output_file = "embeddings1.txt"
    # try:
    #     with open(output_file, 'w') as file:
    #         file.write(json.dumps(final_result))
    # finally:
    #     print("done")

    output_file2 = "templatetypes.txt"
    try:
        with open(output_file2, 'w') as file:
            file.write(json.dumps(template_type_result))
    finally:
        print("done")
    return li

index_name = "hz_templates_es_video_19april"

username = "uss_user"
password = "DhPmPcb9+ibm"
print("executing main function")
all_ids = get_all_ids(index_name, username, password)
print(all_ids)