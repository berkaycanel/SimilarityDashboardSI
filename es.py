import requests
import json
import os
import streamlit as st

ES_URL = st.secrets["ES_URL"]

def get_all_domains():
    payload = json.dumps({
        "size": 34000, 
        "_source": ["domain"],
        "query": {
            "bool": {
                "should": [
                    {"exists": {"field": "refined_gpt_tags"}},
                    {"exists": {"field": "wp_tags"}}
                ],
                "minimum_should_match": 1
            }
        }
    })
    headers = {
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(ES_URL + '/domain_crawler/_search', headers=headers, data=payload)
        response.raise_for_status()
        json_data = response.json()
        domains = [hit['_source']['domain'] for hit in json_data['hits']['hits']]
        return domains
    except requests.exceptions.RequestException as e:
        print(f"Error fetching domain list: {e}")
        return []

def get_domain_tags_new(domain):
    payload = json.dumps({
        'size': 1,
        '_source': [
            'refined_gpt_tags',
            'cb_tags',
            'li_tags',
            'funding_stage',
            'employees',
            'total_funding_amount',
            'wp_tags',
            '_id'
        ],
        'query': {
            'bool': {
                'must': [
                    {
                        'term': {
                            'domain.keyword': domain,
                        }
                    }
                ]
            }
        }
    })
    headers = {
    'Content-Type': 'application/json'
    }
    
    response = requests.request('GET', ES_URL + '/domain_crawler/_search', headers=headers, data=payload)
    json_data = json.loads(response.text)
    
    if len(json_data['hits']['hits']) > 0:
        source_data = json_data['hits']['hits'][0]['_source']
        
        return {
            'refined_gpt_tags': source_data.get('refined_gpt_tags', []),
            'cb_tags': source_data.get('cb_tags', []),
            'li_tags': source_data.get('li_tags', []),
            'funding_stage': source_data.get('funding_stage', 'N/A'),
            'employees': source_data.get('employees', 'N/A'),
            'total_funding_amount': source_data.get('total_funding_amount', 'N/A'),
            'wp_tags': source_data.get('wp_tags', []),
            'id': json_data['hits']['hits'][0]['_id']
        }
    else:
        return {
            'refined_gpt_tags': [],
            'cb_tags': [],
            'li_tags': [],
            'funding_stage': 'N/A',
            'employees': 'N/A',
            'total_funding_amount': 'N/A',
            'wp_tags': [],
            'id': None
        }


def get_related_domains_new(refined_gpt_tags=None, cb_tags=None, li_tags=None, wp_tags=None, domain=None, funding_stage=None, employees=None, total_funding_amount=None, boosts=None):
    if boosts is None:
        boosts = {
            'refined_gpt_tags': 3.5,
            'cb_tags': 2.5,
            'li_tags': 3.0,
            'funding_stage': 3.0,
            'employees': 2.0,
            'total_funding_amount': 3.0,
            'wp_tags': 6.0  
        }

    tags_obj = []

    if refined_gpt_tags:
        for tag in refined_gpt_tags:
            tags_obj.append({
                'term': {'refined_gpt_tags.keyword': {'value': tag, 'boost': boosts['refined_gpt_tags']}}
            })
    if cb_tags:
        for tag in cb_tags:
            tags_obj.append({
                'term': {'cb_tags.keyword': {'value': tag, 'boost': boosts['cb_tags']}}
            })
    if li_tags:
        for tag in li_tags:
            tags_obj.append({
                'term': {'li_tags.keyword': {'value': tag, 'boost': boosts['li_tags']}}
            })
    if wp_tags:
        for tag in wp_tags:
            tags_obj.append({
                'term': {'wp_tags.keyword': {'value': tag, 'boost': boosts['wp_tags']}}
            })

    payload = json.dumps({
        'size': 15,
        '_source': [
            'domain',
            'refined_gpt_tags',
            'cb_tags',
            'li_tags',
            'funding_stage',
            'employees',
            'total_funding_amount',
            'wp_tags'
        ],
        'highlight': {
            'fields': {
                'refined_gpt_tags.keyword': {},
                'cb_tags.keyword': {},
                'li_tags.keyword': {},
                'wp_tags.keyword': {},
                'funding_stage': {},
                'employees': {},
                'total_funding_amount': {}
            }
        },
        'query': {
            'bool': {
                'must_not': [
                    {
                        'term': {
                            'domain.keyword': domain,
                        }
                    }
                ],
                'should': tags_obj + [
                    {'match_phrase': {'funding_stage': {'query': funding_stage, 'boost': boosts['funding_stage']}}} if funding_stage else {},
                    {'match_phrase': {'employees': {'query': employees, 'boost': boosts['employees']}}} if employees else {},
                    {'match_phrase': {'total_funding_amount': {'query': total_funding_amount, 'boost': boosts['total_funding_amount']}}} if total_funding_amount else {}
                ],
                'minimum_should_match': 2,
                'boost': 1.0
            }
        }
    })

    payload_dict = json.loads(payload)
    payload_dict['query']['bool']['should'] = [clause for clause in payload_dict['query']['bool']['should'] if clause]

    payload = json.dumps(payload_dict)

    headers = {
        'Content-Type': 'application/json'
    }


    try:
        response = requests.get(ES_URL + '/domain_crawler/_search', headers=headers, data=payload)
        response.raise_for_status()
        json_data = response.json()

        
        if 'hits' in json_data and 'hits' in json_data['hits']:
            return json_data['hits']['hits']
        else:
            print("No hits found in the Elasticsearch response.")
            print("Response:", json.dumps(json_data, indent=4))  
            return []  

    except requests.exceptions.RequestException as e:
        print(f"Error fetching related domains: {e}")
        return []

