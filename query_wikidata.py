# -*- coding: utf-8 -*-

from SPARQLWrapper import SPARQLWrapper
import re
import json
import pandas as pd
import numpy as np
import os

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
output_path = "odata"

qid_time_path = os.path.join(output_path, "qid_time.csv")
qid_location_path = os.path.join(output_path, "qid_location.csv")
qid_participant_path = os.path.join(output_path, "qid_participant.csv")
qid_partof_path = os.path.join(output_path, "qid_partof.csv")


def extract_qid(entry, name='qid'):
    try:
        return entry[name]['value'].replace('http://www.wikidata.org/entity/Q', '')
    except KeyError:
        return np.nan


def try_until_timeout(sparql_object, timeout_tries=100):
    iteration = 0
    try_again = True
    while try_again:
        try:
            query_result = sparql_object.queryAndConvert()
            try_again = False
            return query_result
        except json.decoder.JSONDecodeError:
            print("Timeout!, number " + str(iteration))
            iteration += 1
            if iteration > timeout_tries:
                print("Max timeout reached.")
                return


def get_statement(query, timeout_tries=100, exclude_unknown=True):
    query = query.replace('\n', '')
    column_names = re.findall(R'SELECT(.*?)WHERE', query)[0]
    column_names = column_names.replace(' ', '')
    column_names = re.findall('\?([^?]*)', column_names)
    print(query)
    sparql.addCustomHttpHeader('User-Agent',
                               'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36')
    sparql.setQuery(query)
    sparql.setReturnFormat('json')
    sparql.setMethod("POST")
    query_result = try_until_timeout(sparql, timeout_tries=timeout_tries)

    result_list = list()
    for column in column_names:
        tmp_list = list()
        for single_query in query_result['results']['bindings']:
            tmp_list.append(extract_qid(single_query, name=column))
        result_list.append(tmp_list)
    result_df = pd.DataFrame(result_list)
    result_df = result_df.transpose()
    result_df.columns = column_names
    if exclude_unknown:
        for column in column_names:
            result_df = result_df[~result_df[column].str.get(0).isin(['t'])]
    return result_df


qid_label = get_statement(query="""
    SELECT DISTINCT ?qid ?qidLabel WHERE {
  ?qid (wdt:P31/wdt:P279*) wd:Q180684.
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
""")

instance = get_statement(query="""
    SELECT DISTINCT ?qid ?instance_of ?instance_ofLabel WHERE {
  ?qid (wdt:P31/wdt:P279*) wd:Q180684.
  OPTIONAL { ?qid wdt:P31 ?instance_of. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
""")

qid_time = get_statement(query="""
  SELECT DISTINCT ?qid ?start ?end ?point_in_time ?instance_of  WHERE {
  ?qid (wdt:P31/wdt:P279*) wd:Q180684.
  OPTIONAL { ?qid wdt:P580 ?start. }
  OPTIONAL { ?qid wdt:P582 ?end. }
  OPTIONAL { ?qid wdt:P585 ?point_in_time. }
  OPTIONAL { ?qid wdt:P31 ?instance_of. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }

}
""")

qid_location = get_statement(query="""
    SELECT DISTINCT ?qid ?location ?locationLabel ?instance_of WHERE {
  ?qid (wdt:P31/wdt:P279*) wd:Q180684.
  OPTIONAL { ?qid wdt:P276 ?location. }
  OPTIONAL { ?qid wdt:P31 ?instance_of. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
""")

qid_participant = get_statement(query="""
   SELECT DISTINCT ?qid ?participant ?participantLabel ?instance_of  WHERE {
  ?qid (wdt:P31/wdt:P279*) wd:Q180684.
  OPTIONAL { ?qid wdt:P710 ?participant. }
  OPTIONAL { ?qid wdt:P31 ?instance_of. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }

}
""")

qid_partof = get_statement(query="""
   SELECT DISTINCT ?qid ?part_of ?part_ofLabel ?instance_of   WHERE {
  ?qid (wdt:P31/wdt:P279*) wd:Q180684.
  OPTIONAL { ?qid wdt:P361 ?part_of. }
  OPTIONAL { ?qid wdt:P31 ?instance_of. }
   SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
""")

instance = instance.loc[:, ['instance_of', 'instance_ofLabel']].drop_duplicates()

qid_location = pd.merge(qid_location, instance, on='instance_of', how='left').drop_duplicates()
qid_partof = pd.merge(qid_partof, instance, on='instance_of', how='left').drop_duplicates()
qid_time = pd.merge(qid_time, instance, on='instance_of', how='left').drop_duplicates()
qid_participant = pd.merge(qid_participant, instance, on='instance_of', how='left').drop_duplicates()

qid_location = pd.merge(qid_location, qid_label, on='qid', how='left').drop_duplicates()
qid_time = pd.merge(qid_time, qid_label, on='qid', how='left').drop_duplicates()
qid_partof = pd.merge(qid_partof, qid_label, on='qid', how='left').drop_duplicates()
qid_participant = pd.merge(qid_participant, qid_label, on='qid', how='left').drop_duplicates()

qid_location = qid_location.loc[:, ['qid', 'qidLabel', 'location', 'locationLabel', 'instance_of', 'instance_ofLabel']]
qid_time = qid_time.loc[:, ['qid', 'qidLabel', 'start', 'end', 'point_in_time', 'instance_of', 'instance_ofLabel']]
qid_partof = qid_partof.loc[:, ['qid', 'qidLabel', 'part_of', 'part_ofLabel', 'instance_of', 'instance_ofLabel']]
qid_participant = qid_participant.loc[:,
                  ['qid', 'qidLabel', 'participant', 'participantLabel', 'instance_of', 'instance_ofLabel']]

qid_time.to_csv(qid_time_path, index=False)
qid_location.to_csv(qid_location_path, index=False)
qid_participant.to_csv(qid_participant_path, index=False)
qid_partof.to_csv(qid_partof_path, index=False)
