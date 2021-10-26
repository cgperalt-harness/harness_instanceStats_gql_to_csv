
__author__ = "Gabs the CSE"
__copyright__ = "N/A"
__credits__ = ["Gabriel Cerioni"]
__license__ = "GPL"
__version__ = "0.0.1"
__maintainer__ = "Gabriel Cerioni"
__email__ = "gabriel.cerioni@harness.io"
__status__ = "Brainstorming Phase with CSM - DEV/STG/PRD is not applicable"

import os
import csv
import logging

from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
#from gql.transport.requests import log as requests_logger

# Configs (if this gets bigger, I'll provide a config file... or even Hashicorp Vault)
# optional - logging.basicConfig(filename='gabs_graphql.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
# this is not working anymore - requests_logger.setLevel(logging.WARNING)
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

# CURRENTLY, PLEASE PUT THE CSV FILE NAME HERE

API_KEY = os.environ.get('HARNESS_GRAPHQL_API_KEY')
API_ENDPOINT = os.environ.get('HARNESS_GRAPHQL_ENDPOINT')
#OUTPUT_CSV_NAME_CONST = "temp_instanceStats_parsed.csv"
OUTPUT_CSV_NAME_CONST = os.environ.get('HARNESS_GQL_CSV_NAME')

def generic_graphql_query(query):
    req_headers = {
        'x-api-key': API_KEY
    }

    _transport = RequestsHTTPTransport(
        url=API_ENDPOINT,
        headers=req_headers,
        use_json=True,
    )

    # Create a GraphQL client using the defined transport
    client = Client(transport=_transport, fetch_schema_from_transport=True)

    # Provide a GraphQL query
    generic_query = gql(query)

    # Execute the query on the transport
    result = client.execute(generic_query)
    return result

def get_all_instances_by_service_by_env():
    query = '''{
    instanceStats(groupBy: [{entityAggregation: Service}, {entityAggregation: Environment}]) {
       ... on StackedData {
         dataPoints {
           key {
             name
             id
           }
           values {
             key {
               name
             }
             value
           }
         }
   
       }
     }
    }'''
    generic_query_result = generic_graphql_query(query)

    return(generic_query_result)

def parse_result_to_csv(instanceStats_gql_resultset):
    # just for readability - I'll build a cleaner result set to make it easier to CSV this later
    clean_dict_list = []

    result_list = instanceStats_gql_resultset['instanceStats']['dataPoints']

    for service_item in result_list:
      instances = []
      service_name = service_item['key']['name']
      service_id = service_item['key']['id']

      instance_environments = service_item['values']

      for service_instance in instance_environments:
        current_dict_entry = {'Service_Name' : service_name, 'Service_ID': service_id,  'Environment' : service_instance['key']['name'], 'Instance_Count' : service_instance['value']}
        clean_dict_list.append(current_dict_entry)
    
    with open(OUTPUT_CSV_NAME_CONST, 'w', encoding='utf8', newline='') as output_file:
      fc = csv.DictWriter(output_file, fieldnames=clean_dict_list[0].keys(),)
      fc.writeheader()
      fc.writerows(clean_dict_list)

    return(clean_dict_list)

if __name__ == '__main__':
    logging.info("Starting the Program...")

    logging.info("Retrieving your current instanceStats GraphQL Query result set...")
    result_from_query = get_all_instances_by_service_by_env()
    logging.info("Done!")

    logging.info("Expanding all rows from the nested dict - and then putting it on the CSV: {0}".format(OUTPUT_CSV_NAME_CONST))
    parsed_result_set = parse_result_to_csv(result_from_query)
    logging.info("Done! Outputting the list content here:")
    print(parsed_result_set)

    logging.info("Program Exited! Have a nice day!")
