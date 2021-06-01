from google.cloud import bigquery
from google.cloud import language_v1
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value
from google.cloud import bigquery_storage
import pandas as pd
import pyarrow
import time
import sys
import base64

client = bigquery.Client()
project = "<INSERT YOUR PROJECT-ID>"
dataset_id = "<INSERT YOUR BIGQUERY DATASET>"
dataset_ref = bigquery.DatasetReference(project, dataset_id)
table_ref = dataset_ref.table("INSERT YOUR INPUT TABLE")
table = client.get_table(table_ref)

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    
    # call_batch_entity("maryland_data.csv", 'marylandout.csv')
    call_batch_entity("/tmp/marylandout.csv")

def sample_analyze_entities_detail(text_content):
    """
    Analyzing Entities in a String

    Args:
      text_content The text content to analyze
    """

    client = language_v1.LanguageServiceClient()

    # text_content = 'California is a state.'

    # Available types: PLAIN_TEXT, HTML
    type_ = language_v1.Document.Type.PLAIN_TEXT

    # Optional. If not specified, the language is automatically detected.
    # For list of supported languages:
    # https://cloud.google.com/natural-language/docs/languages
    language = "en"
    document = {"content": text_content, "type_": type_, "language": language}

    # Available values: NONE, UTF8, UTF16, UTF32
    encoding_type = language_v1.EncodingType.UTF8

    response = client.analyze_entities(request = {'document': document, 'encoding_type': encoding_type})

    # Loop through entitites returned from the API
    
    dict_ret = {}
            
    for entity in response.entities:
        print(u"Representative name for the entity: {}".format(entity.name))

        # Get entity type, e.g. PERSON, LOCATION, ADDRESS, NUMBER, et al
        print(u"Entity type: {}".format(language_v1.Entity.Type(entity.type_).name))

        # Get the salience score associated with the entity in the [0, 1.0] range
        print(u"Salience score: {}".format(entity.salience))


        print("\n\n")
        # Loop over the mentions of this entity in the input document.
        # The API currently supports proper noun mentions.
        #for mention in entity.mentions:
            #print(u"Mention text: {}".format(mention.text.content))

            # Get the mention type, e.g. PROPER for proper noun
            #print(
            #    u"Mention type: {}".format(language_v1.EntityMention.Type(mention.type_).name)
            #)

    return (dict_ret)

def sample_analyze_entity_sentiment(text_content):
    client = language_v1.LanguageServiceClient()
    type_ = language_v1.Document.Type.PLAIN_TEXT

    # Optional. If not specified, the language is automatically detected.
    # For list of supported languages:
    # https://cloud.google.com/natural-language/docs/languages
    language = "en"
    document = {"content": text_content, "type_": type_, "language": language}

    # Available values: NONE, UTF8, UTF16, UTF32
    encoding_type = language_v1.EncodingType.UTF8


    
    response = client.analyze_entity_sentiment(request = {'document': document, 'encoding_type': encoding_type})
    
    
    dict_ret = {}
    
    index = 0
    
    entity_strings = [",,,",",,,",",,,"]
    
    # Loop through entitites returned from the API
    for entity in response.entities:

        dict_ret["name"] = entity.name 
        dict_ret["salience"] = entity.salience 
        sentiment = entity.sentiment
        dict_ret["sentiment"] = sentiment.score
        
        if (index < 3):
            entity_strings[index] = f",'{entity.name}', {entity.salience:8.2f}, {sentiment.score:8.2}"
        index = index +1
    return entity_strings

#def call_batch_entity(filename, outfilename):
def call_batch_entity(outfilename):
    #print ("inputfilename: " + filename)
    print ("outputfilename: " + outfilename)
    
    fout = open(outfilename, 'a')
    fout.write("id, entity1, salience1, sentiment1, entity2, salience2, sentiment2, entity3, salience3, sentiment3 \n")
    
    #df  = pd.read_csv(filename)
    #df.comments = df.comments.astype(str)
    df = client.list_rows(table).to_dataframe()
    
    for index, row in df.iterrows():
        reason_str = str(row["reason_for_your_contact_with_the_state_"]) + " " + str(row["comments_suggestions_about_our_service_"])
        csv_string = row['id']
        if reason_str and  not reason_str.isspace()  :
            entity = sample_analyze_entity_sentiment(reason_str)
            if (not entity):
                print (str(index) + " : empty entity")
            else:
                csv_string = str(csv_string) + entity[0] + entity[1] + entity[2] + "\n"
                #print (csv_string)
                fout.write(csv_string)
           
            time.sleep(.1)
    fout.close()
    
    source_filename = outfilename

    table_ref = dataset_ref.table('output_table')
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True
    )

    with open(source_filename, 'rb') as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            location='US',  # Must match the destination dataset location.
            job_config=job_config)  # API request

    job.result()  # Waits for table load to complete.

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_id, table_ref.path))
