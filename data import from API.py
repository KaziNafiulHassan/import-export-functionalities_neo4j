import requests
from neo4j import GraphDatabase
import pandas as pd

# Neo4j connection
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

def insert_data(tx, record):
    query = """
    MERGE (n:Data {id: $id})
    SET n += {name: $name, value: $value}
    """
    tx.run(query, id=record.get('id'), name=record.get('name'), value=record.get('value'))

def load_and_import(api_url):
    response = requests.get(api_url)
    data = response.json()

    with driver.session() as session:
        for record in data:
            session.write_transaction(insert_data, record)

# Import data from the Flask API
load_and_import("http://127.0.0.1:5001/api/data1")
load_and_import("http://127.0.0.1:5001/api/data2")
load_and_import("http://127.0.0.1:5001/api/data3")

driver.close()
