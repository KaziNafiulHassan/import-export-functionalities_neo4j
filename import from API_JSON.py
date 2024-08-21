import requests
from neo4j import GraphDatabase

# Neo4j connection details
uri = "bolt://localhost:7687"
username = "neo4j"
password = "password"

# API endpoints to fetch JSON data
api_urls = {
    "file_0": "http://127.0.0.1:5001/api/data1",  # Contains Substance and Use_Group data
    "file_1": "http://127.0.0.1:5001/api/data2",  # Contains Site and Measurement data
    "file_2": "http://127.0.0.1:5001/api/data3"   # Contains Species and Toxicity data
}

# Function to fetch JSON data from an API endpoint
def fetch_json_data(url):
    response = requests.get(url)
    response.raise_for_status()  # Raise an error if the request fails
    return response.json()  # Return the JSON data as a Python dictionary

# Function to create constraints in Neo4j
def create_constraints(tx):
    # Ensure unique constraints on specific properties for each node type
    tx.run("CREATE CONSTRAINT Name_Substance_uniq IF NOT EXISTS FOR (n:Substance) REQUIRE (n.Name) IS UNIQUE;")
    tx.run("CREATE CONSTRAINT river_basin_name_n_Site_uniq IF NOT EXISTS FOR (n:Site) REQUIRE (n.river_basin_name_n) IS UNIQUE;")
    tx.run("CREATE CONSTRAINT species_Species_uniq IF NOT EXISTS FOR (n:Species) REQUIRE (n.species) IS UNIQUE;")
    tx.run("CREATE CONSTRAINT use_group_Use_Group_uniq IF NOT EXISTS FOR (n:Use_Group) REQUIRE (n.use_group) IS UNIQUE;")

# Function to load Substance nodes from JSON data
def load_substance_nodes(tx, data):
    for row in data:
        if row.get('preferredName'):
            tx.run("""
                MERGE (n:Substance { Name: $Name })
                SET n.DTXSID = $DTXSID, n.casrn = $casrn
            """, Name=row['preferredName'], DTXSID=row['DTXSID'], casrn=row['casrn'])

# Function to load Site nodes from JSON data
def load_site_nodes(tx, data):
    for row in data:
        if row.get('river_basin_name_n'):
            tx.run("""
                MERGE (n:Site { river_basin_name_n: $river_basin_name_n })
                SET n.DTXSID = $DTXSID, n.lat = $lat, n.lon = $lon, 
                    n.country = $country, n.water_body_name_n = $water_body_name_n
            """, river_basin_name_n=row['river_basin_name_n'], DTXSID=row['DTXSID'], lat=row['lat'],
               lon=row['lon'], country=row['country'], water_body_name_n=row['water_body_name_n'])

# Function to load Species nodes from JSON data
def load_species_nodes(tx, data):
    for row in data:
        if row.get('species'):
            tx.run("""
                MERGE (n:Species { species: $species })
                ON CREATE SET n.DTXSID = $DTXSID, n.species = $species
            """, species=row['species'], DTXSID=row['DTXSID'])

# Function to load Use_Group nodes from JSON data
def load_use_group_nodes(tx, data):
    for row in data:
        if row.get('use_group'):
            tx.run("""
                MERGE (n:Use_Group { use_group: $use_group })
            """, use_group=row['use_group'])

# Function to create relationships between nodes
def create_relationships(tx, data, query):
    for row in data:
        tx.run(query, **row)

# Main function to manage the data loading process
def main():
    driver = GraphDatabase.driver(uri, auth=(username, password))

    with driver.session() as session:
        # Create constraints in the Neo4j database
        session.execute_write(create_constraints)

        # Load nodes into Neo4j from JSON files
        session.execute_write(load_substance_nodes, fetch_json_data(api_urls['file_0']))
        session.execute_write(load_site_nodes, fetch_json_data(api_urls['file_1']))
        session.execute_write(load_species_nodes, fetch_json_data(api_urls['file_2']))
        session.execute_write(load_use_group_nodes, fetch_json_data(api_urls['file_0']))

        # Create relationships between nodes
        session.execute_write(create_relationships, fetch_json_data(api_urls['file_0']), """
            MATCH (sub:Substance { DTXSID: $DTXSID }), (use:Use_Group { use_group: $use_group })
            MERGE (sub)-[r:REGISTERED_AS]->(use)
        """)
        session.execute_write(create_relationships, fetch_json_data(api_urls['file_1']), """
            MATCH (sub:Substance { DTXSID: $DTXSID }), (site:Site { river_basin_name_n: $river_basin_name_n })
            MERGE (sub)-[r:MEASURED_AT]->(site)
            SET r.concentration_value = $concentration_value, 
                r.concentration_unit = $concentration_unit, 
                r.time_point = $time_point
        """)
        session.execute_write(create_relationships, fetch_json_data(api_urls['file_1']), """
            MATCH (sub:Substance { DTXSID: $DTXSID }), (site:Site { river_basin_name_n: $river_basin_name_n })
            MERGE (sub)-[r:IS_DRIVER]->(site)
            SET r.driver_importance = $driver_importance, 
                r.time_point = $time_point
        """)
        session.execute_write(create_relationships, fetch_json_data(api_urls['file_2']), """
            MATCH (sub:Substance { DTXSID: $DTXSID }), (spec:Species { species: $species })
            MERGE (sub)-[r:TESTED_FOR_TOXICITY]->(spec)
            SET r.tox_value_mg_L = $tox_value_mg_L, 
                r.tox_stat = $tox_stat, 
                r.tox_source = $tox_source
        """)
        session.execute_write(create_relationships, fetch_json_data(api_urls['file_2']), """
            MATCH (site:Site { DTXSID: $DTXSID }), (spec:Species { species: $species })
            MERGE (site)-[r:IMPACT_ON]->(spec)
            SET r.sum_TU = $sum_TU, 
                r.time_point = $time_point
        """)

    driver.close()

if __name__ == "__main__":
    main()
