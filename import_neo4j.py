import csv
import requests
from io import StringIO
from neo4j import GraphDatabase

# Neo4j connection details
# Set up the connection to the Neo4j database
uri = "bolt://localhost:7687"
username = "neo4j"
password = "password"

# URLs to fetch CSV data
# Define URLs to the CSV files stored on GitHub
csv_urls = {
    "file_0": "https://raw.githubusercontent.com/KaziNafiulHassan/import-export-functionalities_neo4j/main/01_chemical_data.csv",  # Chemical data (Substances and Use Groups)
    "file_1": "https://raw.githubusercontent.com/KaziNafiulHassan/import-export-functionalities_neo4j/main/03_exposure_data_measured_simplified2.csv",  # Exposure data (Sites and Measurements)
    "file_2": "https://raw.githubusercontent.com/KaziNafiulHassan/import-export-functionalities_neo4j/main/05_hazard_data.csv"  # Hazard data (Species and Toxicity)
}

# Function to fetch CSV data from a URL
def fetch_csv_data(url):
    response = requests.get(url)  # Send a GET request to the URL
    response.raise_for_status()  # Raise an exception for HTTP errors
    return StringIO(response.text)  # Convert the response text to a file-like object

# Function to create constraints in Neo4j
def create_constraints(tx):
    # Ensure unique constraints on specific properties for each node type
    tx.run("CREATE CONSTRAINT Name_Substance_uniq IF NOT EXISTS FOR (n:Substance) REQUIRE (n.Name) IS UNIQUE;")
    tx.run("CREATE CONSTRAINT river_basin_name_n_Site_uniq IF NOT EXISTS FOR (n:Site) REQUIRE (n.river_basin_name_n) IS UNIQUE;")
    tx.run("CREATE CONSTRAINT species_Species_uniq IF NOT EXISTS FOR (n:Species) REQUIRE (n.species) IS UNIQUE;")
    tx.run("CREATE CONSTRAINT use_group_Use_Group_uniq IF NOT EXISTS FOR (n:Use_Group) REQUIRE (n.use_group) IS UNIQUE;")

# Function to load Substance nodes from CSV data
def load_substance_nodes(tx, csv_reader):
    for row in csv_reader:
        if row.get('preferredName'):
            # Create or update Substance nodes with Name, DTXSID, and casrn
            tx.run("""
                MERGE (n:Substance { Name: $Name })
                SET n.DTXSID = $DTXSID, n.casrn = $casrn
            """, Name=row['preferredName'], DTXSID=row['DTXSID'], casrn=row['casrn'])

# Function to load Site nodes from CSV data
def load_site_nodes(tx, csv_reader):
    for row in csv_reader:
        if row.get('river_basin_name_n'):
            # Create or update Site nodes with river basin name, DTXSID, lat, lon, country, and water body name
            tx.run("""
                MERGE (n:Site { river_basin_name_n: $river_basin_name_n })
                SET n.DTXSID = $DTXSID, n.lat = $lat, n.lon = $lon, 
                    n.country = $country, n.water_body_name_n = $water_body_name_n
            """, river_basin_name_n=row['river_basin_name_n'], DTXSID=row['DTXSID'], lat=row['lat'],
               lon=row['lon'], country=row['country'], water_body_name_n=row['water_body_name_n'])

# Function to load Species nodes from CSV data
def load_species_nodes(tx, csv_reader):
    for row in csv_reader:
        if row.get('species'):
            # Create or update Species nodes with species name and DTXSID
            tx.run("""
                MERGE (n:Species { species: $species })
                ON CREATE SET n.DTXSID = $DTXSID, n.species = $species
            """, species=row['species'], DTXSID=row['DTXSID'])

# Function to load Use_Group nodes from CSV data
def load_use_group_nodes(tx, csv_reader):
    for row in csv_reader:
        if row.get('use_group'):
            # Create or update Use_Group nodes with use_group name
            tx.run("""
                MERGE (n:Use_Group { use_group: $use_group })
            """, use_group=row['use_group'])

# Function to create relationships between nodes
def create_relationships(tx, csv_reader, query):
    for row in csv_reader:
        # Execute the relationship creation query using the row data
        tx.run(query, **row)

# Main function to manage the data loading process
def main():
    driver = GraphDatabase.driver(uri, auth=(username, password))  # Establish connection to Neo4j

    with driver.session() as session:
        # Create constraints in the Neo4j database
        session.execute_write(create_constraints)

        # Load nodes into Neo4j from CSV files
        session.execute_write(load_substance_nodes, csv.DictReader(fetch_csv_data(csv_urls['file_0'])))
        session.execute_write(load_site_nodes, csv.DictReader(fetch_csv_data(csv_urls['file_1'])))
        session.execute_write(load_species_nodes, csv.DictReader(fetch_csv_data(csv_urls['file_2'])))
        session.execute_write(load_use_group_nodes, csv.DictReader(fetch_csv_data(csv_urls['file_0'])))

        # Create relationships between nodes
        session.execute_write(create_relationships, csv.DictReader(fetch_csv_data(csv_urls['file_0'])), """
            MATCH (sub:Substance { DTXSID: $DTXSID }), (use:Use_Group { use_group: $use_group })
            MERGE (sub)-[r:REGISTERED_AS]->(use)
        """)
        session.execute_write(create_relationships, csv.DictReader(fetch_csv_data(csv_urls['file_1'])), """
            MATCH (sub:Substance { DTXSID: $DTXSID }), (site:Site { river_basin_name_n: $river_basin_name_n })
            MERGE (sub)-[r:MEASURED_AT]->(site)
            SET r.concentration_value = $concentration_value, 
                r.concentration_unit = $concentration_unit, 
                r.time_point = $time_point
        """)
        session.execute_write(create_relationships, csv.DictReader(fetch_csv_data(csv_urls['file_1'])), """
            MATCH (sub:Substance { DTXSID: $DTXSID }), (site:Site { river_basin_name_n: $river_basin_name_n })
            MERGE (sub)-[r:IS_DRIVER]->(site)
        """)
        session.execute_write(create_relationships, csv.DictReader(fetch_csv_data(csv_urls['file_2'])), """
            MATCH (sub:Substance { DTXSID: $DTXSID }), (spec:Species { species: $species })
            MERGE (sub)-[r:TESTED_FOR_TOXICITY]->(spec)
            SET r.tox_value_mg_L = $tox_value_mg_L, 
                r.tox_stat = $tox_stat, 
                r.tox_source = $tox_source
        """)
        session.execute_write(create_relationships, csv.DictReader(fetch_csv_data(csv_urls['file_2'])), """
            MATCH (site:Site { DTXSID: $DTXSID }), (spec:Species { species: $species })
            MERGE (site)-[r:IMPACT_ON]->(spec)
        """)

    driver.close()  # Close the Neo4j connection

# Entry point of the script
if __name__ == "__main__":
    main()
