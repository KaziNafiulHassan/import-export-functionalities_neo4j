from neo4j import GraphDatabase

# Define your Neo4j connection credentials
uri = "neo4j://localhost:7687"  # Replace with your Neo4j URI
user = "neo4j"                  # Replace with your Neo4j username
password = "password"           # Replace with your Neo4j password

# Define the URLs for your CSV files
file_0 = 'https://raw.githubusercontent.com/KaziNafiulHassan/import-export-functionalities_neo4j/main/01_chemical_data.csv'
file_1 = 'https://raw.githubusercontent.com/KaziNafiulHassan/import-export-functionalities_neo4j/main/03_exposure_data_measured.csv'
file_2 = 'https://raw.githubusercontent.com/KaziNafiulHassan/import-export-functionalities_neo4j/main/05_hazard_data.csv'

# Cypher queries for creating constraints, importing nodes, and creating relationships
cypher_queries = [
    # Create unique constraints
    "CREATE CONSTRAINT FOR (s:Substance) REQUIRE s.DTXSID IS UNIQUE;",
    "CREATE CONSTRAINT FOR (st:Site) REQUIRE (st.station_name_n, st.lat, st.lon) IS UNIQUE;",
    
    # Import Substance Nodes
    f"""
    LOAD CSV WITH HEADERS FROM '{file_0}' AS row
    MERGE (s:Substance {{DTXSID: row.DTXSID}})
    ON CREATE SET 
      s.Name = COALESCE(row.Name, 'NA'),
      s.cas_number = COALESCE(row.cas_number, 'NA'),
      s.inchi = COALESCE(row.inchi, 'NA'),
      s.inchiKey = COALESCE(row.inchiKey, 'NA');
    """,

    # Import Site Nodes
    f"""
    LOAD CSV WITH HEADERS FROM '{file_1}' AS row
    MERGE (st:Site {{station_name_n: row.station_name_n, lat: COALESCE(row.lat, 'NA'), lon: COALESCE(row.lon, 'NA')}})
    ON CREATE SET
      st.country = COALESCE(row.country, 'NA'),
      st.water_body_name_n = COALESCE(row.water_body_name_n, 'NA'),
      st.river_basin_name_n = COALESCE(row.river_basin_name_n, 'NA');
    """,

    # Create 3 main Species nodes
    "MERGE (sp1:Species {species: 'fish'}) MERGE (sp2:Species {species: 'algae'}) MERGE (sp3:Species {species: 'crustacean'});",
    
    # Create MEASURED_AT Relationships
    f"""
    LOAD CSV WITH HEADERS FROM '{file_1}' AS row
    MATCH (s:Substance {{DTXSID: row.DTXSID}})
    MATCH (st:Site {{station_name_n: row.station_name_n}})
    MERGE (s)-[:MEASURED_AT {{
      concentration: COALESCE(row.concentration_value, 'NA'), 
      concentration_unit: COALESCE(row.concentration_unit, 'NA'),
      time_point: COALESCE(row.time_point, 'NA')
    }}]->(st);
    """,

    # Create TESTED_FOR_TOXICITY Relationships
    f"""
    LOAD CSV WITH HEADERS FROM '{file_2}' AS row
    MATCH (s:Substance {{DTXSID: row.DTXSID}})
    WITH s, row
    MATCH (sp:Species {{species: row.species}})
    MERGE (s)-[:TESTED_FOR_TOXICITY {{
      tox_value: COALESCE(row.tox_value_mg_L, 'NA'), 
      tox_source: COALESCE(row.tox_source, 'NA')
    }}]->(sp);
    """
]

# Function to execute Cypher queries
def execute_cypher_queries(driver, queries):
    with driver.session() as session:
        for query in queries:
            print(f"Executing query:\n{query}")
            session.run(query)
            print("Query executed successfully.\n")

# Main function to connect to Neo4j and run the queries
def main():
    # Create Neo4j driver
    driver = GraphDatabase.driver(uri, auth=(user, password))

    try:
        # Execute the Cypher queries
        execute_cypher_queries(driver, cypher_queries)
    finally:
        # Close the Neo4j driver connection
        driver.close()

if __name__ == "__main__":
    main()
