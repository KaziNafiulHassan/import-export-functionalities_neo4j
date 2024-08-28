import requests
from neo4j import GraphDatabase

# Neo4j connection details
uri = "bolt://localhost:7687"
username = "neo4j"
password = "password"

driver = GraphDatabase.driver(uri, auth=(username, password))

# API endpoints to fetch JSON data
api_urls = {
    "file_0": "http://127.0.0.1:5001/api/data1",
    "file_1": "http://127.0.0.1:5001/api/data2",
    "file_2": "http://127.0.0.1:5001/api/data3"
}

# Function to fetch JSON data from an API endpoint
def fetch_json_data(url):
    response = requests.get(url)
    response.raise_for_status()  # Raise an error if the request fails
    return response.json()

# Function to execute a Cypher query
def execute_query(query, parameters=None):
    with driver.session() as session:
        session.run(query, parameters)

# Fetch JSON data from all APIs
data_0 = fetch_json_data(api_urls["file_0"])
data_1 = fetch_json_data(api_urls["file_1"])
data_2 = fetch_json_data(api_urls["file_2"])

# Constraints
constraints = [
    """CREATE CONSTRAINT Name_Substance_uniq IF NOT EXISTS 
       FOR (n:Substance) REQUIRE (n.Name) IS UNIQUE""",
    """CREATE CONSTRAINT river_basin_name_n_Site_uniq IF NOT EXISTS 
       FOR (s:Site) REQUIRE (s.river_basin_name_n) IS UNIQUE""",
    """CREATE CONSTRAINT species_DTXSID_uniq IF NOT EXISTS 
       FOR (sp:Species) REQUIRE (sp.species, sp.DTXSID) IS UNIQUE""",
    """DROP CONSTRAINT species_Species_uniq IF EXISTS""",
    """CREATE CONSTRAINT use_group_Use_Group_uniq IF NOT EXISTS 
       FOR (u:Use_Group) REQUIRE (u.use_group) IS UNIQUE"""
]

for query in constraints:
    execute_query(query)

# Load Substance Nodes with null checks
for item in data_0:
    if item.get("preferredName") and item.get("DTXSID") and item.get("casrn"):
        query = """
        MERGE (sub:Substance { Name: $preferredName })
        SET sub.DTXSID = $DTXSID, 
            sub.casrn = $casrn;
        """
        execute_query(query, parameters={
            "preferredName": item["preferredName"],
            "DTXSID": item["DTXSID"],
            "casrn": item["casrn"]
        })

# Load Use_Group Nodes with null checks
for item in data_0:
    if item.get("USE_GROUP"):
        query = """
        MERGE (use:Use_Group { use_group: $USE_GROUP });
        """
        execute_query(query, parameters={"USE_GROUP": item["USE_GROUP"]})

# Load Site Nodes with null checks
for item in data_1:
    if item.get("river_basin_name_n") and item.get("DTXSID"):
        query = """
        MERGE (site:Site { river_basin_name_n: $river_basin_name_n })
        SET site.DTXSID = $DTXSID,
            site.lat = $lat,
            site.lon = $lon, 
            site.country = $country, 
            site.water_body_name_n = $water_body_name_n;
        """
        execute_query(query, parameters={
            "river_basin_name_n": item["river_basin_name_n"],
            "DTXSID": item["DTXSID"],
            "lat": item["lat"],
            "lon": item["lon"],
            "country": item["country"],
            "water_body_name_n": item["water_body_name_n"]
        })

# Load Species Nodes with null checks
for item in data_2:
    if item.get("species"):
        query = """
        MERGE (spec:Species { species: $species })
        SET spec.species = $species;
        """
        execute_query(query, parameters={"species": item["species"]})

# Relationships with null checks
# Substance -> Use_Group (REGISTERED_AS)
for item in data_0:
    if item.get("DTXSID") and item.get("USE_GROUP"):
        query = """
        MATCH (sub:Substance { DTXSID: $DTXSID }), (use:Use_Group { use_group: $USE_GROUP })
        MERGE (sub)-[r1:REGISTERED_AS]->(use);
        """
        execute_query(query, parameters={
            "DTXSID": item["DTXSID"],
            "USE_GROUP": item["USE_GROUP"]
        })

# Substance -> Site (MEASURED_AT) with additional properties
for item in data_1:
    if item.get("DTXSID") and item.get("river_basin_name_n"):
        query = """
        MATCH (sub:Substance { DTXSID: $DTXSID }), (site:Site { river_basin_name_n: $river_basin_name_n })
        MERGE (sub)-[r2:MEASURED_AT]->(site)
        SET r2.concentration_value = toFloat($concentration_value), 
            r2.concentration_unit = $concentration_unit, 
            r2.time_point = $time_point;
        """
        execute_query(query, parameters={
            "DTXSID": item["DTXSID"],
            "river_basin_name_n": item["river_basin_name_n"],
            "concentration_value": item["concentration_value"],
            "concentration_unit": item["concentration_unit"],
            "time_point": item["time_point"]
        })

# Substance -> Site (IS_DRIVER) with additional properties
for item in data_1:
    if item.get("DTXSID") and item.get("river_basin_name_n"):
        query = """
        MATCH (sub:Substance { DTXSID: $DTXSID }), (site:Site { river_basin_name_n: $river_basin_name_n })
        MERGE (sub)-[r3:IS_DRIVER]->(site)
        SET r3.driver_importance = $DRIVER_IMPORTANCE, 
            r3.time_point = $time_point;
        """
        execute_query(query, parameters={
            "DTXSID": item["DTXSID"],
            "river_basin_name_n": item["river_basin_name_n"],
            "DRIVER_IMPORTANCE": item.get("DRIVER_IMPORTANCE"),  # Use .get() to avoid KeyError
            "time_point": item.get("time_point")  # Use .get() to avoid KeyError
        })

# Substance -> Species (TESTED_FOR_TOXICITY) with additional properties
for item in data_2:
    if item.get("DTXSID") and item.get("species"):
        query = """
        MATCH (sub:Substance { DTXSID: $DTXSID }), (spec:Species { species: $species })
        MERGE (sub)-[r4:TESTED_FOR_TOXICITY]->(spec)
        SET r4.tox_value_mg_L = toFloat($tox_value_mg_L), 
            r4.tox_stat = $tox_stat, 
            r4.tox_source = $tox_source;
        """
        execute_query(query, parameters={
            "DTXSID": item["DTXSID"],
            "species": item["species"],
            "tox_value_mg_L": item["tox_value_mg_L"],
            "tox_stat": item["tox_stat"],
            "tox_source": item["tox_source"]
        })

# Site -> Species (IMPACT_ON) with additional properties
for item in data_2:
    if item.get("DTXSID") and item.get("species"):
        query = """
        MATCH (site:Site { DTXSID: $DTXSID }), (spec:Species { species: $species })
        MERGE (site)-[r5:IMPACT_ON]->(spec);
        """
        execute_query(query, parameters={
            "DTXSID": item["DTXSID"],
            "species": item["species"]
        })

# Close the driver connection
driver.close()
