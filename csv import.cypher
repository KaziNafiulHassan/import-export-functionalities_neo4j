// Set up the file paths to your GitHub raw CSV files
// These files contain data related to substances, sites, and species
:param {
  file_0: 'https://raw.githubusercontent.com/KaziNafiulHassan/import-export-functionalities_neo4j/main/01_chemical_data.csv', // Chemical data (Substances and Use Groups)
  file_1: 'https://raw.githubusercontent.com/KaziNafiulHassan/import-export-functionalities_neo4j/main/03_exposure_data_measured_simplified2.csv', // Exposure data (Sites and Measurements)
  file_2: 'https://raw.githubusercontent.com/KaziNafiulHassan/import-export-functionalities_neo4j/main/05_hazard_data.csv' // Hazard data (Species and Toxicity)
};

// Constraints
// Ensure unique constraints on certain properties for each node type
CREATE CONSTRAINT Name_Substance_uniq IF NOT EXISTS FOR (n:Substance) REQUIRE (n.Name) IS UNIQUE;
CREATE CONSTRAINT river_basin_name_n_Site_uniq IF NOT EXISTS FOR (n:Site) REQUIRE (n.river_basin_name_n) IS UNIQUE;
CREATE CONSTRAINT species_Species_uniq IF NOT EXISTS FOR (n:Species) REQUIRE (n.species) IS UNIQUE;
CREATE CONSTRAINT use_group_Use_Group_uniq IF NOT EXISTS FOR (n:Use_Group) REQUIRE (n.use_group) IS UNIQUE;

// Load Substance Nodes
// Read and import substance data from file_0
LOAD CSV WITH HEADERS FROM $file_0 AS row
WITH row WHERE NOT row.preferredName IS NULL
MERGE (n:Substance { Name: row.preferredName })
SET n.DTXSID = row.DTXSID, n.casrn = row.casrn;

// Load Site Nodes
// Read and import site data from file_1
LOAD CSV WITH HEADERS FROM $file_1 AS row
WITH row WHERE NOT row.river_basin_name_n IS NULL
MERGE (n:Site { river_basin_name_n: row.river_basin_name_n })
SET n.DTXSID = row.DTXSID,
    n.lat = row.lat, 
    n.lon = row.lon, 
    n.country = row.country, 
    n.water_body_name_n = row.water_body_name_n;

// Load Species Nodes
// Read and import species data from file_2
LOAD CSV WITH HEADERS FROM $file_2 AS row
WITH row WHERE NOT row.species IS NULL
MERGE (n:Species { species: row.species })
ON CREATE SET n.DTXSID = row.DTXSID, n.species = row.species;

// Load Use_Group Nodes
// Read and import use group data from file_0
LOAD CSV WITH HEADERS FROM $file_0 AS row
WITH row WHERE NOT row.use_group IS NULL
MERGE (n:Use_Group { use_group: row.use_group });

// Relationships
// Create relationships between nodes with relevant properties

// Relationship: Substance -> Use_Group (REGISTERED_AS)
// Create a relationship between a Substance and a Use_Group indicating registration
LOAD CSV WITH HEADERS FROM $file_0 AS row
WITH row
MATCH (sub:Substance { DTXSID: row.DTXSID }), (use:Use_Group { use_group: row.use_group })
MERGE (sub)-[r:REGISTERED_AS]->(use);

// Relationship: Substance -> Site (MEASURED_AT) with additional properties
// Create a relationship between a Substance and a Site where the substance was measured
LOAD CSV WITH HEADERS FROM $file_1 AS row
WITH row
MATCH (sub:Substance { DTXSID: row.DTXSID }), (site:Site { river_basin_name_n: row.river_basin_name_n })
MERGE (sub)-[r:MEASURED_AT]->(site)
SET r.concentration_value = row.concentration_value, 
    r.concentration_unit = row.concentration_unit, 
    r.time_point = row.time_point;

// Relationship: Substance -> Site (IS_DRIVER) with additional properties
// Create a relationship indicating that the substance is a driver at the site
LOAD CSV WITH HEADERS FROM $file_1 AS row
WITH row
MATCH (sub:Substance { DTXSID: row.DTXSID }), (site:Site { river_basin_name_n: row.river_basin_name_n })
MERGE (sub)-[r:IS_DRIVER]->(site)
SET r.driver_importance = row.driver_importance, 
    r.time_point = row.time_point;

// Relationship: Substance -> Species (TESTED_FOR_TOXICITY) with additional properties
// Create a relationship indicating that the substance was tested for toxicity on a species
LOAD CSV WITH HEADERS FROM $file_2 AS row
WITH row
MATCH (sub:Substance { DTXSID: row.DTXSID }), (spec:Species { species: row.species })
MERGE (sub)-[r:TESTED_FOR_TOXICITY]->(spec)
SET r.tox_value_mg_L = row.tox_value_mg_L, 
    r.tox_stat = row.tox_stat, 
    r.tox_source = row.tox_source;

// Relationship: Site -> Species (IMPACT_ON) with additional properties
// Create a relationship indicating the impact of a site on a species
LOAD CSV WITH HEADERS FROM $file_2 AS row
WITH row
MATCH (site:Site { DTXSID: row.DTXSID }), (spec:Species { species: row.species })
MERGE (site)-[r:IMPACT_ON]->(spec)
SET r.sum_TU = row.sum_TU, 
    r.time_point = row.time_point;
