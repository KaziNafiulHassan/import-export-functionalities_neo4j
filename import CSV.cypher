// Set up the file paths to your local CSV files
:param {
  file_0: 'file:///01_chemical_data.csv',  // Chemical data (Substances and Use Groups)
  file_1: 'file:///03_exposure_data_measured_simplified2.csv',  // Exposure data (Sites and Measurements)
  file_2: 'file:///05_hazard_data.csv'  // Hazard data (Species and Toxicity)
};

// Constraints
CREATE CONSTRAINT Name_Substance_uniq IF NOT EXISTS FOR (n:Substance) REQUIRE (n.Name) IS UNIQUE;
CREATE CONSTRAINT river_basin_name_n_Site_uniq IF NOT EXISTS FOR (n:Site) REQUIRE (n.river_basin_name_n) IS UNIQUE;
CREATE CONSTRAINT species_Species_uniq IF NOT EXISTS FOR (n:Species) REQUIRE (n.species) IS UNIQUE;
CREATE CONSTRAINT use_group_Use_Group_uniq IF NOT EXISTS FOR (n:Use_Group) REQUIRE (n.use_group) IS UNIQUE;

// Load Substance Nodes
LOAD CSV WITH HEADERS FROM $file_0 AS row
WITH row WHERE NOT row.preferredName IS NULL
MERGE (n:Substance { Name: row.preferredName })
SET n.DTXSID = row.DTXSID, n.casrn = row.casrn;

// Load Site Nodes
LOAD CSV WITH HEADERS FROM $file_1 AS row
WITH row WHERE NOT row.river_basin_name_n IS NULL
MERGE (n:Site { river_basin_name_n: row.river_basin_name_n })
SET n.DTXSID = row.DTXSID,
    n.lat = row.lat, 
    n.lon = row.lon, 
    n.country = row.country, 
    n.water_body_name_n = row.water_body_name_n;

// Load Species Nodes
LOAD CSV WITH HEADERS FROM $file_2 AS row
WITH row WHERE NOT row.species IS NULL
MERGE (n:Species { species: row.species })
ON CREATE SET n.DTXSID = row.DTXSID, n.species = row.species;

// Load Use_Group Nodes
LOAD CSV WITH HEADERS FROM $file_0 AS row
WITH row WHERE NOT row.use_group IS NULL
MERGE (n:Use_Group { use_group: row.use_group });

// Relationships
// Substance -> Use_Group (REGISTERED_AS)
LOAD CSV WITH HEADERS FROM $file_0 AS row
WITH row
MATCH (sub:Substance { DTXSID: row.DTXSID }), (use:Use_Group { use_group: row.use_group })
MERGE (sub)-[r:REGISTERED_AS]->(use);

// Substance -> Site (MEASURED_AT) with additional properties
LOAD CSV WITH HEADERS FROM $file_1 AS row
WITH row
MATCH (sub:Substance { DTXSID: row.DTXSID }), (site:Site { river_basin_name_n: row.river_basin_name_n })
MERGE (sub)-[r:MEASURED_AT]->(site)
SET r.concentration_value = row.concentration_value, 
    r.concentration_unit = row.concentration_unit, 
    r.time_point = row.time_point;

// Substance -> Site (IS_DRIVER) with additional properties
LOAD CSV WITH HEADERS FROM $file_1 AS row
WITH row
MATCH (sub:Substance { DTXSID: row.DTXSID }), (site:Site { river_basin_name_n: row.river_basin_name_n })
MERGE (sub)-[r:IS_DRIVER]->(site)
SET r.driver_importance = row.driver_importance, 
    r.time_point = row.time_point;

// Substance -> Species (TESTED_FOR_TOXICITY) with additional properties
LOAD CSV WITH HEADERS FROM $file_2 AS row
WITH row
MATCH (sub:Substance { DTXSID: row.DTXSID }), (spec:Species { species: row.species })
MERGE (sub)-[r:TESTED_FOR_TOXICITY]->(spec)
SET r.tox_value_mg_L = row.tox_value_mg_L, 
    r.tox_stat = row.tox_stat, 
    r.tox_source = row.tox_source;

// Site -> Species (IMPACT_ON) with additional properties
LOAD CSV WITH HEADERS FROM $file_2 AS row
WITH row
MATCH (site:Site { DTXSID: row.DTXSID }), (spec:Species { species: row.species })
MERGE (site)-[r:IMPACT_ON]->(spec)
SET r.sum_TU = row.sum_TU, 
    r.time_point = row.time_point;
