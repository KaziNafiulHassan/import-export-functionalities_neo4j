// Set up the file paths to your GitHub raw CSV files
:param {
  file_0: 'https://raw.githubusercontent.com/KaziNafiulHassan/import-export-functionalities_neo4j/main/01_chemical_data.csv',
  file_1: 'https://raw.githubusercontent.com/KaziNafiulHassan/import-export-functionalities_neo4j/main/03_exposure_data_measured_simplified2.csv',
  file_2: 'https://raw.githubusercontent.com/KaziNafiulHassan/import-export-functionalities_neo4j/main/05_hazard_data.csv'
};

// Constraints
CREATE CONSTRAINT Name_Substance_uniq IF NOT EXISTS FOR (n:Substance) REQUIRE (n.Name) IS UNIQUE;
CREATE CONSTRAINT river_basin_name_n_Site_uniq IF NOT EXISTS FOR (n:Site) REQUIRE (n.river_basin_name_n) IS UNIQUE;
CREATE CONSTRAINT species_Species_uniq IF NOT EXISTS FOR (n:Species) REQUIRE (n.species) IS UNIQUE;

// Load Substance Nodes
LOAD CSV WITH HEADERS FROM $file_0 AS row
WITH row WHERE NOT row.preferredName IS NULL
MERGE (n:Substance { Name: row.preferredName })
SET n.DTXSID = row.DTXSID, n.casrn = row.casrn;

// Load Site Nodes
LOAD CSV WITH HEADERS FROM $file_1 AS row
WITH row WHERE NOT row.river_basin_name_n IS NULL
MERGE (n:Site { river_basin_name_n: row.river_basin_name_n })
SET n.DTXSID = row.DTXSID, n.lat = row.lat, n.lon = row.lon, n.country = row.country, n.water_body_name_n = row.water_body_name_n;

// Load Species Nodes
LOAD CSV WITH HEADERS FROM $file_2 AS row
WITH row WHERE NOT row.species IS NULL
MERGE (n:Species { species: row.species })
ON CREATE SET n.DTXSID = row.DTXSID
ON CREATE SET n.species = row.species, n.DTXSID = row.DTXSID;
// Ensure Correct Labeling of Species Nodes
MATCH (n:Species)
SET n.name = n.species
REMOVE n.DTXSID

// Relationships
// Substance -> Site (MEASURED_AT)
LOAD CSV WITH HEADERS FROM $file_1 AS row
WITH row
MATCH (sub:Substance { DTXSID: row.DTXSID }), (site:Site { river_basin_name_n: row.river_basin_name_n })
MERGE (sub)-[r:MEASURED_AT]->(site);

// Substance -> Species (TESTED_FOR_TOXICITY)
LOAD CSV WITH HEADERS FROM $file_2 AS row
WITH row
MATCH (sub:Substance { DTXSID: row.DTXSID }), (spec:Species { species: row.species })
MERGE (sub)-[r:TESTED_FOR_TOXICITY]->(spec);

// Site -> Species (IMPACT_ON)
LOAD CSV WITH HEADERS FROM $file_2 AS row
WITH row
MATCH (site:Site { DTXSID: row.DTXSID }), (spec:Species { species: row.species })
MERGE (site)-[r:IMPACT_ON]->(spec);
