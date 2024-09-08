// Create unique constraints
CREATE CONSTRAINT FOR (s:Substance) REQUIRE s.DTXSID IS UNIQUE;
CREATE CONSTRAINT FOR (st:Site) REQUIRE (st.station_name_n, st.lat, st.lon) IS UNIQUE;

// Import Substance Node
LOAD CSV WITH HEADERS FROM 'file:///01_chemical_data.csv' AS row
MERGE (s:Substance {DTXSID: row.DTXSID})
ON CREATE SET 
  s.Name = COALESCE(row.Name, 'NA'),
  s.cas_number = COALESCE(row.cas_number, 'NA'),
  s.inchi = COALESCE(row.inchi, 'NA'),
  s.inchiKey = COALESCE(row.inchiKey, 'NA');

// Import Site Nodes and replace null lat/lon with 'NA'
LOAD CSV WITH HEADERS FROM 'file:///03_exposure_data_measured.csv' AS row
MERGE (st:Site {station_name_n: row.station_name_n, lat: COALESCE(row.lat, 'NA'), lon: COALESCE(row.lon, 'NA')})
ON CREATE SET
  st.country = COALESCE(row.country, 'NA'),
  st.water_body_name_n = COALESCE(row.water_body_name_n, 'NA'),
  st.river_basin_name_n = COALESCE(row.river_basin_name_n, 'NA');

// Create 3 main Species nodes
MERGE (sp1:Species {species: 'fish'})
MERGE (sp2:Species {species: 'algae'})
MERGE (sp3:Species {species: 'crustacean'});

// Link substances to site nodes with concentration values
LOAD CSV WITH HEADERS FROM 'file:///03_exposure_data_measured.csv' AS row
MATCH (s:Substance {DTXSID: row.DTXSID})  // Match the substance using DTXSID
MATCH (st:Site {station_name_n: row.station_name_n})  // Match the site by name or location
MERGE (s)-[:MEASURED_AT {
  concentration: COALESCE(row.concentration_value, 'NA'), 
  concentration_unit: COALESCE(row.concentration_unit, 'NA'),
  time_point: COALESCE(row.time_point, 'NA')
}]->(st);

// Link substances to species nodes with toxicity values
LOAD CSV WITH HEADERS FROM 'file:///05_hazard_data.csv' AS row
MATCH (s:Substance {DTXSID: row.DTXSID})  // Match the substance using DTXSID
WITH s, row
// Match the correct species node based on the species in the row
MATCH (sp:Species {species: row.species})
MERGE (s)-[:TESTED_FOR_TOXICITY {
  tox_value: COALESCE(row.tox_value_mg_L, 'NA'), 
  tox_source: COALESCE(row.tox_source, 'NA')
}]->(sp);

