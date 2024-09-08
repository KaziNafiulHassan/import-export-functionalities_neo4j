// Step 1: Create unique constraints to avoid duplicates
CREATE CONSTRAINT FOR (s:Substance) REQUIRE s.DTXSID IS UNIQUE;
CREATE CONSTRAINT FOR (st:Site) REQUIRE (st.station_name_n, st.lat, st.lon) IS UNIQUE;

// Step 2: Import Substance Nodes in batches using apoc.periodic.iterate
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///01_chemical_data.csv' AS row RETURN row",
  "MERGE (s:Substance {DTXSID: row.DTXSID})
   ON CREATE SET 
     s.Name = COALESCE(row.Name, 'NA'),
     s.cas_number = COALESCE(row.cas_number, 'NA'),
     s.inchi = COALESCE(row.inchi, 'NA'),
     s.inchiKey = COALESCE(row.inchiKey, 'NA')",
  {batchSize: 10000, iterateList: true}
);

// Step 3: Import Site Nodes in batches
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///03_exposure_data_measured.csv' AS row RETURN row",
  "MERGE (st:Site {station_name_n: row.station_name_n, lat: COALESCE(row.lat, 'NA'), lon: COALESCE(row.lon, 'NA')})
   ON CREATE SET
     st.country = COALESCE(row.country, 'NA'),
     st.water_body_name_n = COALESCE(row.water_body_name_n, 'NA'),
     st.river_basin_name_n = COALESCE(row.river_basin_name_n, 'NA')",
  {batchSize: 10000, iterateList: true}
);

// Step 4: Create 3 main Species nodes
MERGE (sp1:Species {species: 'fish'})
MERGE (sp2:Species {species: 'algae'})
MERGE (sp3:Species {species: 'crustacean'});

// Step 5: Create MEASURED_AT Relationships in batches
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///03_exposure_data_measured.csv' AS row RETURN row",
  "MATCH (s:Substance {DTXSID: row.DTXSID})
   MATCH (st:Site {station_name_n: row.station_name_n})
   MERGE (s)-[:MEASURED_AT {
     concentration: COALESCE(row.concentration_value, 'NA'), 
     concentration_unit: COALESCE(row.concentration_unit, 'NA'),
     time_point: COALESCE(row.time_point, 'NA')
   }]->(st)",
  {batchSize: 10000, iterateList: true}
);

// Step 6: Create TESTED_FOR_TOXICITY Relationships in batches
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///05_hazard_data.csv' AS row RETURN row",
  "MATCH (s:Substance {DTXSID: row.DTXSID})
   MATCH (sp:Species {species: row.species})
   MERGE (s)-[:TESTED_FOR_TOXICITY {
     tox_value: COALESCE(row.tox_value_mg_L, 'NA'), 
     tox_source: COALESCE(row.tox_source, 'NA')
   }]->(sp)",
  {batchSize: 10000, iterateList: true}
);
