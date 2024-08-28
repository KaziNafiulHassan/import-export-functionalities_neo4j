// Constraints
CREATE CONSTRAINT Name_Substance_uniq IF NOT EXISTS FOR (n:Substance) REQUIRE (n.Name) IS UNIQUE;
CREATE CONSTRAINT river_basin_name_n_Site_uniq IF NOT EXISTS FOR (s:Site) REQUIRE (s.river_basin_name_n) IS UNIQUE;
CREATE CONSTRAINT species_DTXSID_uniq IF NOT EXISTS FOR (sp:Species) REQUIRE (sp.species, sp.DTXSID) IS UNIQUE;
DROP CONSTRAINT species_Species_uniq IF EXISTS;
CREATE CONSTRAINT use_group_Use_Group_uniq IF NOT EXISTS FOR (u:Use_Group) REQUIRE (u.use_group) IS UNIQUE;

// Load Substance Nodes
LOAD CSV WITH HEADERS FROM 'file:///01_chemical_data.csv' AS line
MERGE (sub:Substance { Name: line.preferredName })
SET sub.DTXSID = line.DTXSID, 
    sub.casrn = line.casrn;

// Load Use_Group Nodes
LOAD CSV WITH HEADERS FROM 'file:///01_chemical_data.csv' AS line
MERGE (use:Use_Group { use_group: line.USE_GROUP });

// Load Site Nodes
LOAD CSV WITH HEADERS FROM 'file:///03_exposure_data_measured_simplified2.csv' AS line
MERGE (site:Site { river_basin_name_n: line.river_basin_name_n })
SET site.DTXSID = line.DTXSID,
    site.lat = line.lat,
    site.lon = line.lon, 
    site.country = line.country, 
    site.water_body_name_n = line.water_body_name_n;

// Load Species Nodes
LOAD CSV WITH HEADERS FROM 'file:///05_hazard_data.csv' AS line
MERGE (spec:Species { species: line.species })
SET spec.species = line.species;

// Relationships
// Substance -> Use_Group (REGISTERED_AS)
LOAD CSV WITH HEADERS FROM 'file:///01_chemical_data.csv' AS line1
MATCH (sub:Substance { DTXSID: line1.DTXSID }), (use:Use_Group { use_group: line1.USE_GROUP })
MERGE (sub)-[r1:REGISTERED_AS]->(use);

// Substance -> Site (MEASURED_AT) with additional properties
LOAD CSV WITH HEADERS FROM 'file:///03_exposure_data_measured_simplified2.csv' AS line2
MATCH (sub:Substance { DTXSID: line2.DTXSID }), (site:Site { river_basin_name_n: line2.river_basin_name_n })
MERGE (sub)-[r2:MEASURED_AT]->(site)
SET r2.concentration_value = toFloat(line2.concentration_value), 
    r2.concentration_unit = line2.concentration_unit, 
    r2.time_point = line2.time_point;

// Substance -> Site (IS_DRIVER) with additional properties
LOAD CSV WITH HEADERS FROM 'file:///03_exposure_data_measured_simplified2.csv' AS line3
MATCH (sub:Substance { DTXSID: line3.DTXSID }), (site:Site { river_basin_name_n: line3.river_basin_name_n })
MERGE (sub)-[r3:IS_DRIVER]->(site)
SET r3.driver_importance = line3.DRIVER_IMPORTANCE, 
    r3.time_point = line3.time_point;

// Substance -> Species (TESTED_FOR_TOXICITY) with additional properties
LOAD CSV WITH HEADERS FROM 'file:///05_hazard_data.csv' AS line4
MATCH (sub:Substance { DTXSID: line4.DTXSID }), (spec:Species { species: line4.species })
MERGE (sub)-[r4:TESTED_FOR_TOXICITY]->(spec)
SET r4.tox_value_mg_L = toFloat(line4.tox_value_mg_L), 
    r4.tox_stat = line4.tox_stat, 
    r4.tox_source = line4.tox_source;

// Site -> Species (IMPACT_ON) with additional properties
LOAD CSV WITH HEADERS FROM 'file:///05_hazard_data.csv' AS line5
MATCH (site:Site { DTXSID: line5.DTXSID }), (spec:Species { species: line5.species })
MERGE (site)-[r5:IMPACT_ON]->(spec);
