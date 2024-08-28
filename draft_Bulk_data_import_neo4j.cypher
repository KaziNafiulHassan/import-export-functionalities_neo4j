// Each queries has to be imported one by one. Otherwise it will take a long time.
// Site Node Creation
CALL apoc.periodic.iterate(
  'LOAD CSV WITH HEADERS FROM "file:///03_exposure_data_measured.csv" AS line RETURN line',
  'MERGE (site:Site { river_basin_name_n: line.river_basin_name_n })
  SET site.DTXSID = line.DTXSID,
    site.lat = line.lat,
    site.lon = line.lon, 
    site.country = line.country, 
    site.water_body_name_n = line.water_body_name_n',
    {batchSize: 1000, iterateList: true}
     );

// Substance Node Creation
