-- All China pairs, grouped daily. GoogleSQL. SELECT only.
-- @start_date / @end_date: DATE, inclusive / exclusive.
-- @outlet_country_json: compact JSON string [[domain,country_fips], ...].
-- @country_domain_json: contents of data/gdelt/outlet_lookup/country_domain_parameter.json.
-- ccTLD countries are clues, not verified publisher headquarters.
-- Lookup countries and geography below use FIPS, NOT ISO codes.
-- Domains denote publishing sites, not independent ownership or wire origin.
WITH mapping AS (
  SELECT LOWER(JSON_VALUE(item, '$[0]')) AS domain,
    IF(COUNT(DISTINCT JSON_VALUE(item, '$[1]')) = 1,
       MAX(JSON_VALUE(item, '$[1]')), NULL) AS estimated_country
  FROM UNNEST(JSON_QUERY_ARRAY(@outlet_country_json)) item
  GROUP BY domain
), country_domains AS (
  SELECT LOWER(JSON_VALUE(item, '$[0]')) AS suffix,
    IF(COUNT(DISTINCT JSON_VALUE(item, '$[1]')) = 1,
       MAX(JSON_VALUE(item, '$[1]')), NULL) AS domain_country
  FROM UNNEST(JSON_QUERY_ARRAY(@country_domain_json)) item
  GROUP BY suffix
), documents AS (
  SELECT DATE(_PARTITIONTIME) AS day,
    DocumentIdentifier AS url,
    V2Locations,
    LOWER(NET.REG_DOMAIN(DocumentIdentifier)) AS outlet,
    ARRAY(SELECT DISTINCT SPLIT(loc, '#')[SAFE_OFFSET(2)]
          FROM UNNEST(SPLIT(COALESCE(V2Locations, ''), ';')) loc
          WHERE REGEXP_CONTAINS(SPLIT(loc, '#')[SAFE_OFFSET(2)], r'^[A-Z]{2}$')) AS countries
  FROM `gdelt-bq.gdeltv2.gkg_partitioned`
  WHERE _PARTITIONTIME >= TIMESTAMP(@start_date)
    AND _PARTITIONTIME < TIMESTAMP(@end_date)
    AND REGEXP_CONTAINS(DocumentIdentifier, r'(?i)^https?://')
), evidence AS (
  SELECT d.*, m.estimated_country,
    c.domain_country
  FROM documents d LEFT JOIN mapping m ON d.outlet = m.domain
  LEFT JOIN country_domains c
    ON REGEXP_EXTRACT(LOWER(RTRIM(outlet, '.')), r'\.([^.]+)$') = c.suffix
), classified AS (
  SELECT *,
    CASE WHEN estimated_country IS NOT NULL AND domain_country IS NOT NULL
                   AND estimated_country != domain_country THEN NULL
         ELSE COALESCE(estimated_country, domain_country) END AS outlet_country,
    CASE WHEN estimated_country IS NOT NULL AND domain_country IS NOT NULL
                   AND estimated_country != domain_country THEN 'conflict'
         WHEN estimated_country = domain_country THEN 'estimate_domain_agree'
         WHEN estimated_country IS NOT NULL THEN 'estimate_only'
         WHEN domain_country IS NOT NULL THEN 'domain_only'
         ELSE 'unknown' END AS classification_basis
  FROM evidence
)
SELECT day,url,outlet,outlet_country,classification_basis,estimated_country,domain_country,
 CASE WHEN outlet_country='CH' THEN 'Chinese' WHEN outlet_country='KE' THEN 'Local'
      WHEN outlet_country IS NULL THEN 'Unknown or uncertain' ELSE 'Third country' END AS outlet_group,
 ARRAY_AGG(DISTINCT V2Locations IGNORE NULLS) AS raw_locations, COUNT(*) AS source_records
FROM classified
WHERE (outlet_country='CH' AND 'KE' IN UNNEST(countries))
 OR (outlet_country='KE' AND 'CH' IN UNNEST(countries))
 OR ('CH' IN UNNEST(countries) AND 'KE' IN UNNEST(countries))
GROUP BY day,url,outlet,outlet_country,classification_basis,estimated_country,domain_country
