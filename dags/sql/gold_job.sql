-- =============================================================================
-- Gold Layer — UK Road Safety Data
-- Database: SPARQ_CHALLENGE
-- Schema:   GOLD
-- Tool:     Power BI
--
-- Design principles:
--   - One table per dashboard KPI group
--   - Pre-aggregated: one row per group, no joins needed in Power BI
--   - Dimension labels joined here so Power BI receives readable strings
--   - IS_PROVISIONAL flag included in all tables for year filtering
--
-- Execution order:
--   1. gold_yoy_trends.sql
--   2. gold_severity_by_context.sql
--   3. gold_casualty_profile.sql
--   4. gold_hotspots.sql
--   5. gold_vehicle_profile.sql
-- =============================================================================


-- =============================================================================
-- Step 1 — GOLD.YOY_TRENDS
-- Grain: one row per (year, is_provisional)
-- KPI:   total accidents, total casualties, fatal / serious / slight counts
--        and YoY % change for each metric
-- =============================================================================
CREATE OR REPLACE TABLE SPARQ_CHALLENGE.GOLD.YOY_TRENDS AS

WITH base AS (
    SELECT
        a.accident_year,
        a.is_provisional,
        COUNT(DISTINCT a.accident_index)                            AS total_accidents,
        SUM(a.number_of_casualties)                                 AS total_casualties,
        COUNT_IF(s.label = 'Fatal')                                 AS fatal_accidents,
        COUNT_IF(s.label = 'Serious')                               AS serious_accidents,
        COUNT_IF(s.label = 'Slight')                                AS slight_accidents
    FROM SPARQ_CHALLENGE.SILVER.F_ACCIDENTS            a
    LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_ACCIDENT_SEVERITY s
           ON a.accident_severity = TRY_CAST(s.code AS INT)
    GROUP BY a.accident_year, a.is_provisional
),

with_yoy AS (
    SELECT
        accident_year,
        is_provisional,
        total_accidents,
        total_casualties,
        fatal_accidents,
        serious_accidents,
        slight_accidents,
        -- YoY % change vs previous year (NULL for first year in series)
        ROUND(
            (total_accidents - LAG(total_accidents) OVER (ORDER BY accident_year))
            / NULLIF(LAG(total_accidents) OVER (ORDER BY accident_year), 0) * 100
        , 2)                                                        AS yoy_pct_accidents,
        ROUND(
            (fatal_accidents - LAG(fatal_accidents) OVER (ORDER BY accident_year))
            / NULLIF(LAG(fatal_accidents) OVER (ORDER BY accident_year), 0) * 100
        , 2)                                                        AS yoy_pct_fatal,
        ROUND(
            (total_casualties - LAG(total_casualties) OVER (ORDER BY accident_year))
            / NULLIF(LAG(total_casualties) OVER (ORDER BY accident_year), 0) * 100
        , 2)                                                        AS yoy_pct_casualties
    FROM base
)

SELECT
    *,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM with_yoy
ORDER BY accident_year;


-- =============================================================================
-- Step 2 — GOLD.SEVERITY_BY_CONTEXT
-- Grain: one row per (year, severity_label, context_type, context_value)
-- KPI:   accident count and fatal rate by speed limit, urban/rural,
--        light conditions — all in a single unpivoted table for easy
--        Power BI slicing without maintaining 3 separate tables
-- =============================================================================
CREATE OR REPLACE TABLE SPARQ_CHALLENGE.GOLD.SEVERITY_BY_CONTEXT AS

WITH accidents_labeled AS (
    SELECT
        a.accident_year,
        a.is_provisional,
        a.accident_index,
        sev.label                                                   AS severity_label,
        spd.label                                                   AS speed_limit_label,
        urb.label                                                   AS urban_rural_label,
        lgt.label                                                   AS light_conditions_label
    FROM SPARQ_CHALLENGE.SILVER.F_ACCIDENTS                 a
    LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_ACCIDENT_SEVERITY  sev ON a.accident_severity    = TRY_CAST(sev.code AS INT)
    LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_SPEED_LIMIT        spd ON a.speed_limit          = TRY_CAST(spd.code AS INT)
    LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_URBAN_RURAL        urb ON a.urban_or_rural_area  = TRY_CAST(urb.code AS INT)
    LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_LIGHT_CONDITIONS   lgt ON a.light_conditions     = TRY_CAST(lgt.code AS INT)
),

-- Unpivot context dimensions so Power BI can slice by any of them
-- using a single slicer on CONTEXT_TYPE + CONTEXT_VALUE
speed_agg AS (
    SELECT
        accident_year, is_provisional,
        'Speed Limit'                                               AS context_type,
        COALESCE(speed_limit_label, 'Unknown')                      AS context_value,
        severity_label,
        COUNT(*)                                                    AS accident_count
    FROM accidents_labeled
    GROUP BY 1, 2, 3, 4, 5
),

urban_agg AS (
    SELECT
        accident_year, is_provisional,
        'Urban / Rural'                                             AS context_type,
        COALESCE(urban_rural_label, 'Unknown')                      AS context_value,
        severity_label,
        COUNT(*)                                                    AS accident_count
    FROM accidents_labeled
    GROUP BY 1, 2, 3, 4, 5
),

light_agg AS (
    SELECT
        accident_year, is_provisional,
        'Light Conditions'                                          AS context_type,
        COALESCE(light_conditions_label, 'Unknown')                 AS context_value,
        severity_label,
        COUNT(*)                                                    AS accident_count
    FROM accidents_labeled
    GROUP BY 1, 2, 3, 4, 5
)

SELECT *, CURRENT_TIMESTAMP() AS _loaded_at FROM speed_agg
UNION ALL
SELECT *, CURRENT_TIMESTAMP() AS _loaded_at FROM urban_agg
UNION ALL
SELECT *, CURRENT_TIMESTAMP() AS _loaded_at FROM light_agg;


-- =============================================================================
-- Step 3 — GOLD.CASUALTY_PROFILE
-- Grain: one row per (year, casualty_severity, age_band, sex, casualty_type)
-- KPI:   casualty count by demographic and user type
-- =============================================================================
CREATE OR REPLACE TABLE SPARQ_CHALLENGE.GOLD.CASUALTY_PROFILE AS

SELECT
    a.accident_year,
    a.is_provisional,
    sev.label                                                       AS casualty_severity_label,
    age.label                                                       AS age_band_label,
    sex.label                                                       AS sex_label,
    ctype.label                                                     AS casualty_type_label,
    cclass.label                                                    AS casualty_class_label,
    COUNT(*)                                                        AS casualty_count,
    CURRENT_TIMESTAMP()                                             AS _loaded_at

FROM SPARQ_CHALLENGE.SILVER.F_CASUALTIES                    c
JOIN SPARQ_CHALLENGE.SILVER.F_ACCIDENTS                     a
        ON c.accident_index = a.accident_index
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_CASUALTY_SEVERITY      sev
        ON c.casualty_severity    = TRY_CAST(sev.code AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_AGE_BAND               age
        ON c.age_band_of_casualty = TRY_CAST(age.code AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_SEX_OF_CASUALTY        sex
        ON c.sex_of_casualty      = TRY_CAST(sex.code AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_CASUALTY_TYPE          ctype
        ON c.casualty_type        = TRY_CAST(ctype.code AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_CASUALTY_CLASS         cclass
        ON c.casualty_class       = TRY_CAST(cclass.code AS INT)

GROUP BY
    a.accident_year,
    a.is_provisional,
    sev.label,
    age.label,
    sex.label,
    ctype.label,
    cclass.label;


-- =============================================================================
-- Step 4 (revised) — GOLD.HOTSPOTS
-- Adds AVG(latitude) and AVG(longitude) per district as centroid proxy
-- for map visuals in Power BI
-- =============================================================================
CREATE OR REPLACE TABLE SPARQ_CHALLENGE.GOLD.HOTSPOTS AS
 
WITH base AS (
    SELECT
        a.accident_year,
        a.is_provisional,
        d.label                                                     AS local_authority_district,
        a.lsoa_of_accident_location                                 AS lsoa,
        sev.label                                                   AS severity_label,
        COUNT(*)                                                    AS accident_count,
        -- Centroid proxy: average of all accident coordinates in the district
        ROUND(AVG(a.latitude),  6)                                  AS district_lat,
        ROUND(AVG(a.longitude), 6)                                  AS district_lon,
        -- Severity score: Fatal=3, Serious=2, Slight=1
        SUM(
            CASE sev.label
                WHEN 'Fatal'   THEN 3
                WHEN 'Serious' THEN 2
                WHEN 'Slight'  THEN 1
                ELSE 0
            END
        )                                                           AS severity_score
    FROM SPARQ_CHALLENGE.SILVER.F_ACCIDENTS                 a
    LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_LOCAL_AUTHORITY_DISTRICT d
           ON a.local_authority_district = d.code
    LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_ACCIDENT_SEVERITY  sev
           ON a.accident_severity = TRY_CAST(sev.code AS INT)
    -- Exclude 2019 provisional — district and LSOA not available
    WHERE a.is_provisional = FALSE
      AND a.local_authority_district IS NOT NULL
      -- Only use rows with valid coordinates for centroid calculation
      AND a.has_geo = TRUE
    GROUP BY 1, 2, 3, 4, 5
)
 
SELECT
    *,
    RANK() OVER (
        PARTITION BY accident_year
        ORDER BY severity_score DESC
    )                                                               AS district_severity_rank,
    CURRENT_TIMESTAMP()                                             AS _loaded_at
FROM base;


-- =============================================================================
-- Step 5 — GOLD.VEHICLE_PROFILE
-- Grain: one row per (year, vehicle_type, propulsion_code, age_band_of_driver)
-- KPI:   vehicle involvement count, avg driver age, avg vehicle age
--        joined to accident severity for context
-- =============================================================================
CREATE OR REPLACE TABLE SPARQ_CHALLENGE.GOLD.VEHICLE_PROFILE AS

SELECT
    a.accident_year,
    a.is_provisional,
    vtype.label                                                     AS vehicle_type_label,
    prop.label                                                      AS propulsion_label,
    age.label                                                       AS driver_age_band_label,
    sev.label                                                       AS accident_severity_label,
    COUNT(*)                                                        AS vehicle_count,
    ROUND(AVG(v.age_of_driver),   1)                               AS avg_driver_age,
    ROUND(AVG(v.age_of_vehicle),  1)                               AS avg_vehicle_age,
    CURRENT_TIMESTAMP()                                             AS _loaded_at

FROM SPARQ_CHALLENGE.SILVER.F_VEHICLES                      v
JOIN SPARQ_CHALLENGE.SILVER.F_ACCIDENTS                     a
        ON v.accident_index   = a.accident_index
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_VEHICLE_TYPE           vtype
        ON v.vehicle_type     = TRY_CAST(vtype.code AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_VEHICLE_PROPULSION_CODE prop
        ON v.propulsion_code  = TRY_CAST(prop.code  AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_AGE_BAND               age
        ON v.age_band_of_driver = TRY_CAST(age.code AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_ACCIDENT_SEVERITY      sev
        ON a.accident_severity = TRY_CAST(sev.code  AS INT)

GROUP BY
    a.accident_year,
    a.is_provisional,
    vtype.label,
    prop.label,
    age.label,
    sev.label;


-- =============================================================================
-- Step 6 — GOLD.BLOOD_ALCOHOL_INSIGHTS
-- Grain: one row per (blood_alcohol_band, severity_of_injury,
--                     casualty_class, sex, age_band)
-- KPI:   casualty count and avg blood alcohol level by demographic
--        and injury severity — connects root cause (alcohol) to outcome
-- =============================================================================
CREATE OR REPLACE TABLE SPARQ_CHALLENGE.GOLD.BLOOD_ALCOHOL_INSIGHTS AS
 
WITH banded AS (
    SELECT
        -- Group blood alcohol levels into readable bands for Power BI slicers
        blood_alcohol_level_mg,
        CASE
            WHEN blood_alcohol_level_mg = 0              THEN '0 (Sober)'
            WHEN blood_alcohol_level_mg BETWEEN 1  AND 20  THEN '1-20 mg'
            WHEN blood_alcohol_level_mg BETWEEN 21 AND 50  THEN '21-50 mg'
            WHEN blood_alcohol_level_mg BETWEEN 51 AND 80  THEN '51-80 mg (Legal limit)'
            WHEN blood_alcohol_level_mg BETWEEN 81 AND 150 THEN '81-150 mg (Over limit)'
            WHEN blood_alcohol_level_mg > 150              THEN '150+ mg (Severely over)'
            ELSE 'Unknown'
        END                                                         AS blood_alcohol_band,
        severity_of_injury,
        casualty_class,
        sex_of_casualty,
        age_band
    FROM SPARQ_CHALLENGE.SILVER.F_BLOOD_ALCOHOL
)
 
SELECT
    blood_alcohol_band,
    severity_of_injury,
    casualty_class,
    sex_of_casualty,
    age_band,
    COUNT(*)                                                        AS casualty_count,
    ROUND(AVG(blood_alcohol_level_mg), 1)                          AS avg_blood_alcohol_level,
    ROUND(MAX(blood_alcohol_level_mg), 1)                          AS max_blood_alcohol_level,
    CURRENT_TIMESTAMP()                                             AS _loaded_at
FROM banded
GROUP BY 1, 2, 3, 4, 5;
 
 
-- =============================================================================
-- Step 7 — GOLD.MART_ACCIDENTS
-- Grain: one row per accident (accident_index)
-- Purpose: flat wide table with all dimension labels pre-resolved
--          for ad-hoc analysis in Power BI without any joins
-- Includes: accident attributes, aggregated casualty/vehicle counts,
--           and all readable labels from Silver dimensions
-- =============================================================================
CREATE OR REPLACE TABLE SPARQ_CHALLENGE.GOLD.MART_ACCIDENTS AS
 
WITH casualty_agg AS (
    -- Aggregate casualties to accident level to avoid row inflation on join
    SELECT
        accident_index,
        COUNT(*)                                                    AS total_casualties,
        COUNT_IF(casualty_severity = 1)                            AS fatal_casualties,
        COUNT_IF(casualty_severity = 2)                            AS serious_casualties,
        COUNT_IF(casualty_severity = 3)                            AS slight_casualties,
        ROUND(AVG(age_of_casualty), 1)                             AS avg_casualty_age,
        COUNT_IF(sex_of_casualty = 1)                              AS male_casualties,
        COUNT_IF(sex_of_casualty = 2)                              AS female_casualties
    FROM SPARQ_CHALLENGE.SILVER.F_CASUALTIES
    GROUP BY accident_index
),
 
vehicle_agg AS (
    -- Aggregate vehicles to accident level
    SELECT
        accident_index,
        COUNT(*)                                                    AS total_vehicles,
        ROUND(AVG(age_of_driver),  1)                              AS avg_driver_age,
        ROUND(AVG(age_of_vehicle), 1)                              AS avg_vehicle_age
    FROM SPARQ_CHALLENGE.SILVER.F_VEHICLES
    GROUP BY accident_index
)
 
SELECT
    -- -----------------------------------------------------------------------
    -- Keys & dates
    -- -----------------------------------------------------------------------
    a.accident_index,
    a.accident_date,
    a.accident_year,
    a.accident_month,
    a.accident_day,
    a.accident_hour,
    dow.label                                                       AS day_of_week_label,
    a.is_provisional,
 
    -- -----------------------------------------------------------------------
    -- Geography
    -- -----------------------------------------------------------------------
    a.latitude,
    a.longitude,
    a.lsoa_of_accident_location,
    dist.label                                                      AS local_authority_district,
    hway.label                                                      AS local_authority_highway,
 
    -- -----------------------------------------------------------------------
    -- Accident attributes — readable labels
    -- -----------------------------------------------------------------------
    sev.label                                                       AS accident_severity,
    spd.label                                                       AS speed_limit,
    rtype.label                                                     AS road_type,
    urb.label                                                       AS urban_or_rural_area,
    lgt.label                                                       AS light_conditions,
    wth.label                                                       AS weather_conditions,
    rsurf.label                                                     AS road_surface_conditions,
    jdet.label                                                      AS junction_detail,
    jctl.label                                                      AS junction_control,
    scs.label                                                       AS special_conditions_at_site,
    chaz.label                                                      AS carriageway_hazards,
    pol.label                                                       AS police_force,
    att.label                                                       AS police_officer_attended,
 
    -- -----------------------------------------------------------------------
    -- Aggregated casualty metrics (from casualty_agg CTE)
    -- -----------------------------------------------------------------------
    ca.total_casualties,
    ca.fatal_casualties,
    ca.serious_casualties,
    ca.slight_casualties,
    ca.avg_casualty_age,
    ca.male_casualties,
    ca.female_casualties,
 
    -- -----------------------------------------------------------------------
    -- Aggregated vehicle metrics (from vehicle_agg CTE)
    -- -----------------------------------------------------------------------
    va.total_vehicles,
    va.avg_driver_age,
    va.avg_vehicle_age,
 
    -- -----------------------------------------------------------------------
    -- Quality flags (passed through from Silver for filtering)
    -- -----------------------------------------------------------------------
    a.has_geo,
    a.has_lsoa,
    a.has_time,
    a.is_date_valid,
 
    CURRENT_TIMESTAMP()                                             AS _loaded_at
 
FROM SPARQ_CHALLENGE.SILVER.F_ACCIDENTS                     a
 
-- Dimension joins — all LEFT JOIN to preserve accidents with missing codes
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_ACCIDENT_SEVERITY      sev
       ON a.accident_severity           = TRY_CAST(sev.code  AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_SPEED_LIMIT            spd
       ON a.speed_limit                 = TRY_CAST(spd.code  AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_ROAD_TYPE              rtype
       ON a.road_type                   = TRY_CAST(rtype.code AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_URBAN_RURAL            urb
       ON a.urban_or_rural_area         = TRY_CAST(urb.code  AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_LIGHT_CONDITIONS       lgt
       ON a.light_conditions            = TRY_CAST(lgt.code  AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_WEATHER                wth
       ON a.weather_conditions          = TRY_CAST(wth.code  AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_ROAD_SURFACE           rsurf
       ON a.road_surface_conditions     = TRY_CAST(rsurf.code AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_JUNCTION_DETAIL        jdet
       ON a.junction_detail             = TRY_CAST(jdet.code  AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_JUNCTION_CONTROL       jctl
       ON a.junction_control            = TRY_CAST(jctl.code  AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_SPECIAL_CONDITIONS_AT_SITE scs
       ON a.special_conditions_at_site  = TRY_CAST(scs.code  AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_CARRIAGEWAY_HAZARDS    chaz
       ON a.carriageway_hazards         = TRY_CAST(chaz.code  AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_POLICE_FORCE           pol
       ON a.police_force                = TRY_CAST(pol.code   AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_POLICE_OFFICER_ATTEND  att
       ON a.did_police_officer_attend   = TRY_CAST(att.code   AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_DAY_OF_WEEK            dow
       ON a.day_of_week                 = TRY_CAST(dow.code   AS INT)
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_LOCAL_AUTHORITY_DISTRICT dist
       ON a.local_authority_district    = dist.code
LEFT JOIN SPARQ_CHALLENGE.SILVER.DIM_LOCAL_AUTHORITY_HIGHWAY hway
       ON a.local_authority_highway     = hway.code
 
-- Aggregated child table joins
LEFT JOIN casualty_agg ca ON a.accident_index = ca.accident_index
LEFT JOIN vehicle_agg  va ON a.accident_index = va.accident_index;
 
 
-- =============================================================================
-- Validation — run after all steps
-- =============================================================================
SELECT
    'YOY_TRENDS'              AS table_name, COUNT(*) AS total_rows FROM SPARQ_CHALLENGE.GOLD.YOY_TRENDS
UNION ALL SELECT
    'SEVERITY_BY_CONTEXT',    COUNT(*) FROM SPARQ_CHALLENGE.GOLD.SEVERITY_BY_CONTEXT
UNION ALL SELECT
    'CASUALTY_PROFILE',       COUNT(*) FROM SPARQ_CHALLENGE.GOLD.CASUALTY_PROFILE
UNION ALL SELECT
    'HOTSPOTS',               COUNT(*) FROM SPARQ_CHALLENGE.GOLD.HOTSPOTS
UNION ALL SELECT
    'VEHICLE_PROFILE',        COUNT(*) FROM SPARQ_CHALLENGE.GOLD.VEHICLE_PROFILE
UNION ALL SELECT
    'BLOOD_ALCOHOL_INSIGHTS', COUNT(*) FROM SPARQ_CHALLENGE.GOLD.BLOOD_ALCOHOL_INSIGHTS
UNION ALL SELECT
    'MART_ACCIDENTS',         COUNT(*) FROM SPARQ_CHALLENGE.GOLD.MART_ACCIDENTS
ORDER BY table_name;