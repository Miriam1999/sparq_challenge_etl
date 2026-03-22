-- =============================================================================
-- Silver Layer — UK Road Safety Data
-- Database: SPARQ_CHALLENGE
-- Schema:   SILVER
-- 
-- Execution order:
--   1. silver_accidents_conformed.sql  (this file, Step 1)
--   2. silver_accidents_2019.sql       (this file, Step 2)
--   3. silver_f_accidents.sql          (this file, Step 3 — union of 1 + 2)
--   4. silver_casualties.sql           (this file, Step 4)
--   5. silver_vehicles.sql             (this file, Step 5)
--   6. silver_blood_alcohol.sql        (this file, Step 6)
--   7. silver_quality_metrics.sql      (this file, Step 7 — audit table)
-- =============================================================================
 
-- =============================================================================
-- Step 0 — Normalize Bronze column names to UPPERCASE
-- Run this ONCE before Steps 1–6.
-- INFER_SCHEMA preserves original CSV casing; this step standardizes to
-- uppercase so all subsequent SQL can reference columns without quoting.
-- =============================================================================

CREATE OR REPLACE VIEW SPARQ_CHALLENGE.BRONZE.V_ACCIDENTS AS
SELECT
    "Accident_Index"                              AS ACCIDENT_INDEX,
    "Location_Easting_OSGR"                       AS LOCATION_EASTING_OSGR,
    "Location_Northing_OSGR"                      AS LOCATION_NORTHING_OSGR,
    "Longitude"                                   AS LONGITUDE,
    "Latitude"                                    AS LATITUDE,
    "Police_Force"                                AS POLICE_FORCE,
    "Accident_Severity"                           AS ACCIDENT_SEVERITY,
    "Number_of_Vehicles"                          AS NUMBER_OF_VEHICLES,
    "Number_of_Casualties"                        AS NUMBER_OF_CASUALTIES,
    "Date"                                        AS DATE,
    "Day_of_Week"                                 AS DAY_OF_WEEK,
    "Time"                                        AS TIME,
    "Local_Authority_(District)"                  AS LOCAL_AUTHORITY__DISTRICT_,
    "Local_Authority_(Highway)"                   AS LOCAL_AUTHORITY__HIGHWAY_,
    "1st_Road_Class"                              AS FIRST_ROAD_CLASS,
    "1st_Road_Number"                             AS FIRST_ROAD_NUMBER,
    "Road_Type"                                   AS ROAD_TYPE,
    "Speed_limit"                                 AS SPEED_LIMIT,
    "Junction_Detail"                             AS JUNCTION_DETAIL,
    "Junction_Control"                            AS JUNCTION_CONTROL,
    "2nd_Road_Class"                              AS SECOND_ROAD_CLASS,
    "2nd_Road_Number"                             AS SECOND_ROAD_NUMBER,
    "Pedestrian_Crossing-Human_Control"           AS PEDESTRIAN_CROSSING_HUMAN_CONTROL,
    "Pedestrian_Crossing-Physical_Facilities"     AS PEDESTRIAN_CROSSING_PHYSICAL_FACILITIES,
    "Light_Conditions"                            AS LIGHT_CONDITIONS,
    "Weather_Conditions"                          AS WEATHER_CONDITIONS,
    "Road_Surface_Conditions"                     AS ROAD_SURFACE_CONDITIONS,
    "Special_Conditions_at_Site"                  AS SPECIAL_CONDITIONS_AT_SITE,
    "Carriageway_Hazards"                         AS CARRIAGEWAY_HAZARDS,
    "Urban_or_Rural_Area"                         AS URBAN_OR_RURAL_AREA,
    "Did_Police_Officer_Attend_Scene_of_Accident" AS DID_POLICE_OFFICER_ATTEND_SCENE_OF_ACCIDENT,
    "LSOA_of_Accident_Location"                   AS LSOA_OF_ACCIDENT_LOCATION
FROM SPARQ_CHALLENGE.BRONZE.ACCIDENTS;


CREATE OR REPLACE VIEW SPARQ_CHALLENGE.BRONZE.V_ACCIDENTS_2019 AS
SELECT
    "Accident_Index"                              AS ACCIDENT_INDEX,
    "Status"                                      AS STATUS,
    "Location_Easting_OSGR"                       AS LOCATION_EASTING_OSGR,
    "Location_Northing_OSGR"                      AS LOCATION_NORTHING_OSGR,
    "Police_Force"                                AS POLICE_FORCE,
    "Accident_Severity"                           AS ACCIDENT_SEVERITY,
    "Number_of_Vehicles"                          AS NUMBER_OF_VEHICLES,
    "Number_of_Casualties"                        AS NUMBER_OF_CASUALTIES,
    "Date"                                        AS DATE,
    "Time"                                        AS TIME,
    "st_Road_Class"                               AS ST_ROAD_CLASS,
    "st_Road_Number"                              AS ST_ROAD_NUMBER,
    "Road_Type"                                   AS ROAD_TYPE,
    "Speed_limit"                                 AS SPEED_LIMIT,
    "Junction_Detail"                             AS JUNCTION_DETAIL,
    "Junction_Control"                            AS JUNCTION_CONTROL,
    "nd_Road_Class"                               AS ND_ROAD_CLASS,
    "nd_Road_Number"                              AS ND_ROAD_NUMBER,
    "Pedestrian_Crossing-Human_Control"           AS PEDESTRIAN_CROSSING_HUMAN_CONTROL,
    "Pedestrian_Crossing-Physical_Facilities"     AS PEDESTRIAN_CROSSING_PHYSICAL_FACILITIES,
    "Light_Conditions"                            AS LIGHT_CONDITIONS,
    "Weather_Conditions"                          AS WEATHER_CONDITIONS,
    "Road_Surface_Conditions"                     AS ROAD_SURFACE_CONDITIONS,
    "Special_Conditions_at_Site"                  AS SPECIAL_CONDITIONS_AT_SITE,
    "Carriageway_Hazards"                         AS CARRIAGEWAY_HAZARDS
FROM SPARQ_CHALLENGE.BRONZE.ACCIDENTS_2019_MIDYEAR_PROVISIONAL;


CREATE OR REPLACE VIEW SPARQ_CHALLENGE.BRONZE.V_CASUALTIES AS
SELECT
    "Accident_Index"                              AS ACCIDENT_INDEX,
    "Vehicle_Reference"                           AS VEHICLE_REFERENCE,
    "Casualty_Reference"                          AS CASUALTY_REFERENCE,
    "Casualty_Class"                              AS CASUALTY_CLASS,
    "Sex_of_Casualty"                             AS SEX_OF_CASUALTY,
    "Age_of_Casualty"                             AS AGE_OF_CASUALTY,
    "Age_Band_of_Casualty"                        AS AGE_BAND_OF_CASUALTY,
    "Casualty_Severity"                           AS CASUALTY_SEVERITY,
    "Pedestrian_Location"                         AS PEDESTRIAN_LOCATION,
    "Pedestrian_Movement"                         AS PEDESTRIAN_MOVEMENT,
    "Car_Passenger"                               AS CAR_PASSENGER,
    "Bus_or_Coach_Passenger"                      AS BUS_OR_COACH_PASSENGER,
    "Pedestrian_Road_Maintenance_Worker"          AS PEDESTRIAN_ROAD_MAINTENANCE_WORKER,
    "Casualty_Type"                               AS CASUALTY_TYPE,
    "Casualty_Home_Area_Type"                     AS CASUALTY_HOME_AREA_TYPE,
    "Casualty_IMD_Decile"                         AS CASUALTY_IMD_DECILE
FROM SPARQ_CHALLENGE.BRONZE.CASUALTIES;


CREATE OR REPLACE VIEW SPARQ_CHALLENGE.BRONZE.V_VEHICLES AS
SELECT
    "Accident_Index"                              AS ACCIDENT_INDEX,
    "Vehicle_Reference"                           AS VEHICLE_REFERENCE,
    "Vehicle_Type"                                AS VEHICLE_TYPE,
    "Towing_and_Articulation"                     AS TOWING_AND_ARTICULATION,
    "Vehicle_Manoeuvre"                           AS VEHICLE_MANOEUVRE,
    "Vehicle_Location-Restricted_Lane"            AS VEHICLE_LOCATION_RESTRICTED_LANE,
    "Junction_Location"                           AS JUNCTION_LOCATION,
    "Skidding_and_Overturning"                    AS SKIDDING_AND_OVERTURNING,
    "Hit_Object_in_Carriageway"                   AS HIT_OBJECT_IN_CARRIAGEWAY,
    "Vehicle_Leaving_Carriageway"                 AS VEHICLE_LEAVING_CARRIAGEWAY,
    "Hit_Object_off_Carriageway"                  AS HIT_OBJECT_OFF_CARRIAGEWAY,
    "1st_Point_of_Impact"                         AS FIRST_POINT_OF_IMPACT,
    "Was_Vehicle_Left_Hand_Drive?"                AS WAS_VEHICLE_LEFT_HAND_DRIVE_,
    "Journey_Purpose_of_Driver"                   AS JOURNEY_PURPOSE_OF_DRIVER,
    "Sex_of_Driver"                               AS SEX_OF_DRIVER,
    "Age_of_Driver"                               AS AGE_OF_DRIVER,
    "Age_Band_of_Driver"                          AS AGE_BAND_OF_DRIVER,
    "Engine_Capacity_(CC)"                        AS ENGINE_CAPACITY__CC_,
    "Propulsion_Code"                             AS PROPULSION_CODE,
    "Age_of_Vehicle"                              AS AGE_OF_VEHICLE,
    "Driver_IMD_Decile"                           AS DRIVER_IMD_DECILE,
    "Driver_Home_Area_Type"                       AS DRIVER_HOME_AREA_TYPE,
    "Vehicle_IMD_Decile"                          AS VEHICLE_IMD_DECILE
FROM SPARQ_CHALLENGE.BRONZE.VEHICLES;


CREATE OR REPLACE VIEW SPARQ_CHALLENGE.BRONZE.V_BLOOD_ALCOHOL AS
SELECT
    "BLOODALCOHOLLEVEL_MG_100ML"                  AS BLOODALCOHOLLEVEL_MG_100ML,
    "SEVERITYOFINJURY"                            AS SEVERITYOFINJURY,
    "CASUALTYCLASS"                               AS CASUALTYCLASS,
    "SEXOFCASUALTY"                               AS SEXOFCASUALTY,
    "AGEBAND"                                     AS AGEBAND
FROM SPARQ_CHALLENGE.BRONZE.BLOOD_ALCOHOL_CONTENT;

-- =============================================================================
-- Step 1 — SILVER.F_ACCIDENTS_BASE (2015–2018 conformed)
-- =============================================================================
CREATE OR REPLACE TABLE SPARQ_CHALLENGE.SILVER.F_ACCIDENTS_BASE AS
 
WITH source AS (
    SELECT * FROM SPARQ_CHALLENGE.BRONZE.V_ACCIDENTS
),
 
typed AS (
    SELECT
        -- ---------------------------------------------------------------
        -- Keys
        -- ---------------------------------------------------------------
        ACCIDENT_INDEX,
 
        -- ---------------------------------------------------------------
        -- Date / time
        -- Dates in STATS19 are DD/MM/YYYY
        -- ---------------------------------------------------------------
        TRY_TO_DATE(DATE, 'DD/MM/YYYY')                         AS accident_date,
        YEAR(TRY_TO_DATE(DATE, 'DD/MM/YYYY'))                   AS accident_year,
        MONTH(TRY_TO_DATE(DATE, 'DD/MM/YYYY'))                  AS accident_month,
        DAY(TRY_TO_DATE(DATE, 'DD/MM/YYYY'))                    AS accident_day,
        TRY_TO_TIME(TIME)                                        AS accident_time,
        HOUR(TRY_TO_TIME(TIME))                                  AS accident_hour,
        TRY_CAST(DAY_OF_WEEK AS INT)                             AS day_of_week,
 
        -- ---------------------------------------------------------------
        -- Geography
        -- ---------------------------------------------------------------
        TRY_CAST(LONGITUDE                  AS FLOAT)            AS longitude,
        TRY_CAST(LATITUDE                   AS FLOAT)            AS latitude,
        TRY_CAST(LOCATION_EASTING_OSGR      AS INT)              AS location_easting_osgr,
        TRY_CAST(LOCATION_NORTHING_OSGR     AS INT)              AS location_northing_osgr,
        "LOCAL_AUTHORITY__DISTRICT_"                             AS local_authority_district,
        "LOCAL_AUTHORITY__HIGHWAY_"                              AS local_authority_highway,
        LSOA_OF_ACCIDENT_LOCATION                                AS lsoa_of_accident_location,
 
        -- ---------------------------------------------------------------
        -- Accident attributes
        -- ---------------------------------------------------------------
        TRY_CAST(POLICE_FORCE               AS INT)              AS police_force,
        TRY_CAST(ACCIDENT_SEVERITY          AS INT)              AS accident_severity,
        TRY_CAST(NUMBER_OF_VEHICLES         AS INT)              AS number_of_vehicles,
        TRY_CAST(NUMBER_OF_CASUALTIES       AS INT)              AS number_of_casualties,
 
        -- SPEED_LIMIT fix: Bronze stored as '30.0', dim has '30'
        -- Cast float -> int to align with DIM_SPEED_LIMIT codes
        TRY_CAST(SPEED_LIMIT AS FLOAT)::INT         AS speed_limit,
 
        TRY_CAST(ROAD_TYPE                  AS INT)              AS road_type,
        TRY_CAST(FIRST_ROAD_CLASS           AS INT)              AS first_road_class,
        TRY_CAST(FIRST_ROAD_NUMBER          AS INT)              AS first_road_number,
        TRY_CAST(SECOND_ROAD_CLASS           AS INT)              AS second_road_class,
        TRY_CAST(SECOND_ROAD_NUMBER          AS INT)              AS second_road_number,
        TRY_CAST(JUNCTION_DETAIL            AS INT)              AS junction_detail,
        TRY_CAST(JUNCTION_CONTROL           AS INT)              AS junction_control,
        TRY_CAST(PEDESTRIAN_CROSSING_HUMAN_CONTROL    AS INT)    AS pedestrian_crossing_human_control,
        TRY_CAST(PEDESTRIAN_CROSSING_PHYSICAL_FACILITIES AS INT) AS pedestrian_crossing_physical_facilities,
        TRY_CAST(LIGHT_CONDITIONS           AS INT)              AS light_conditions,
        TRY_CAST(WEATHER_CONDITIONS         AS INT)              AS weather_conditions,
        TRY_CAST(ROAD_SURFACE_CONDITIONS    AS INT)              AS road_surface_conditions,
        TRY_CAST(SPECIAL_CONDITIONS_AT_SITE AS INT)              AS special_conditions_at_site,
        TRY_CAST(CARRIAGEWAY_HAZARDS        AS INT)              AS carriageway_hazards,
        TRY_CAST(URBAN_OR_RURAL_AREA        AS INT)              AS urban_or_rural_area,
        TRY_CAST(DID_POLICE_OFFICER_ATTEND_SCENE_OF_ACCIDENT AS INT)
                                                                 AS did_police_officer_attend,
 
        -- ---------------------------------------------------------------
        -- Quality flags
        -- TRUE = value is present and usable
        -- ---------------------------------------------------------------
        (LONGITUDE IS NOT NULL AND LATITUDE IS NOT NULL)         AS has_geo,
        (LSOA_OF_ACCIDENT_LOCATION IS NOT NULL
            AND LSOA_OF_ACCIDENT_LOCATION != '')                 AS has_lsoa,
        (TIME IS NOT NULL AND TIME != '')                        AS has_time,
        (JUNCTION_CONTROL IS NOT NULL
            AND JUNCTION_CONTROL NOT IN ('-1', ''))              AS has_junction_control,
        (TRY_TO_DATE(DATE, 'DD/MM/YYYY') IS NOT NULL)            AS is_date_valid,
 
        -- ---------------------------------------------------------------
        -- Provenance
        -- ---------------------------------------------------------------
        FALSE                                                    AS is_provisional,
        CURRENT_TIMESTAMP()                                      AS _loaded_at
 
    FROM source
)
 
SELECT * FROM typed;
 
 
-- =============================================================================
-- Step 2 — SILVER.F_ACCIDENTS_2019 (2019 provisional conformed)
-- Renames truncated columns (ST_ / ND_ → first_ / second_)
-- Fills missing columns with NULL to match Step 1 schema
-- =============================================================================
CREATE OR REPLACE TABLE SPARQ_CHALLENGE.SILVER.F_ACCIDENTS_2019 AS
 
WITH source AS (
    SELECT * FROM SPARQ_CHALLENGE.BRONZE.V_ACCIDENTS_2019
),
 
typed AS (
    SELECT
        -- ---------------------------------------------------------------
        -- Keys
        -- ---------------------------------------------------------------
        ACCIDENT_INDEX,
 
        -- ---------------------------------------------------------------
        -- Date / time (2019 table has no DAY_OF_WEEK column)
        -- ---------------------------------------------------------------
        TRY_TO_DATE(DATE, 'DD/MM/YYYY')                         AS accident_date,
        YEAR(TRY_TO_DATE(DATE, 'DD/MM/YYYY'))                   AS accident_year,
        MONTH(TRY_TO_DATE(DATE, 'DD/MM/YYYY'))                  AS accident_month,
        DAY(TRY_TO_DATE(DATE, 'DD/MM/YYYY'))                    AS accident_day,
        TRY_TO_TIME(TIME)                                        AS accident_time,
        HOUR(TRY_TO_TIME(TIME))                                  AS accident_hour,
        -- DAY_OF_WEEK not available in 2019 provisional table
        NULL::INT                                                AS day_of_week,
 
        -- ---------------------------------------------------------------
        -- Geography (lat/lon not available in 2019 provisional)
        -- ---------------------------------------------------------------
        NULL::FLOAT                                              AS longitude,
        NULL::FLOAT                                              AS latitude,
        NULL::INT                                                AS location_easting_osgr,
        NULL::INT                                                AS location_northing_osgr,
        NULL::VARCHAR                                            AS local_authority_district,
        NULL::VARCHAR                                            AS local_authority_highway,
        NULL::VARCHAR                                            AS lsoa_of_accident_location,
 
        -- ---------------------------------------------------------------
        -- Accident attributes
        -- ST_ROAD_CLASS / ND_ROAD_CLASS are truncated versions of
        -- 1ST_ROAD_CLASS / 2ND_ROAD_CLASS — renamed here for alignment
        -- ---------------------------------------------------------------
        TRY_CAST(POLICE_FORCE               AS INT)              AS police_force,
        TRY_CAST(ACCIDENT_SEVERITY          AS INT)              AS accident_severity,
        TRY_CAST(NUMBER_OF_VEHICLES         AS INT)              AS number_of_vehicles,
        TRY_CAST(NUMBER_OF_CASUALTIES       AS INT)              AS number_of_casualties,
        TRY_CAST(SPEED_LIMIT AS FLOAT)::INT         AS speed_limit,
        TRY_CAST(ROAD_TYPE                  AS INT)              AS road_type,
 
        -- Renamed from ST_ROAD_CLASS / ST_ROAD_NUMBER
        TRY_CAST(ST_ROAD_CLASS              AS INT)              AS first_road_class,
        TRY_CAST(ST_ROAD_NUMBER             AS INT)              AS first_road_number,
 
        -- Renamed from ND_ROAD_CLASS / ND_ROAD_NUMBER
        TRY_CAST(ND_ROAD_CLASS              AS INT)              AS second_road_class,
        TRY_CAST(ND_ROAD_NUMBER             AS INT)              AS second_road_number,
 
        TRY_CAST(JUNCTION_DETAIL            AS INT)              AS junction_detail,
        TRY_CAST(JUNCTION_CONTROL           AS INT)              AS junction_control,
        TRY_CAST(PEDESTRIAN_CROSSING_HUMAN_CONTROL    AS INT)    AS pedestrian_crossing_human_control,
        TRY_CAST(PEDESTRIAN_CROSSING_PHYSICAL_FACILITIES AS INT) AS pedestrian_crossing_physical_facilities,
        TRY_CAST(LIGHT_CONDITIONS           AS INT)              AS light_conditions,
        TRY_CAST(WEATHER_CONDITIONS         AS INT)              AS weather_conditions,
        TRY_CAST(ROAD_SURFACE_CONDITIONS    AS INT)              AS road_surface_conditions,
        TRY_CAST(SPECIAL_CONDITIONS_AT_SITE AS INT)              AS special_conditions_at_site,
        TRY_CAST(CARRIAGEWAY_HAZARDS        AS INT)              AS carriageway_hazards,
 
        -- Not available in 2019 provisional
        NULL::INT                                                AS urban_or_rural_area,
        NULL::INT                                                AS did_police_officer_attend,
 
        -- ---------------------------------------------------------------
        -- Quality flags
        -- ---------------------------------------------------------------
        FALSE                                                    AS has_geo,
        FALSE                                                    AS has_lsoa,
        (TIME IS NOT NULL AND TIME != '')                        AS has_time,
        (JUNCTION_CONTROL IS NOT NULL
            AND JUNCTION_CONTROL NOT IN ('-1', ''))              AS has_junction_control,
        (TRY_TO_DATE(DATE, 'DD/MM/YYYY') IS NOT NULL)            AS is_date_valid,
 
        -- ---------------------------------------------------------------
        -- Provenance
        -- ---------------------------------------------------------------
        TRUE                                                     AS is_provisional,
        CURRENT_TIMESTAMP()                                      AS _loaded_at
 
    FROM source
)
 
SELECT * FROM typed;
 
 
-- =============================================================================
-- Step 3 — SILVER.F_ACCIDENTS (full union 2015–2019)
-- Single source of truth for all accident analysis.
-- Use IS_PROVISIONAL flag to exclude 2019 from year-comparable KPIs.
-- =============================================================================
CREATE OR REPLACE TABLE SPARQ_CHALLENGE.SILVER.F_ACCIDENTS AS
 
    SELECT * FROM SPARQ_CHALLENGE.SILVER.F_ACCIDENTS_BASE
 
    UNION ALL
 
    SELECT * FROM SPARQ_CHALLENGE.SILVER.F_ACCIDENTS_2019;
 
 
-- =============================================================================
-- Step 4 — SILVER.F_CASUALTIES
-- =============================================================================
CREATE OR REPLACE TABLE SPARQ_CHALLENGE.SILVER.F_CASUALTIES AS
 
WITH source AS (
    SELECT * FROM SPARQ_CHALLENGE.BRONZE.V_CASUALTIES
),
 
typed AS (
    SELECT
        -- ---------------------------------------------------------------
        -- Keys
        -- ---------------------------------------------------------------
        ACCIDENT_INDEX,
        TRY_CAST(VEHICLE_REFERENCE      AS INT)                  AS vehicle_reference,
        TRY_CAST(CASUALTY_REFERENCE     AS INT)                  AS casualty_reference,
 
        -- ---------------------------------------------------------------
        -- Casualty attributes
        -- ---------------------------------------------------------------
        TRY_CAST(CASUALTY_CLASS         AS INT)                  AS casualty_class,
        TRY_CAST(SEX_OF_CASUALTY        AS INT)                  AS sex_of_casualty,
        TRY_CAST(AGE_OF_CASUALTY        AS INT)                  AS age_of_casualty,
        TRY_CAST(AGE_BAND_OF_CASUALTY   AS INT)                  AS age_band_of_casualty,
        TRY_CAST(CASUALTY_SEVERITY      AS INT)                  AS casualty_severity,
        TRY_CAST(PEDESTRIAN_LOCATION    AS INT)                  AS pedestrian_location,
        TRY_CAST(PEDESTRIAN_MOVEMENT    AS INT)                  AS pedestrian_movement,
        TRY_CAST(CAR_PASSENGER          AS INT)                  AS car_passenger,
        TRY_CAST(BUS_OR_COACH_PASSENGER AS INT)                  AS bus_or_coach_passenger,
        TRY_CAST(PEDESTRIAN_ROAD_MAINTENANCE_WORKER AS INT)      AS pedestrian_road_maintenance_worker,
        TRY_CAST(CASUALTY_TYPE          AS INT)                  AS casualty_type,
        TRY_CAST(CASUALTY_HOME_AREA_TYPE AS INT)                 AS casualty_home_area_type,
        TRY_CAST(CASUALTY_IMD_DECILE    AS INT)                  AS casualty_imd_decile,
 
        -- ---------------------------------------------------------------
        -- Quality flags
        -- IMD and home area type have high null rates (21% and 13%)
        -- flagged explicitly so downstream queries can filter reliably
        -- ---------------------------------------------------------------
        (AGE_OF_CASUALTY IS NOT NULL
            AND AGE_OF_CASUALTY NOT IN ('-1', ''))               AS has_age,
        (CASUALTY_IMD_DECILE IS NOT NULL
            AND CASUALTY_IMD_DECILE NOT IN ('-1', ''))           AS has_imd_decile,
        (CASUALTY_HOME_AREA_TYPE IS NOT NULL
            AND CASUALTY_HOME_AREA_TYPE NOT IN ('-1', ''))       AS has_home_area_type,
 
        -- ---------------------------------------------------------------
        -- Provenance
        -- ---------------------------------------------------------------
        CURRENT_TIMESTAMP()                                      AS _loaded_at
 
    FROM source
)
 
SELECT * FROM typed;
 
 
-- =============================================================================
-- Step 5 — SILVER.F_VEHICLES
-- =============================================================================
CREATE OR REPLACE TABLE SPARQ_CHALLENGE.SILVER.F_VEHICLES AS
 
WITH source AS (
    SELECT * FROM SPARQ_CHALLENGE.BRONZE.V_VEHICLES
),
 
typed AS (
    SELECT
        -- ---------------------------------------------------------------
        -- Keys
        -- ---------------------------------------------------------------
        ACCIDENT_INDEX,
        TRY_CAST(VEHICLE_REFERENCE          AS INT)              AS vehicle_reference,
 
        -- ---------------------------------------------------------------
        -- Vehicle attributes
        -- ---------------------------------------------------------------
        TRY_CAST(VEHICLE_TYPE               AS INT)              AS vehicle_type,
        TRY_CAST(TOWING_AND_ARTICULATION    AS INT)              AS towing_and_articulation,
        TRY_CAST(VEHICLE_MANOEUVRE          AS INT)              AS vehicle_manoeuvre,
        TRY_CAST(VEHICLE_LOCATION_RESTRICTED_LANE AS INT)        AS vehicle_location_restricted_lane,
        TRY_CAST(JUNCTION_LOCATION          AS INT)              AS junction_location,
        TRY_CAST(SKIDDING_AND_OVERTURNING   AS INT)              AS skidding_and_overturning,
        TRY_CAST(HIT_OBJECT_IN_CARRIAGEWAY  AS INT)              AS hit_object_in_carriageway,
        TRY_CAST(VEHICLE_LEAVING_CARRIAGEWAY AS INT)             AS vehicle_leaving_carriageway,
        TRY_CAST(HIT_OBJECT_OFF_CARRIAGEWAY AS INT)              AS hit_object_off_carriageway,
        TRY_CAST(FIRST_POINT_OF_IMPACT      AS INT)              AS first_point_of_impact,
        TRY_CAST(WAS_VEHICLE_LEFT_HAND_DRIVE_ AS INT)            AS was_vehicle_left_hand_drive,
        TRY_CAST(JOURNEY_PURPOSE_OF_DRIVER  AS INT)              AS journey_purpose_of_driver,
        TRY_CAST(SEX_OF_DRIVER              AS INT)              AS sex_of_driver,
        TRY_CAST(AGE_OF_DRIVER              AS INT)              AS age_of_driver,
        TRY_CAST(AGE_BAND_OF_DRIVER         AS INT)              AS age_band_of_driver,
        TRY_CAST("ENGINE_CAPACITY__CC_"     AS INT)              AS engine_capacity_cc,
        TRY_CAST(PROPULSION_CODE            AS INT)              AS propulsion_code,
        TRY_CAST(AGE_OF_VEHICLE             AS INT)              AS age_of_vehicle,
        TRY_CAST(DRIVER_IMD_DECILE          AS INT)              AS driver_imd_decile,
        TRY_CAST(DRIVER_HOME_AREA_TYPE      AS INT)              AS driver_home_area_type,
        TRY_CAST(VEHICLE_IMD_DECILE         AS INT)              AS vehicle_imd_decile,
 
        -- ---------------------------------------------------------------
        -- Quality flags
        -- High null rates: IMD (48%), engine capacity (24%),
        -- age of vehicle (28%), propulsion code (24%)
        -- ---------------------------------------------------------------
        (AGE_OF_DRIVER IS NOT NULL
            AND AGE_OF_DRIVER NOT IN ('-1', ''))                 AS has_driver_age,
        (AGE_OF_VEHICLE IS NOT NULL
            AND AGE_OF_VEHICLE NOT IN ('-1', ''))                AS has_vehicle_age,
        (ENGINE_CAPACITY__CC_ IS NOT NULL
            AND ENGINE_CAPACITY__CC_ NOT IN ('-1', ''))          AS has_engine_capacity,
        (PROPULSION_CODE IS NOT NULL
            AND PROPULSION_CODE NOT IN ('-1', ''))               AS has_propulsion_code,
        (DRIVER_IMD_DECILE IS NOT NULL
            AND DRIVER_IMD_DECILE NOT IN ('-1', ''))             AS has_driver_imd,
        (VEHICLE_IMD_DECILE IS NOT NULL
            AND VEHICLE_IMD_DECILE NOT IN ('-1', ''))            AS has_vehicle_imd,
 
        -- ---------------------------------------------------------------
        -- Provenance
        -- ---------------------------------------------------------------
        CURRENT_TIMESTAMP()                                      AS _loaded_at
 
    FROM source
)
 
SELECT * FROM typed;
 
 
-- =============================================================================
-- Step 6 — SILVER.F_BLOOD_ALCOHOL
-- 77.3% of rows are duplicates — deduplicate with SELECT DISTINCT.
-- No natural PK; this is a statistical aggregate table.
-- =============================================================================
CREATE OR REPLACE TABLE SPARQ_CHALLENGE.SILVER.F_BLOOD_ALCOHOL AS
 
SELECT DISTINCT
    TRY_CAST(BLOODALCOHOLLEVEL_MG_100ML AS INT)                  AS blood_alcohol_level_mg,
    SEVERITYOFINJURY                                             AS severity_of_injury,
    TRY_CAST(CASUALTYCLASS              AS INT)                  AS casualty_class,
    TRY_CAST(SEXOFCASUALTY              AS INT)                  AS sex_of_casualty,
    AGEBAND                                                      AS age_band,
    CURRENT_TIMESTAMP()                                          AS _loaded_at
 
FROM SPARQ_CHALLENGE.BRONZE.V_BLOOD_ALCOHOL;
 
 
-- =============================================================================
-- Step 7 — SILVER.QUALITY_METRICS (revised)
-- Audit table: row counts, null rates, and integrity checks per run.
-- Covers F_ACCIDENTS, F_CASUALTIES, F_VEHICLES, and F_BLOOD_ALCOHOL.
-- Run this after Steps 1–6 to validate the Silver load.
-- =============================================================================
CREATE OR REPLACE TABLE SPARQ_CHALLENGE.SILVER.QUALITY_METRICS AS
 
-- -----------------------------------------------------------------------------
-- F_ACCIDENTS
-- Checks: row count vs expected union total, date validity, geo, lsoa, time
-- Expected total: 529,294 (base) + 46,747 (provisional) = 576,041
-- -----------------------------------------------------------------------------
WITH accidents_qc AS (
    SELECT
        'F_ACCIDENTS'                                               AS table_name,
        COUNT(*)                                                    AS total_rows,
        COUNT_IF(is_provisional = FALSE)                            AS rows_base,
        COUNT_IF(is_provisional = TRUE)                             AS rows_provisional,
        576041                                                      AS expected_total_rows,
        COUNT(*) - 576041                                           AS row_count_diff,
        COUNT_IF(NOT is_date_valid)                                 AS invalid_dates,
        ROUND(COUNT_IF(NOT is_date_valid) / COUNT(*) * 100, 2)      AS pct_invalid_dates,
        COUNT_IF(NOT has_geo)                                       AS missing_geo,
        ROUND(COUNT_IF(NOT has_geo)       / COUNT(*) * 100, 2)      AS pct_missing_geo,
        COUNT_IF(NOT has_lsoa)                                      AS missing_lsoa,
        ROUND(COUNT_IF(NOT has_lsoa)      / COUNT(*) * 100, 2)      AS pct_missing_lsoa,
        COUNT_IF(NOT has_time)                                      AS missing_time,
        ROUND(COUNT_IF(NOT has_time)      / COUNT(*) * 100, 2)      AS pct_missing_time,
        COUNT_IF(NOT has_junction_control)                          AS missing_junction_control,
        ROUND(COUNT_IF(NOT has_junction_control) / COUNT(*) * 100, 2) AS pct_missing_junction_control
    FROM SPARQ_CHALLENGE.SILVER.F_ACCIDENTS
),
 
-- -----------------------------------------------------------------------------
-- F_CASUALTIES
-- Checks: row count, age, imd decile, home area type
-- -----------------------------------------------------------------------------
casualties_qc AS (
    SELECT
        'F_CASUALTIES'                                              AS table_name,
        COUNT(*)                                                    AS total_rows,
        NULL::INT                                                   AS rows_base,
        NULL::INT                                                   AS rows_provisional,
        699163                                                      AS expected_total_rows,
        COUNT(*) - 699163                                           AS row_count_diff,
        NULL::INT                                                   AS invalid_dates,
        NULL::FLOAT                                                 AS pct_invalid_dates,
        COUNT_IF(NOT has_age)                                       AS missing_age,
        ROUND(COUNT_IF(NOT has_age)           / COUNT(*) * 100, 2)  AS pct_missing_age,
        COUNT_IF(NOT has_imd_decile)                                AS missing_imd_decile,
        ROUND(COUNT_IF(NOT has_imd_decile)    / COUNT(*) * 100, 2)  AS pct_missing_imd_decile,
        COUNT_IF(NOT has_home_area_type)                            AS missing_home_area_type,
        ROUND(COUNT_IF(NOT has_home_area_type)/ COUNT(*) * 100, 2)  AS pct_missing_home_area_type,
        NULL::INT                                                   AS missing_junction_control,
        NULL::FLOAT                                                 AS pct_missing_junction_control
    FROM SPARQ_CHALLENGE.SILVER.F_CASUALTIES
),
 
-- -----------------------------------------------------------------------------
-- F_VEHICLES
-- Checks: row count, driver age, vehicle age, engine capacity, propulsion,
--         driver imd, vehicle imd
-- -----------------------------------------------------------------------------
vehicles_qc AS (
    SELECT
        'F_VEHICLES'                                                AS table_name,
        COUNT(*)                                                    AS total_rows,
        NULL::INT                                                   AS rows_base,
        NULL::INT                                                   AS rows_provisional,
        975680                                                      AS expected_total_rows,
        COUNT(*) - 975680                                           AS row_count_diff,
        NULL::INT                                                   AS invalid_dates,
        NULL::FLOAT                                                 AS pct_invalid_dates,
        COUNT_IF(NOT has_driver_age)                                AS missing_driver_age,
        ROUND(COUNT_IF(NOT has_driver_age)    / COUNT(*) * 100, 2)  AS pct_missing_driver_age,
        COUNT_IF(NOT has_vehicle_age)                               AS missing_vehicle_age,
        ROUND(COUNT_IF(NOT has_vehicle_age)   / COUNT(*) * 100, 2)  AS pct_missing_vehicle_age,
        COUNT_IF(NOT has_engine_capacity)                           AS missing_engine_capacity,
        ROUND(COUNT_IF(NOT has_engine_capacity)/ COUNT(*) * 100, 2) AS pct_missing_engine_capacity,
        COUNT_IF(NOT has_driver_imd)                                AS missing_driver_imd,
        ROUND(COUNT_IF(NOT has_driver_imd)    / COUNT(*) * 100, 2)  AS pct_missing_driver_imd
    FROM SPARQ_CHALLENGE.SILVER.F_VEHICLES
),
 
-- -----------------------------------------------------------------------------
-- F_BLOOD_ALCOHOL
-- Checks: row count after deduplication
-- Bronze had 2,141 rows with 77.3% duplicates → expected ~486 unique rows
-- Flag if Silver count is unexpectedly high (dedup may have failed)
-- -----------------------------------------------------------------------------
blood_alcohol_qc AS (
    SELECT
        'F_BLOOD_ALCOHOL'                                           AS table_name,
        COUNT(*)                                                    AS total_rows,
        NULL::INT                                                   AS rows_base,
        NULL::INT                                                   AS rows_provisional,
        -- Expected unique rows after dedup; flag if total_rows >> 486
        384                                                         AS expected_total_rows,
        COUNT(*) - 384                                              AS row_count_diff,
        NULL::INT                                                   AS invalid_dates,
        NULL::FLOAT                                                 AS pct_invalid_dates,
        COUNT_IF(blood_alcohol_level_mg IS NULL)                    AS missing_blood_alcohol_level,
        ROUND(COUNT_IF(blood_alcohol_level_mg IS NULL) / COUNT(*) * 100, 2) AS pct_missing_blood_alcohol_level,
        NULL::INT                                                   AS missing_imd_decile,
        NULL::FLOAT                                                 AS pct_missing_imd_decile,
        NULL::INT                                                   AS missing_home_area_type,
        NULL::FLOAT                                                 AS pct_missing_home_area_type,
        NULL::INT                                                   AS missing_junction_control,
        NULL::FLOAT                                                 AS pct_missing_junction_control
    FROM SPARQ_CHALLENGE.SILVER.F_BLOOD_ALCOHOL
)
 
SELECT *, CURRENT_TIMESTAMP() AS _run_at FROM accidents_qc
UNION ALL
SELECT *, CURRENT_TIMESTAMP() AS _run_at FROM casualties_qc
UNION ALL
SELECT *, CURRENT_TIMESTAMP() AS _run_at FROM vehicles_qc
UNION ALL
SELECT *, CURRENT_TIMESTAMP() AS _run_at FROM blood_alcohol_qc;
 
 
-- -----------------------------------------------------------------------------
-- Quick validation query — run after creating QUALITY_METRICS
-- ROW_COUNT_DIFF = 0 means the load matched expected counts exactly
-- -----------------------------------------------------------------------------
SELECT
    table_name,
    total_rows,
    expected_total_rows,
    row_count_diff,
    CASE
        WHEN row_count_diff = 0 THEN 'PASS'
        ELSE 'FAIL — investigate before Gold layer'
    END AS load_status
FROM SPARQ_CHALLENGE.SILVER.QUALITY_METRICS
ORDER BY table_name;