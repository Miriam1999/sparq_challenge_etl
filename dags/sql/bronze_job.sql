
USE DATABASE SPARQ_CHALLENGE;
USE SCHEMA BRONZE;

CREATE FILE FORMAT IF NOT EXISTS FF_CSV_BRONZE
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  EMPTY_FIELD_AS_NULL = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- ---------------------------------------------------------------------------
-- ACCIDENTS (2015–2018: shared schema; excludes provisional 2019 file)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ACCIDENTS (
  "Accident_Index" VARCHAR,
  "Location_Easting_OSGR" VARCHAR,
  "Location_Northing_OSGR" VARCHAR,
  "Longitude" VARCHAR,
  "Latitude" VARCHAR,
  "Police_Force" VARCHAR,
  "Accident_Severity" VARCHAR,
  "Number_of_Vehicles" VARCHAR,
  "Number_of_Casualties" VARCHAR,
  "Date" VARCHAR,
  "Day_of_Week" VARCHAR,
  "Time" VARCHAR,
  "Local_Authority_(District)" VARCHAR,
  "Local_Authority_(Highway)" VARCHAR,
  "1st_Road_Class" VARCHAR,
  "1st_Road_Number" VARCHAR,
  "Road_Type" VARCHAR,
  "Speed_limit" VARCHAR,
  "Junction_Detail" VARCHAR,
  "Junction_Control" VARCHAR,
  "2nd_Road_Class" VARCHAR,
  "2nd_Road_Number" VARCHAR,
  "Pedestrian_Crossing-Human_Control" VARCHAR,
  "Pedestrian_Crossing-Physical_Facilities" VARCHAR,
  "Light_Conditions" VARCHAR,
  "Weather_Conditions" VARCHAR,
  "Road_Surface_Conditions" VARCHAR,
  "Special_Conditions_at_Site" VARCHAR,
  "Carriageway_Hazards" VARCHAR,
  "Urban_or_Rural_Area" VARCHAR,
  "Did_Police_Officer_Attend_Scene_of_Accident" VARCHAR,
  "LSOA_of_Accident_Location" VARCHAR
);

TRUNCATE TABLE IF EXISTS ACCIDENTS;

-- Excluye accidents2015/Accidents_2015.csv (duplicado de roadsafetydata2015) y el provisional 2019.
COPY INTO ACCIDENTS
FROM @SPARQ_CHALLENGE.BRONZE.internal_stage/
PATTERN = '^(roadsafetydata2015/Accidents_2015|roadsafetyaccidents2016/dftRoadSafety_Accidents_2016|roadsafetydataaccidents2017/Acc|roadsafetydataaccidents2018/dftRoadSafetyData_Accidents_2018)\\.csv$'
FILE_FORMAT = (FORMAT_NAME = 'FF_CSV_BRONZE')
FORCE = TRUE;

-- ---------------------------------------------------------------------------
-- ACCIDENTS 2019 mid-year provisional (different column layout)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ACCIDENTS_2019_MIDYEAR_PROVISIONAL (
  "Accident_Index" VARCHAR,
  "Status" VARCHAR,
  "Location_Easting_OSGR" VARCHAR,
  "Location_Northing_OSGR" VARCHAR,
  "Police_Force" VARCHAR,
  "Accident_Severity" VARCHAR,
  "Number_of_Vehicles" VARCHAR,
  "Number_of_Casualties" VARCHAR,
  "Date" VARCHAR,
  "Time" VARCHAR,
  "st_Road_Class" VARCHAR,
  "st_Road_Number" VARCHAR,
  "Road_Type" VARCHAR,
  "Speed_limit" VARCHAR,
  "Junction_Detail" VARCHAR,
  "Junction_Control" VARCHAR,
  "nd_Road_Class" VARCHAR,
  "nd_Road_Number" VARCHAR,
  "Pedestrian_Crossing-Human_Control" VARCHAR,
  "Pedestrian_Crossing-Physical_Facilities" VARCHAR,
  "Light_Conditions" VARCHAR,
  "Weather_Conditions" VARCHAR,
  "Road_Surface_Conditions" VARCHAR,
  "Special_Conditions_at_Site" VARCHAR,
  "Carriageway_Hazards" VARCHAR
);

TRUNCATE TABLE IF EXISTS ACCIDENTS_2019_MIDYEAR_PROVISIONAL;

COPY INTO ACCIDENTS_2019_MIDYEAR_PROVISIONAL
FROM @SPARQ_CHALLENGE.BRONZE.internal_stage/midyearprovisionalroadsafetydata2019datagov/
PATTERN = '.*\\.csv'
FILE_FORMAT = (FORMAT_NAME = 'FF_CSV_BRONZE')
FORCE = TRUE;

-- ---------------------------------------------------------------------------
-- CASUALTIES (2015–2018: same schema across years)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CASUALTIES (
  "Accident_Index" VARCHAR,
  "Vehicle_Reference" VARCHAR,
  "Casualty_Reference" VARCHAR,
  "Casualty_Class" VARCHAR,
  "Sex_of_Casualty" VARCHAR,
  "Age_of_Casualty" VARCHAR,
  "Age_Band_of_Casualty" VARCHAR,
  "Casualty_Severity" VARCHAR,
  "Pedestrian_Location" VARCHAR,
  "Pedestrian_Movement" VARCHAR,
  "Car_Passenger" VARCHAR,
  "Bus_or_Coach_Passenger" VARCHAR,
  "Pedestrian_Road_Maintenance_Worker" VARCHAR,
  "Casualty_Type" VARCHAR,
  "Casualty_Home_Area_Type" VARCHAR,
  "Casualty_IMD_Decile" VARCHAR
);

TRUNCATE TABLE IF EXISTS CASUALTIES;

COPY INTO CASUALTIES
FROM @SPARQ_CHALLENGE.BRONZE.internal_stage/
PATTERN = '^(roadsafetydata2015/Casualties_2015|roadsafetydatacusualties2016/Cas|roadsafetydatacasualties2017/Cas|roadsafetydatacasualties2018/dftRoadSafetyData_Casualties_2018)\\.csv$'
FILE_FORMAT = (FORMAT_NAME = 'FF_CSV_BRONZE')
FORCE = TRUE;

-- ---------------------------------------------------------------------------
-- VEHICLES (2015–2018: same schema across years)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS VEHICLES (
  "Accident_Index" VARCHAR,
  "Vehicle_Reference" VARCHAR,
  "Vehicle_Type" VARCHAR,
  "Towing_and_Articulation" VARCHAR,
  "Vehicle_Manoeuvre" VARCHAR,
  "Vehicle_Location-Restricted_Lane" VARCHAR,
  "Junction_Location" VARCHAR,
  "Skidding_and_Overturning" VARCHAR,
  "Hit_Object_in_Carriageway" VARCHAR,
  "Vehicle_Leaving_Carriageway" VARCHAR,
  "Hit_Object_off_Carriageway" VARCHAR,
  "1st_Point_of_Impact" VARCHAR,
  "Was_Vehicle_Left_Hand_Drive?" VARCHAR,
  "Journey_Purpose_of_Driver" VARCHAR,
  "Sex_of_Driver" VARCHAR,
  "Age_of_Driver" VARCHAR,
  "Age_Band_of_Driver" VARCHAR,
  "Engine_Capacity_(CC)" VARCHAR,
  "Propulsion_Code" VARCHAR,
  "Age_of_Vehicle" VARCHAR,
  "Driver_IMD_Decile" VARCHAR,
  "Driver_Home_Area_Type" VARCHAR,
  "Vehicle_IMD_Decile" VARCHAR
);

TRUNCATE TABLE IF EXISTS VEHICLES;

COPY INTO VEHICLES
FROM @SPARQ_CHALLENGE.BRONZE.internal_stage/
PATTERN = '^(roadsafetydata2015/Vehicles_2015|roadsafetydatavehicles2016/Veh|roadsafetydatavehicles2017/Veh|roadsafetydatavehicles2018/dftRoadSafetyData_Vehicles_2018)\\.csv$'
FILE_FORMAT = (FORMAT_NAME = 'FF_CSV_BRONZE')
FORCE = TRUE;