-- DATA CLENING EXERCISE #1

-- Things to account when cleaning
-- 1. Remove Duplicates
-- 2. Standardize Data
-- 3. Handling Null Value or Blank Values
-- 4. Remove Unnecessary Columns

-- First step
-- Make a duplicate of the raw data
-- This will be the working environment
-- So that the raw data would be preserved

CREATE TABLE working_table
LIKE dummy_datasetcsv;

INSERT working_table
SELECT *
FROM dummy_datasetcsv;

-- START CLEANING

-- Find duplicates
WITH duplicated_rows AS (
	SELECT *,
	ROW_NUMBER() OVER(
		PARTITION BY 
        company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
	) AS row_num
	FROM working_table
)
SELECT *
FROM duplicated_rows
WHERE row_num > 1;


-- Store data cleaned without duplicates
-- -Create table for the data to be stored
CREATE TABLE no_duplicates AS 
SELECT *
FROM working_table
WHERE 1=0;

-- -Check if the table is working properly
SELECT *
FROM no_duplicates;

-- -Insert the data from the formula for finding duplicates
-- -Can't Use CTE together with insert
INSERT INTO no_duplicates 
SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
FROM (
	SELECT *,
	ROW_NUMBER() OVER(
		PARTITION BY 
        company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
	) AS row_num
	FROM working_table
) AS duplication_category
WHERE row_num = 1 AND NOT EXISTS (
-- Disprevents from inserting a duplicate making it reusable
    SELECT 1 
    FROM no_duplicates nd
    WHERE nd.company = duplication_category.company
      AND nd.location = duplication_category.location
      AND nd.industry = duplication_category.industry
      AND nd.total_laid_off = duplication_category.total_laid_off
      AND nd.percentage_laid_off = duplication_category.percentage_laid_off
      AND nd.`date` = duplication_category.`date`
      AND nd.stage = duplication_category.stage
      AND nd.country = duplication_category.country
      AND nd.funds_raised_millions = duplication_category.funds_raised_millions
);

-- Double check for duplication
WITH duplicated_rows AS (
	SELECT *,
	ROW_NUMBER() OVER(
		PARTITION BY 
        company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
	) AS row_num
	FROM no_duplicates
)
SELECT *
FROM duplicated_rows
WHERE row_num > 1;

-- START STANDARDAZING
-- clean column by column

-- First Column
SELECT DISTINCT company from no_duplicates ORDER BY 1;

UPDATE no_duplicates
-- REMOVE trailing spaces from company column
SET company = TRIM(company);

-- Second Column
SELECT DISTINCT location from no_duplicates ORDER BY 1;
-- Found out there are superscripts and other none standard letters

UPDATE no_duplicates
-- Convert misencoded UTF-8 strings in the 'location' column to the correct UTF-8 format.
-- The process involves:
-- 1. CONVERT(location USING latin1): Interprets the original UTF-8 string as Latin-1 to correct misencoding.
-- 2. HEX(...): Converts the Latin-1 string to its hexadecimal representation.
-- 3. UNHEX(...): Converts the hexadecimal string back to binary.
-- 4. CONVERT(... USING utf8mb4): Converts the binary data back to UTF-8, ensuring proper character representation.
SET location = CONVERT(UNHEX(HEX(CONVERT(location USING latin1))) USING utf8mb4)
WHERE HEX(location) REGEXP '^(..)*[8-9A-F]';

-- Third Column
SELECT DISTINCT industry from no_duplicates ORDER BY 1;
-- Standardize industry with multiple description
UPDATE no_duplicates
SET industry = "Crypto"
WHERE industry LIKE 'Crypto%';

-- Fifth column
SELECT DISTINCT `date` from no_duplicates ORDER BY 1;
-- Convert STR to standard date format
UPDATE no_duplicates
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');
-- Convert column data type
ALTER TABLE no_duplicates
MODIFY COLUMN `date` DATE;

-- Seventh column
SELECT DISTINCT country from no_duplicates ORDER BY 1;

UPDATE no_duplicates
SET country = "United States"
WHERE country LIKE "United States%";

-- NEXT IS HANDLING NULL VALUES
-- companies having blank industry but other entries have
UPDATE no_duplicates t1
JOIN no_duplicates t2
USING(company)
SET t1.industry = t2.industry
WHERE 
	(t1.industry = '' OR t1.industry IS NULL) AND 
	t2.industry != '';

SELECT w.company, w.industry, n.industry as updated
FROM working_table as w
JOIN no_duplicates as n
USING(company)
WHERE w.industry = '' OR w.industry IS NULL;

-- NEXT DELETE Unncessary data
DELETE FROM no_duplicates
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;