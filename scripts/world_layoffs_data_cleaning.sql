-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022


SELECT * 
FROM world_layoffs.layoffs;


-- First thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
CREATE TABLE world_layoffs.layoffs_staging
LIKE world_layoffs.layoffs;

INSERT layoffs_staging
SELECT * FROM world_layoffs.layoffs;

SELECT * 
FROM world_layoffs.layoffs_staging;


-- Now when we are data cleaning we usually follow a few steps
-- 1. Check for duplicates and remove any
-- 2. Standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. Remove any columns and rows that are not necessary - few ways





-- 1. REMOVE DUPLICATES 

# First let's check for duplicates

SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
-- These are the ones we want to delete where the row number is > 1 or 2or greater essentially


-- Create a new table and add column and those row numbers in. Then delete where row numbers are over 2, then delete that column
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
    ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
        ) AS row_num
    FROM 
        world_layoffs.layoffs_staging;
        
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;





-- 2. STANDARDIZE DATA 

SELECT *
FROM world_layoffs.layoffs_staging2;


-- If we look at company it looks like we have some  spaces, we find these rows and update it
SELECT company, TRIM(company)
FROM world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2
SET company = TRIM(company);




-- I also noticed the Crypto has multiple different variations. We need to standardize that all to Crypto
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- Everything looks good except apparently we have some "United States" and some "United States." with a period at the end. I standardize this.
SELECT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

UPDATE world_layoffs.layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Now if we run this again it is fixed
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;


-- I also fixed the data column
SELECT * 
FROM world_layoffs.layoffs_staging2;

-- We can use str to date to update this field
UPDATE world_layoffs.layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Now I can convert the data type properly
ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT * 
FROM world_layoffs.layoffs_staging2;





-- Step 3: Handle Null Values

-- Select all rows from the 'layoffs_staging2' table where the 'industry' column has empty values.
-- This query helps to identify records with missing or empty industry data.
SELECT * 
FROM world_layoffs.layoffs_staging2
WHERE industry = '';

-- Update the 'industry' column in the 'layoffs_staging2' table.
-- Set the value to NULL for rows where the 'industry' column currently has an empty string.
-- This converts empty strings to NULL, indicating that the data is missing.
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Select all rows from the 'layoffs_staging2' table.
-- This query is used to verify the changes made, ensuring that empty strings have been replaced with NULL values.
SELECT * 
FROM world_layoffs.layoffs_staging2;

-- The null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- So there isn't anything I want to change with the null values




-- 4. Remove any columns and rows we need to

SELECT * 
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2;

ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM world_layoffs.layoffs_staging2;





