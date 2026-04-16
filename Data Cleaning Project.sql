-- Data Cleaning --

SELECT *
FROM layoffs;

-- 1. Remove Duplicates 
-- 2. Standardize the Data
-- 3. NULL values or Blank values 
-- 4. Remove any Columns and Rowa

CREATE TABLE layofffs_staging
LIKE layoffs;

SELECT *
FROM layofffs_staging;

INSERT layofffs_staging
select *
from layoffs;

select *,
ROW_NUMBER() OVER(PARTITION BY company,industry,total_laid_off,percentage_laid_off,'date') AS row_num
from layofffs_staging;

WITH duplicate_cte AS
(
select *,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
from layofffs_staging
)
select *
from duplicate_cte
where row_num > 1;

select *
from layofffs_staging
where company =	 'Casper';



WITH duplicate_cte AS
(
select *,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
from layofffs_staging
)
delete
from duplicate_cte
where row_num > 1;






CREATE TABLE `layofffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layofffs_staging2
where row_num > 1;

insert into layofffs_staging2
select *,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
from layofffs_staging;

DELETE
from layofffs_staging2
where row_num > 1;

select *
from layofffs_staging2
where row_num > 1;

SET sql_safe_updates = 0;

DELETE 
FROM layofffs_staging2
WHERE row_num > 1;

select *
from layofffs_staging2
;

-- Standardizing Data --

SELECT company, TRIM(company)
FROM layofffs_staging2;

UPDATE layofffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry 
FROM layofffs_staging2
ORDER BY 1
;

UPDATE layofffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'
;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layofffs_staging2
ORDER BY 1;

UPDATE layofffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`
FROM layofffs_staging2
ORDER BY 1;

UPDATE layofffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')
;

ALTER TABLE layofffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layofffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layofffs_staging2
SET industry =  NULL
WHERE industry = '';



SELECT *
FROM layofffs_staging2
WHERE industry is NULL
OR industry = '';

SELECT *
FROM layofffs_staging2
WHERE company LIKE 'Bally%';


SELECT t1.industry, t2.industry
FROM layofffs_staging2 t1
JOIN layofffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layofffs_staging2 t1
JOIN layofffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;




SELECT *
FROM layofffs_staging2;


SELECT *
FROM layofffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layofffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layofffs_staging2;

ALTER TABLE layofffs_staging2
DROP COLUMN row_num;

