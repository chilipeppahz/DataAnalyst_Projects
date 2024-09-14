-- data cleaning
-- ---------------------------------------------
-- 1. remove duplicates
-- 2. standardise data
-- 3. null/blank value filtering
-- 4. remove unused columns
-- ---------------------------------------------

drop table if exists layoffs_staging;

create table layoffs_staging
like layoffs;

insert layoffs_staging
select * from layoffs;

-- 1. duplicates filtering

with duplicate_cte as
(
	select * ,
	row_number() over(
		partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions
		) as row_num
	from layoffs_staging
)
select * from duplicate_cte
where row_num > 1;

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
select * ,
	row_number() over(
		partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions
		) as row_num
	from layoffs_staging;

delete from layoffs_staging2 where row_num > 1;

select * from layoffs_staging2;

-- 2. standardizing

select industry, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

update layoffs_staging2
set location = 'Malmo'
where industry like 'MalmÃ¶';

update layoffs_staging2
set location = 'Florianopolis'
where location like 'FlorianÃ³polis';

update layoffs_staging2
set location = 'Dusseldorf'
where location like 'DÃ¼sseldorf';

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

update layoffs_staging2
set `date` = str_to_date(trim(`date`), '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;

-- 3. null/blank values

select *
from layoffs_staging2
where company like 'bally%';

update layoffs_staging2
set industry = null
where industry = '';

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

-- 4. remove columns

select * 
from layoffs_staging2;

delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;