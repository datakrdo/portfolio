/*
Covid 19 Data Exploration 
Tool used: MS SQL Server
Skills used: Aggregate Functions, Converting Data Types, Windows Functions, Joins, CTE's, Temp Tables, Creating Views
*/


SELECT *
FROM CovidPortfolio..Deaths
ORDER BY location,date


-- First Step: Select the data I'm going to be using
SELECT location, date, total_cases, new_cases, total_deaths, population
FROM CovidPortfolio..Deaths
ORDER BY 1,2

-- Second Step: Total Cases vs Total Deaths
-- Shows likelihood of dying if you contract COVID in your Country
SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS death_pct
FROM CovidPortfolio..Deaths
WHERE location LIKE 'Argentina'
ORDER BY 1,2

-- Third Step: Total Cases vs Population
-- Shows % of population with COVID
SELECT location, date, population, total_cases, (total_cases/population)*100 AS infected_pop_pct
FROM CovidPortfolio..Deaths
WHERE location = 'Argentina'
ORDER BY 1,2

-- Fourth Step: Countries with highest infected population
SELECT location, population, MAX(total_cases) AS highest_infected_people, MAX((total_cases/population))*100 AS infected_pop_pct
FROM CovidPortfolio..Deaths
WHERE continent IS NOT NULL
GROUP BY location, population
ORDER BY infected_pop_pct desc

-- Fifth Step: Countries with highest death count per population
SELECT location, MAX(cast(total_deaths as int)) AS total_death_count
FROM CovidPortfolio..Deaths
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY total_death_count desc

-- Sixth Step: Regions with highest dead count
SELECT location, MAX(cast(total_deaths as int)) AS total_death_count
FROM CovidPortfolio..Deaths
WHERE continent IS NULL
GROUP BY location
ORDER BY total_death_count desc

-- LET'S BREAK THINGS DOWN BY CONTINENT

-- Showing continents with highest dead count by population
SELECT continent, MAX(cast(total_deaths as int)) AS total_death_count
FROM CovidPortfolio..Deaths
WHERE continent IS NULL
GROUP BY location
ORDER BY total_death_count desc

-- GLOBAL NUMBERS
-- BY DAY
SELECT date, SUM(new_cases) AS total_cases, SUM(CAST(new_deaths AS INT)) as total_deaths, SUM(CAST(new_deaths AS INT))/SUM(new_cases)*100 AS death_pct
FROM CovidPortfolio..Deaths
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY 1,2

-- UP TO TODAY
SELECT SUM(new_cases) AS total_cases, SUM(CAST(new_deaths AS INT)) as total_deaths, SUM(CAST(new_deaths AS INT))/SUM(new_cases)*100 AS death_pct
FROM CovidPortfolio..Deaths
WHERE continent IS NOT NULL
ORDER BY 1,2


-- NOW LET'S JOIN
-- Looking at Total Population vs Vaccination

SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location ORDER BY dea.location, dea.date) as RollingVaccinatedPeople
FROM CovidPortfolio..Deaths AS dea
JOIN CovidPortfolio..Vaccinations AS vac
ON dea.date = vac.date
	AND dea.location = vac.location
WHERE dea.continent is not null 
ORDER BY 2,3

-- Common Table Expression (CTE) 

WITH PopvsVac (continent, location, date, population, new_vaccinations, RollingVaccinatedPeople)
AS
(
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingVaccinatedPeople
FROM CovidPortfolio..Deaths dea
JOIN CovidPortfolio..Vaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL 
)
SELECT *, (RollingVaccinatedPeople/population)*100 AS VaccinedPct
FROM PopvsVac

-- Temporary Table
-- Show % of vaccinated population

DROP TABLE IF EXISTS #PercentPopulationVaccinated
CREATE TABLE #PercentPopulationVaccinated
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
new_vaccinations numeric,
RollingVaccinatedPeople numeric
)

INSERT INTO #PercentPopulationVaccinated
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingVaccinatedPeople
FROM CovidPortfolio..Deaths dea
JOIN CovidPortfolio..Vaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date

SELECT *, (RollingVaccinatedPeople/population)*100 AS VaccinationPerct
FROM #PercentPopulationVaccinated

-- Creating View to store data for later visualization

USE CovidPortfolio
GO
CREATE VIEW PercentPopulationVaccinated AS
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.location ORDER BY dea.location, dea.date) as RollingVaccinatedPeople
FROM CovidPortfolio..Deaths AS dea
JOIN CovidPortfolio..Vaccinations AS vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL

SELECT *
FROM PercentPopulationVaccinated

SELECT SUM(new_cases) AS total_cases, SUM(CAST(new_deaths AS int)) AS total_deaths, SUM(CAST(new_deaths AS int))/SUM(New_Cases)*100 AS DeathPercentage
FROM CovidPortfolio..Deaths
WHERE continent IS NOT NULL
ORDER BY 1,2

SELECT location, SUM(CAST(new_deaths AS int)) AS TotalDeathCount
FROM CovidPortfolio..Deaths
WHERE continent IS NULL
AND location NOT IN ('World', 'European Union', 'International')
GROUP BY location
ORDER BY TotalDeathCount DESC


SELECT Location, Population, MAX(total_cases) AS HighestInfectionCount,MAX((total_cases/population))*100 AS PercentPopulationInfected
FROM CovidPortfolio..Deaths
GROUP BY Location, Population
ORDER BY PercentPopulationInfected DESC

SELECT Location, Population,date, MAX(total_cases) AS HighestInfectionCount,MAX((total_cases/population))*100 AS PercentPopulationInfected
FROM CovidPortfolio..Deaths
GROUP BY Location, Population, date
ORDER BY PercentPopulationInfected DESC
