/*

Tool used: MS SQL Server
Skill: Data Cleaning, Joins, CTEs, Create Columns, Drop Columns

*/

SELECT *
FROM HousingData..Nashville;

-- TASK 1: Change Date Format
ALTER TABLE HousingData..Nashville
ADD SaleDateConverted DATE;

UPDATE HousingData..Nashville
SET SaleDateConverted = CONVERT(DATE,SaleDate);

ALTER TABLE HousingData..Nashville
DROP COLUMN SaleDate;

-- TASK 2: Check for Null Values. Fill them.
SELECT PropertyAddress
FROM HousingData..Nashville
WHERE PropertyAddress IS NULL;

-- we have 29 null values

SELECT one.ParcelID, one.PropertyAddress, two.ParcelID, two.PropertyAddress, ISNULL(one.PropertyAddress, two.PropertyAddress)
FROM HousingData..Nashville AS one
JOIN HousingData..Nashville AS two
ON one.ParcelID = two.ParcelID
AND one.[UniqueID ] <> two.[UniqueID ]
WHERE one.PropertyAddress IS NULL;

UPDATE one
SET PropertyAddress = ISNULL(one.PropertyAddress, two.PropertyAddress)
FROM HousingData..Nashville AS one
JOIN HousingData..Nashville AS two
	ON one.ParcelID = two.ParcelID
	AND one.[UniqueID ]<> two.[UniqueID ]
WHERE one.PropertyAddress IS NULL;

-- TASK 3: Split One Column Into Two

SELECT
SUBSTRING(PropertyAddress, 1, CHARINDEX(',', PropertyAddress) -1) AS Address,
SUBSTRING(PropertyAddress, CHARINDEX(',', PropertyAddress) +1, LEN(PropertyAddress)) AS Address
FROM HousingData..Nashville;

ALTER TABLE HousingData..Nashville
ADD StreetAddress NVARCHAR(255),
ADD CityAddress NVARCHAR(255);

UPDATE HousingData..Nashville
SET StreetAddress = SUBSTRING(PropertyAddress, 1, CHARINDEX(',', PropertyAddress) -1),
SET CityAddress = SUBSTRING(PropertyAddress, CHARINDEX(',', PropertyAddress) +1, LEN(PropertyAddress));

-- TASK 4: Split One Column Into Three

SELECT
PARSENAME(REPLACE(OwnerAddress,',','.') , 3),
PARSENAME(REPLACE(OwnerAddress,',','.') , 2),
PARSENAME(REPLACE(OwnerAddress,',','.') , 1)
FROM HousingData..Nashville;

ALTER TABLE HousingData..Nashville
ADD OwnerStreet NVARCHAR(255);

UPDATE HousingData..Nashville
SET OwnerStreet = PARSENAME(REPLACE(OwnerAddress,',','.') , 3);

ALTER TABLE HousingData..Nashville
ADD OwnerCity NVARCHAR(255);

UPDATE HousingData..Nashville
SET OwnerCity = PARSENAME(REPLACE(OwnerAddress,',','.') , 2);

ALTER TABLE HousingData..Nashville
ADD OwnerState NVARCHAR(255);

UPDATE HousingData..Nashville
SET OwnerState = PARSENAME(REPLACE(OwnerAddress,',','.') , 1);

-- TASK 5: Rename values in a column

SELECT SoldAsVacant
,	CASE WHEN SoldAsVacant = 'Y' THEN 'Yes'
		 WHEN SoldAsVacant = 'N' THEN 'No'
		 ELSE SoldAsVacant
	END
FROM HousingData..Nashville;

UPDATE HousingData..Nashville
SET SoldAsVacant = CASE WHEN SoldAsVacant = 'Y' THEN 'Yes'
				        WHEN SoldAsVacant = 'N' THEN 'No'
						ELSE SoldAsVacant
						END;

-- TASK 6: Remove Duplicates

WITH RowNumCTE AS(
SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY ParcelID,
				 PropertyAddress,
				 SalePrice,
				 SaleDateConverted,
				 LegalReference
				 ORDER BY 
					UniqueID
					) AS row_num
FROM HousingData..Nashville
)
DELETE
FROM RowNumCTE
WHERE row_num > 1;

-- TASK 7: Delete Unused Columns

ALTER TABLE HousingData..Nashville
DROP COLUMN OwnerAddress, TaxDistrict, PropertyAddress;