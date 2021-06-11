CREATE DATABASE beijing_2;
USE beijing_2;


-- DIMENZIJSKI MODEL
CREATE TABLE dim_date (
	date_tk INT NOT NULL, 
    date DATE,
    day INT,
    month INT,
    year INT,
    PRIMARY KEY (date_tk),
    UNIQUE INDEX date_tk (date_tk ASC)
);

CREATE TABLE dim_apartment_details (
	apartment_details_tk BIGINT NOT NULL,
    version INT,
    date_from DATETIME,
    date_to DATETIME,
    apartment_details_id INT,
    bedroom INT,
    kitchen INT,
    bathroom INT,
    squares FLOAT,
    renovation_condition VARCHAR(20),
    five_years_property VARCHAR(12),
    PRIMARY KEY (apartment_details_tk),
    UNIQUE INDEX apartment_details_tk (apartment_details_tk ASC)
);

CREATE TABLE dim_building_details (
	building_details_tk BIGINT NOT NULL,
    version INT,
    date_from DATETIME,
    date_to DATETIME,
    building_details_id INT,
    elevator VARCHAR(8),
    building_structure VARCHAR(15),
    building_type VARCHAR(15),
    PRIMARY KEY (building_details_tk),
    UNIQUE INDEX building_details_tk (building_details_tk ASC)
);

CREATE TABLE fact_sales (
	sales_tk INT NOT NULL,
    sale_price FLOAT NOT NULL,
    avg_square_price FLOAT NOT NULL,
    year_built INT NOT NULL,
    DOM INT,
    district VARCHAR(15),
    date_tk INT NOT NULL,
    apartment_details_tk BIGINT NOT NULL,
    building_details_tk BIGINT NOT NULL,
    UNIQUE INDEX sales_tk (sales_tk ASC),
    CONSTRAINT FK_date FOREIGN KEY (date_tk) REFERENCES dim_date (date_tk) ON DELETE NO ACTION ON UPDATE CASCADE,
    CONSTRAINT FK_apartment_details FOREIGN KEY (apartment_details_tk) REFERENCES dim_apartment_details (apartment_details_tk) ON DELETE NO ACTION ON UPDATE CASCADE,
	CONSTRAINT FK_building_details FOREIGN KEY (building_details_tk) REFERENCES dim_building_details (building_details_tk) ON DELETE NO ACTION ON UPDATE CASCADE
);

SELECT 
	s.sale_price,
    s.avg_square_price,
    s.year_built,
    s.DOM,
    d.name,
    s.sale_date,
    ad.bedroom,
    ad.kitchen,
    ad.bathroom,
    ad.squares,
    ad.five_years_property,
    bd.elevator,
    bs.name,
    bt.name
FROM sale AS s, apartment_details AS ad, building_details AS bd, building_structure AS bs, building_type AS bt, district AS d
WHERE s.apartment_details_fk = ad.id 
AND s.building_details_fk = bd.id
AND bd.building_structure_fk = bs.id
AND bd.building_type_fk = bt.id
AND s.district_fk = d.id
ORDER BY s.sale_date ASC;
