-- Création de la base
CREATE OR REPLACE DATABASE FOOD_BEVERAGE_LAB;
-- Utilisation de la base
USE DATABASE FOOD_BEVERAGE_LAB;
SHOW TABLES LIKE 'PROMOTIONS_ANALYTICS' IN SCHEMA FOOD_BEVERAGE_LAB.ANALYTICS;

-- Création des schémas
CREATE SCHEMA BRONZE;
CREATE SCHEMA SILVER;


--Utilisation du schéma bronze 
USE SCHEMA Bronze;

-- Création du stockage AWS S3
CREATE or replace STAGE food_beverage
 URL = "s3://logbrain-datalake/datasets/food-beverage/";

-- Création du fichier en format csv
create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ','
  record_delimiter = '\n'  skip_header = 1
  field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none'
  escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto'
  null_if = ('') comment = 'file format for ingesting csv';

  -- Vérifier que le stage fonctionne
LIST @food_beverage;

--Exploration de la structure des fichiers : 
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,$11, $12
FROM @food_beverage/customer_demographics.csv 
(file_format => csv) 
LIMIT 5;
--Création de la table customer_demographics
CREATE OR REPLACE TABLE customer_demographics (
    customer_id      STRING(50),
    customer_name    STRING(100),
    date_of_birth    DATE,
    gender           STRING(20),
    region           STRING(50),
    country          STRING(50),
    city             STRING(100),
    marital_status   STRING(20),
    annual_income    NUMBER(12,2)
);
--Copie des données du fichier d'origine stocké dans le cloud 
copy into customer_demographics from @food_beverage file_format= csv files=('customer_demographics.csv'); 

--Compter le nombre de lignes dans la table 
SELECT COUNT(*) as total_rows FROM customer_demographics;

--Affichage de la table 
SELECT * FROM customer_demographics LIMIT 10;

--Exploration de la structure des fichiers : 
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,$11, $12
FROM @food_beverage/customer_service_interactions.csv
(file_format => csv) 
LIMIT 5;

-- Création de la table customer_service_interactions 
CREATE OR REPLACE TABLE customer_service_interactions (
    interaction_id         STRING(50),
    interaction_date       DATE,
    interaction_type       STRING(50),        
    issue_category         STRING(100),       
    description            STRING(1000),     
    duration_minutes       NUMBER(10,2),      
    resolution_status      STRING(50),        
    follow_up_required     BOOLEAN,
    customer_satisfaction  NUMBER(3,1)        
);
--Copie des données du fichier d'origine stocké dans le cloud 
copy into customer_service_interactions from @food_beverage file_format= csv files=('customer_service_interactions.csv'); 

--Compter le nombre de lignes dans la table 
SELECT COUNT(*) as total_rows FROM customer_service_interactions;

--Affichage de la table 
select * from customer_service_interactions;

--Exploration de la structure des fichiers : 
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,$11, $12
FROM @food_beverage/financial_transactions.csv
(file_format => csv) 
LIMIT 5;

-- Création de la table financial_transactions
CREATE OR REPLACE TABLE financial_transactions (
    transaction_id    STRING(50), 
    transaction_date  DATE, 
    transaction_type  STRING(50),        
    amount            NUMBER(15,2),      
    payment_method    STRING(50),        
    entity            STRING(100),       
    region            STRING(50), 
    account_code      STRING(50)         
);

--Copie des données du fichier d'origine stocké dans le cloud 
copy into financial_transactions from @food_beverage file_format= csv files=('financial_transactions.csv'); 

--Compter le nombre de lignes dans la table 
SELECT COUNT(*) as total_rows FROM financial_transactions ;

--Affichage de la table 
select * from financial_transactions ;

--Exploration de la structure des fichiers :
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,$11, $12
FROM @food_beverage/promotions-data.csv
(file_format => csv) 
LIMIT 5;

-- Création de la table promotions_data
CREATE OR REPLACE TABLE promotions_data (
    promotion_id         STRING(50),
    product_category     STRING(100),
    promotion_type       STRING(50),        
    discount_percentage  NUMBER(5,2),       
    start_date           DATE,
    end_date             DATE,
    region               STRING(50)
);
--Copie des données du fichier d'origine stocké dans le cloud 
COPY INTO promotions_data FROM @food_beverage/promotions-data.csv FILE_FORMAT = (FORMAT_NAME = csv) ;

--Compter le nombre de lignes dans la table 
SELECT COUNT(*) as total_rows FROM promotions_data;

--Affichage de la table 
SELECT * FROM promotions_data LIMIT 10;

--Exploration de la structure du fichier :
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,$11, $12
FROM @food_beverage/marketing_campaigns.csv
(file_format => csv) 
LIMIT 5;

-- Création de la table marketing_campaigns
CREATE OR REPLACE TABLE marketing_campaigns (
    campaign_id       STRING(50),
    campaign_name     STRING(200),
    campaign_type     STRING(50),
    product_category  STRING(100),
    target_audience   STRING(100),
    start_date        DATE,
    end_date          DATE,
    region            STRING(50),
    budget            NUMBER(15,2),
    reach             INTEGER,
    conversion_rate   NUMBER(5,2)
);
--Copie des données du fichier d'origine stocké dans le cloud 
COPY INTO marketing_campaigns FROM @food_beverage/marketing_campaigns.csv FILE_FORMAT = (FORMAT_NAME = csv);

--Compter le nombre de lignes dans la table 
SELECT COUNT(*) as total_rows FROM marketing_campaigns;

--Affichage de la table 
SELECT * FROM marketing_campaigns LIMIT 10;

--Exploration de la structure du fichier :
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,$11,$12,$13,$14,$15,$16,$18,$19
FROM @food_beverage/product_reviews.csv
LIMIT 5;

--Création d'un nouveau format pour le fichier qui montre des erreurs de parsing 
CREATE OR REPLACE FILE FORMAT csv_raw_text
TYPE = 'CSV'
FIELD_DELIMITER = 'NONE'
RECORD_DELIMITER = '\n'
SKIP_HEADER = 0
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- Création de la table product_reviews_raw :
CREATE OR REPLACE TABLE product_reviews_raw (
    raw_line STRING
);

--Copie des données du fichier d'origine stocké dans le cloud 
COPY INTO product_reviews_raw FROM @food_beverage/product_reviews.csv FILE_FORMAT = (FORMAT_NAME = csv_raw_text);

--Compter le nombre de lignes dans la table 
SELECT COUNT(*) FROM product_reviews_raw;

--Affichage de la table 
SELECT * FROM product_reviews_raw LIMIT 20;

--Exploration de la structure du fichier :
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,$11, $12
FROM @food_beverage/logistics_and_shipping.csv
(file_format => csv) 
LIMIT 5;

-- Création de la table logistics_and_shipping :
CREATE OR REPLACE TABLE logistics_and_shipping (
    shipment_id          STRING(50),
    order_id             STRING(50),
    ship_date            DATE,
    estimated_delivery   DATE,
    shipping_method      STRING(50),
    status               STRING(50),
    shipping_cost        NUMBER(10,2),
    destination_region   STRING(50),
    destination_country  STRING(50),
    carrier              STRING(100)
);

--Copie des données du fichier d'origine stocké dans le cloud 
COPY INTO logistics_and_shipping FROM @food_beverage/logistics_and_shipping.csv FILE_FORMAT = (FORMAT_NAME = csv);

--Compter le nombre de lignes dans la table 
SELECT COUNT(*) FROM logistics_and_shipping;

--Affichage de la table 
SELECT * FROM logistics_and_shipping LIMIT 10;

--Exploration de la structure du fichier :
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,$11, $12
FROM @food_beverage/supplier_information.csv
(file_format => csv) 
LIMIT 5;

-- Création de la table supplier_information :
CREATE OR REPLACE TABLE supplier_information (
    supplier_id         STRING(50),
    supplier_name       STRING(200),
    product_category    STRING(100),
    region              STRING(50),
    country             STRING(50),
    city                STRING(100),
    lead_time           INTEGER,
    reliability_score   NUMBER(5,2),
    quality_rating      STRING(20)
);

--Copie des données du fichier d'origine stocké dans le cloud 
COPY INTO supplier_information FROM @food_beverage/supplier_information.csv FILE_FORMAT = (FORMAT_NAME = csv);

--Compter le nombre de lignes dans la table 
SELECT COUNT(*) FROM supplier_information;

--Affichage d'un extrait de la table 
SELECT * FROM supplier_information LIMIT 10;

--Exploration de la structure du fichier :
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,$11, $12
FROM @food_beverage/employee_records.csv
(file_format => csv) 
LIMIT 5;

-- Création de la table employee_records :
CREATE OR REPLACE TABLE employee_records (
    employee_id    STRING(50),
    name           STRING(100),
    date_of_birth  DATE,
    hire_date      DATE,
    department     STRING(100),
    job_title      STRING(100),
    salary         NUMBER(15,2),
    region         STRING(50),
    country        STRING(50),
    email          STRING(200)
);

--Copie des données du fichier d'origine stocké dans le cloud 
COPY INTO employee_records FROM @food_beverage/employee_records.csv FILE_FORMAT = (FORMAT_NAME = csv);

--Compter le nombre de lignes dans la table 
SELECT COUNT(*) FROM employee_records;

--Affichage d'un extrait de la table 
SELECT * FROM employee_records LIMIT 10;

--Création du format json
CREATE or REPLACE file format json 
type = 'JSON'
STRIP_OUTER_ARRAY=TRUE;

--Création de la table inventory_raw
CREATE OR REPLACE TABLE inventory_raw (
    v VARIANT);
    
--Copie des données du fichier d'origine stocké dans le cloud 
COPY INTO inventory_raw FROM @food_beverage/inventory.json FILE_FORMAT = (FORMAT_NAME = 'json');

--Compter le nombre de lignes dans la table 
SELECT COUNT(*) FROM inventory_raw;

--Affichage d'un extrait de la table 
Select * from inventory_raw LIMIT 10

--Création de la table store_locations_raw
CREATE OR REPLACE TABLE store_locations_raw (
    w VARIANT);
    
--Copie des données du fichier d'origine stocké dans le cloud 
COPY INTO store_locations_raw FROM @food_beverage/store_locations.json FILE_FORMAT = (FORMAT_NAME = 'json');

--Affichage d'un extrait de la table 
select * from store_locations_raw LIMIT 10;

--Identification des colonnes clés pour chaque table : 
SELECT 
    COUNT(DISTINCT customer_id) as unique_customers,
    MIN(date_of_birth) as oldest_birth,
    MAX(date_of_birth) as youngest_birth,
    COUNT(DISTINCT region) as nb_regions,
    COUNT(DISTINCT country) as nb_countries,
    COUNT(DISTINCT gender) as nb_genders,
    AVG(annual_income) as avg_income
FROM customer_demographics;

--La table customer_demographics contient 5 000 clients uniques, avec des dates de naissance comprises entre 1944 et 2007, couvrant une population adulte variée. Les données sont géographiquement diversifiées (7 régions, 33 pays) et présentent trois catégories de genre, avec un revenu moyen élevé.

SELECT 
    COUNT(DISTINCT interaction_id) as unique_interactions,
    MIN(interaction_date) as first_interaction,
    MAX(interaction_date) as last_interaction,
    COUNT(DISTINCT interaction_type) as nb_types,
    COUNT(DISTINCT issue_category) as nb_categories,
    AVG(duration_minutes) as avg_duration,
    AVG(customer_satisfaction) as avg_satisfaction,
    COUNT(DISTINCT resolution_status) as nb_status
FROM customer_service_interactions;
--La table customer_service_interactions recense 4 997 interactions uniques entre 2010 et 2023, couvrant 4 types de contacts et 5 catégories de problèmes, avec une durée moyenne d’environ 31 minutes et une satisfaction client proche de 3/5.

SELECT 
    COUNT(DISTINCT transaction_id) as unique_transactions,
    MIN(transaction_date) as first_transaction,
    MAX(transaction_date) as last_transaction,
    COUNT(DISTINCT transaction_type) as nb_types,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount,
    COUNT(DISTINCT payment_method) as nb_payment_methods,
    COUNT(DISTINCT region) as nb_regions
FROM financial_transactions;
--La table financial_transactions contient 5 000 transactions uniques entre 2010 et 2023, couvrant 5 types de transactions et 5 moyens de paiement, avec un montant moyen d’environ 5 016 et des valeurs comprises entre 12 et 9 998, réparties sur 7 régions.

SELECT 
    COUNT(DISTINCT campaign_id) as unique_campaigns,
    COUNT(DISTINCT campaign_type) as nb_types,
    COUNT(DISTINCT product_category) as nb_categories,
    MIN(start_date) as first_campaign,
    MAX(end_date) as last_campaign,
    SUM(budget) as total_budget,
    AVG(budget) as avg_budget,
    SUM(reach) as total_reach,
    AVG(reach) as avg_reach,
    AVG(conversion_rate) as avg_conversion_rate,
    COUNT(DISTINCT region) as nb_regions
FROM marketing_campaigns;
--La table marketing_campaigns regroupe des campagnes réparties sur 7 régions, avec un budget total d’environ 127 M, un budget moyen proche de 254 k et un taux de conversion moyen d’environ 5,5 %, pour une portée moyenne d’environ 50 k par campagne.

SELECT 
    COUNT(DISTINCT promotion_id) as unique_promotions,
    COUNT(DISTINCT product_category) as nb_categories,
    COUNT(DISTINCT promotion_type) as nb_types,
    MIN(start_date) as first_promo_start,
    MAX(end_date) as last_promo_end,
    AVG(discount_percentage) as avg_discount,
    MIN(discount_percentage) as min_discount,
    MAX(discount_percentage) as max_discount,
    COUNT(DISTINCT region) as nb_regions
FROM promotions_data;

--La table promotions_data comprend 87 promotions couvrant 75 types et 3 catégories de produits, réparties sur 9 régions, avec des réductions comprises entre 5 % et 25 % et une moyenne d’environ 15 %, sur la période 2020–2025.

SELECT 
    COUNT(DISTINCT shipment_id) as unique_shipments,
    COUNT(DISTINCT order_id) as unique_orders,
    MIN(ship_date) as first_shipment,
    MAX(ship_date) as last_shipment,
    MIN(estimated_delivery) as earliest_delivery,
    MAX(estimated_delivery) as latest_delivery,
    COUNT(DISTINCT shipping_method) as nb_shipping_methods,
    COUNT(DISTINCT status) as nb_status,
    AVG(shipping_cost) as avg_shipping_cost,
    SUM(shipping_cost) as total_shipping_cost,
    COUNT(DISTINCT destination_region) as nb_regions,
    COUNT(DISTINCT destination_country) as nb_countries,
    COUNT(DISTINCT carrier) as nb_carriers
FROM logistics_and_shipping;
--La table logistics_and_shipping recense près de 5 000 expéditions entre 2010 et 2023, couvrant 4 méthodes de livraison et 5 statuts, avec un coût moyen d’environ 52, réparties sur 7 régions, 33 pays et plus de 4 500 transporteurs distincts


SELECT 
    COUNT(DISTINCT supplier_id) as unique_suppliers,
    COUNT(DISTINCT supplier_name) as unique_supplier_names,
    COUNT(DISTINCT product_category) as nb_categories,
    COUNT(DISTINCT region) as nb_regions,
    COUNT(DISTINCT country) as nb_countries,
    AVG(lead_time) as avg_lead_time,
    MIN(lead_time) as min_lead_time,
    MAX(lead_time) as max_lead_time,
    AVG(reliability_score) as avg_reliability,
    COUNT(DISTINCT quality_rating) as nb_quality_ratings
FROM supplier_information;
--La table supplier_information regroupe 4 865 fournisseurs répartis sur 7 régions et 33 pays, avec un délai moyen de livraison d’environ 15 jours, des valeurs comprises entre 1 et 30 jours, et un score de fiabilité moyen proche de 0,70

SELECT 
    COUNT(DISTINCT employee_id) as unique_employees,
    MIN(hire_date) as first_hire,
    MAX(hire_date) as last_hire,
    MIN(date_of_birth) as oldest_employee,
    MAX(date_of_birth) as youngest_employee,
    COUNT(DISTINCT department) as nb_departments,
    COUNT(DISTINCT job_title) as nb_job_titles,
    AVG(salary) as avg_salary,
    MIN(salary) as min_salary,
    MAX(salary) as max_salary,
    COUNT(DISTINCT region) as nb_regions,
    COUNT(DISTINCT country) as nb_countries
FROM employee_records;
--La table employee_records comprend 4 867 employés embauchés entre 2000 et 2023, répartis sur 7 régions et 33 pays, couvrant 42 postes différents, avec un salaire moyen d’environ 89 946 et des valeurs de salaire comprises entre 30 020 et 149 989.

SELECT 
    COUNT(DISTINCT v:product_id) as unique_products,
    COUNT(DISTINCT v:product_category) as nb_categories,
    COUNT(DISTINCT v:region) as nb_regions,
    COUNT(DISTINCT v:country) as nb_countries,
    COUNT(DISTINCT v:warehouse) as nb_warehouses,
    AVG(v:current_stock::INTEGER) as avg_stock,
    SUM(v:current_stock::INTEGER) as total_stock,
    AVG(v:lead_time::INTEGER) as avg_lead_time
FROM inventory_raw;
--La table inventory_raw recense près de 5 000 produits répartis sur 7 catégories et 7 régions, stockés dans plus de 4 500 entrepôts à travers 33 pays, avec un stock moyen d’environ 5 002 unités et un délai d’approvisionnement moyen d’environ 55 jours.

SELECT 
    COUNT(DISTINCT w:store_id) as unique_stores,
    COUNT(DISTINCT w:region) as nb_regions,
    COUNT(DISTINCT w:country) as nb_countries,
    COUNT(DISTINCT w:city) as nb_cities,
    COUNT(DISTINCT w:store_type) as nb_store_types
FROM store_locations_raw;
--La table store_locations_raw recense 897 magasins répartis sur 7 régions et 33 pays, couvrant plus de 4 200 villes et 5 types de points de vente

-- Pour la table product_reviews_raw, les Colonnes clés seront identifier après le parsing pour la création du modèle silver car pour le moment il est chargé en raw text 