USE SCHEMA SILVER;

-- Analyse des problèmes de qualité dans BRONZE
SELECT 
    COUNT(*) as total_rows,
    COUNT(customer_id) as customer_id_non_null,
    COUNT(customer_name) as customer_name_non_null,
    COUNT(date_of_birth) as date_of_birth_non_null,
    COUNT(gender) as gender_non_null,
    COUNT(region) as region_non_null,
    COUNT(country) as country_non_null,
    COUNT(city) as city_non_null,
    COUNT(marital_status) as marital_status_non_null,
    COUNT(annual_income) as annual_income_non_null
FROM BRONZE.customer_demographics;
--Il n'y a aucune valeur manquante 

-- 2. Vérifier les doublons
SELECT 
    customer_id, 
    COUNT(*) as nb_occurrences
FROM BRONZE.customer_demographics
GROUP BY customer_id
HAVING COUNT(*) > 1;
--il n'y a aucun doublon

-- 3. Vérifier les valeurs négatives pour annual_income
SELECT 
    MIN(annual_income) as min_income,
    MAX(annual_income) as max_income,
    COUNT(*) as nb_negative
FROM BRONZE.customer_demographics
WHERE annual_income < 0;
--Aucun revenu annuel négatif n’a été détecté

-- 4. Vérifier les dates aberrantes (naissance dans le futur)
SELECT 
    MIN(date_of_birth) as oldest_date,
    MAX(date_of_birth) as newest_date,
    COUNT(*) as nb_future_dates
FROM BRONZE.customer_demographics
WHERE date_of_birth > CURRENT_DATE();
--Aucune date de naissance future n’a été détectée

--Création de la table customer_demographics_clean 
CREATE OR REPLACE TABLE customer_demographics_clean AS
SELECT *
FROM BRONZE.customer_demographics;

--Compter le nombre de ligne dans cette table : 

SELECT COUNT(*) as total_rows_clean FROM customer_demographics_clean;

--Affichage d'un extrait de cette table : 
SELECT * FROM customer_demographics_clean LIMIT 10;

-- Analyse des problèmes de qualité
SELECT 
    COUNT(*) as total_rows,
    COUNT(*) - COUNT(interaction_id) as interaction_id_nulls,
    COUNT(*) - COUNT(interaction_date) as interaction_date_nulls,
    COUNT(*) - COUNT(duration_minutes) as duration_minutes_nulls,
    COUNT(*) - COUNT(customer_satisfaction) as customer_satisfaction_nulls
FROM BRONZE.customer_service_interactions;
--Aucune valeur manquanté n'a été détecté 

-- Vérifier les doublons
SELECT interaction_id, COUNT(*) FROM BRONZE.customer_service_interactions GROUP BY interaction_id HAVING COUNT(*) > 1;
--3 doublons ont été détectés 

-- Voir les lignes en doublon
SELECT *
FROM BRONZE.customer_service_interactions
WHERE interaction_id IN ('CSI5701976', 'CSI1558061', 'CSI4435949')
ORDER BY interaction_id;

--Analyse des valeurs manquantes 
SELECT COUNT(*) FROM BRONZE.customer_service_interactions WHERE duration_minutes < 0;
--Aucune durée négative

--Analyse des dates abérrantes 
SELECT COUNT(*) FROM BRONZE.customer_service_interactions WHERE interaction_date > CURRENT_DATE();
--Aucune date de futur n'a été détecté

--Créatino de la table customer_service_interactions_clean en enlevant les doublons
CREATE OR REPLACE TABLE customer_service_interactions_clean AS SELECT * FROM BRONZE.customer_service_interactions
QUALIFY ROW_NUMBER() OVER (PARTITION BY interaction_id ORDER BY interaction_date DESC, customer_satisfaction DESC NULLS LAST) = 1;

--Affichage d'un extrait de la table 
SELECT * FROM customer_service_interactions_clean LIMIT 10;

--Verfication du nombre de ligne de la table brute à la table nettoyé 
SELECT 
    'BRONZE' as source,
    COUNT(*) as nb_rows
FROM BRONZE.customer_service_interactions
UNION ALL
SELECT 
    'SILVER' as source,
    COUNT(*) as nb_rows
FROM customer_service_interactions_clean;

--Analyse des valeurs manquantes
SELECT 
    COUNT(*) as total_rows,
    COUNT(*) - COUNT(transaction_id) as transaction_id_nulls,
    COUNT(*) - COUNT(transaction_date) as transaction_date_nulls,
    COUNT(*) - COUNT(amount) as amount_nulls
FROM BRONZE.financial_transactions;
--Aucune valeur manquante n’a été détectée

-- Verification des Doublons 
SELECT transaction_id, COUNT(*) FROM BRONZE.financial_transactions GROUP BY transaction_id HAVING COUNT(*) > 1;
--Aucun doublons 

-- Montants négatifs
SELECT 
    MIN(amount) as min_amount,
    COUNT(*) as nb_negative
FROM BRONZE.financial_transactions
WHERE amount < 0;
--Aucun montant négatif n'a été détecté

--Dates futures
SELECT COUNT(*) as nb_future_dates 
FROM BRONZE.financial_transactions 
WHERE transaction_date > CURRENT_DATE();
--Aucune date futur n'a été détecté

--Création de la table financial_transactions_clean
CREATE OR REPLACE TABLE financial_transactions_clean AS
SELECT *
FROM BRONZE.financial_transactions;

--Affichage d'un extrait de la table 
SELECT * FROM financial_transactions_clean LIMIT 10;

--Analyse des valeurs manquantes 
SELECT 
    COUNT(*) as total_rows,
    COUNT(*) - COUNT(promotion_id) as promotion_id_nulls,
    COUNT(*) - COUNT(start_date) as start_date_nulls,
    COUNT(*) - COUNT(end_date) as end_date_nulls,
    COUNT(*) - COUNT(discount_percentage) as discount_percentage_nulls
FROM BRONZE.promotions_data;
--Aucune valeur manquante 

-- Vérification des Doublons
SELECT promotion_id, COUNT(*) FROM BRONZE.promotions_data  GROUP BY promotion_id HAVING COUNT(*) > 1;
--Aucun doublon n'a été détecté

--Pourcentages négatifs
SELECT COUNT(*) FROM BRONZE.promotions_data WHERE discount_percentage < 0;
--Aucun pourcentage négatif 

-- Analyse des Dates incohérentes
SELECT COUNT(*) FROM BRONZE.promotions_data WHERE start_date > end_date;
--Aucune date cohérente 

--Création de la table promotions_data_clean
CREATE OR REPLACE TABLE promotions_data_clean AS SELECT *FROM BRONZE.promotions_data;

--Affichage d'un extrait de la table 
SELECT * FROM promotions_data_clean LIMIT 10;

--Analyse des valeurs manquantes : 
SELECT 
    COUNT(*) as total_rows,
    COUNT(*) - COUNT(campaign_id) as campaign_id_nulls,
    COUNT(*) - COUNT(budget) as budget_nulls,
    COUNT(*) - COUNT(conversion_rate) as conversion_rate_nulls
FROM BRONZE.marketing_campaigns;
--Aucune valeur manquante n'a été détecté 

--Analyse des doublons 
SELECT campaign_id, COUNT(*) FROM BRONZE.marketing_campaigns GROUP BY campaign_id HAVING COUNT(*) > 1;
--De nombreux doublons ont été détectés 

-- Analyse Budgets négatifs
SELECT COUNT(*) FROM BRONZE.marketing_campaigns WHERE budget < 0;
--Aucun budget négatif n'a été détecté 

-- Analyse des dates incohérentes 
SELECT COUNT(*) FROM BRONZE.marketing_campaigns WHERE start_date > end_date;
--Aucune date incohérente n'a été détecté 

--Création de la table marketing_campaigns_clean en enlevant les doublons : 
CREATE OR REPLACE TABLE marketing_campaigns_clean AS
SELECT 
    campaign_id,
    campaign_name,
    campaign_type,
    product_category,
    target_audience,
    start_date,
    end_date,
    region,
    budget,
    reach,
    conversion_rate
FROM BRONZE.marketing_campaigns
QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY conversion_rate DESC NULLS LAST, budget DESC) = 1;

--Nombre de lignes totale intégrés dans la nouvelle table 
SELECT COUNT(*) as total_rows_clean FROM marketing_campaigns_clean;

--Affichage d'un extrait de la table 
SELECT * FROM marketing_campaigns_clean LIMIT 10;

--Comparaison du nombre de ligne de la table source à la table néttoyé 
SELECT 
    'BRONZE' as source,
    COUNT(*) as nb_rows
FROM BRONZE.marketing_campaigns
UNION ALL
SELECT 
    'SILVER' as source,
    COUNT(*) as nb_rows
FROM marketing_campaigns_clean;
--La déduplication appliquée à marketing_campaigns a réduit le volume de données de 5 000 à 4 861 lignes dans la couche SILVER.

--Analyse des valeurs manquantes 
SELECT 
    COUNT(*) as total_rows,
    COUNT(*) - COUNT(shipment_id) as shipment_id_nulls,
    COUNT(*) - COUNT(order_id) as order_id_nulls,
    COUNT(*) - COUNT(shipping_cost) as shipping_cost_nulls
FROM BRONZE.logistics_and_shipping;
--Aucune valeur manquante détécté 

--Analyse des doublons 
SELECT shipment_id, COUNT(*) FROM BRONZE.logistics_and_shipping GROUP BY shipment_id HAVING COUNT(*) > 1;
--Il y a 1 doublons qui a été détecté

--Analyse du coût négatif 
SELECT COUNT(*) FROM BRONZE.logistics_and_shipping WHERE shipping_cost < 0;
--Aucun coût négatif n'a été détecté 

--Analyse des dates abérrantes
SELECT COUNT(*) FROM BRONZE.logistics_and_shipping WHERE ship_date > estimated_delivery;
--Aucune incohérence entre dates d’expédition et de livraison n’a été détecté

--Création de la table logistics_and_shipping_clean
CREATE OR REPLACE TABLE logistics_and_shipping_clean AS
SELECT 
    shipment_id,
    order_id,
    ship_date,
    estimated_delivery,
    shipping_method,
    status,
    shipping_cost,
    destination_region,
    destination_country,
    carrier
FROM BRONZE.logistics_and_shipping
QUALIFY ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY ship_date DESC) = 1;

--Affichage d'un extrait de la table 
SELECT * FROM logistics_and_shipping_clean LIMIT 10;

--Comparaison du nombre de ligne entre la table source et la table nettoyée 
SELECT 
    'BRONZE' as source,
    COUNT(*) as nb_rows
FROM BRONZE.logistics_and_shipping
UNION ALL
SELECT 
    'SILVER' as source,
    COUNT(*) as nb_rows
FROM logistics_and_shipping_clean;

--Analyse des valeurs manquantes 
SELECT 
    COUNT(*) as total_rows,
    COUNT(*) - COUNT(supplier_id) as supplier_id_nulls,
    COUNT(*) - COUNT(lead_time) as lead_time_nulls,
    COUNT(*) - COUNT(reliability_score) as reliability_score_nulls
FROM BRONZE.supplier_information;
--Aucune valeur manquante n'a été détecté

--Analyse des doublons
SELECT supplier_id, COUNT(*) FROM BRONZE.supplier_information GROUP BY supplier_id HAVING COUNT(*) > 1;
--Il y a plusieurs doublons qui ont été détectés 

--Analyse d'une valeur d'approvisionnement négative 
SELECT COUNT(*) FROM BRONZE.supplier_information WHERE lead_time < 0;
--Aucune valeur négative n'a été détecté

--Analyse des valeurs abérrantes
SELECT COUNT(*) FROM BRONZE.supplier_information WHERE reliability_score < 0 OR reliability_score > 100;
--Aucun score de fiabilité hors plage [0–100] n’a été détecté

--Création de la table supplier_information_clean
CREATE OR REPLACE TABLE supplier_information_clean AS
SELECT 
    supplier_id,
    supplier_name,
    product_category,
    region,
    country,
    city,
    lead_time,
    reliability_score,
    quality_rating
FROM BRONZE.supplier_information
QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY reliability_score DESC NULLS LAST, quality_rating DESC) = 1;

--Affichage d'un extrait de la table 
SELECT * FROM supplier_information_clean LIMIT 10;

--Comparaison des lignes entre la table source et la table nettoyée
SELECT 
    'BRONZE' as source,
    COUNT(*) as nb_rows
FROM BRONZE.supplier_information
UNION ALL
SELECT 
    'SILVER' as source,
    COUNT(*) as nb_rows
FROM supplier_information_clean;

--Analyse des valeurs manquantes
SELECT 
    COUNT(*) as total_rows,
    COUNT(*) - COUNT(employee_id) as employee_id_nulls,
    COUNT(*) - COUNT(hire_date) as hire_date_nulls,
    COUNT(*) - COUNT(salary) as salary_nulls
FROM BRONZE.employee_records;
--Aucune valeur manquante n'a été détecté

--Analyse des doublons 
SELECT employee_id, COUNT(*) FROM BRONZE.employee_records GROUP BY employee_id HAVING COUNT(*) > 1;
--Il y a plusieurs doublons qui ont été détectés

--Analyse du salaire négatif
SELECT COUNT(*) FROM BRONZE.employee_records WHERE salary < 0;
--Aucun salaire négatif n'a été détecté

--Analyse des dates abérrantes
SELECT COUNT(*) FROM BRONZE.employee_records WHERE hire_date > CURRENT_DATE() OR date_of_birth > CURRENT_DATE();
--Aucune date futur n'a été détecté

--Création de la table employee_records_clean en enlevant les doublons : 
CREATE OR REPLACE TABLE employee_records_clean AS
SELECT 
    employee_id,
    name,
    date_of_birth,
    hire_date,
    department,
    job_title,
    salary,
    region,
    country,
    email
FROM BRONZE.employee_records
QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY hire_date DESC NULLS LAST, salary DESC NULLS LAST) = 1;

--Affichage d'un extrait de la table
SELECT * FROM employee_records_clean LIMIT 10;

--Comparaison des lignes intégrés entre la table source et la table nettoyée 
SELECT 
    'BRONZE' as source,
    COUNT(*) as nb_rows
FROM BRONZE.employee_records
UNION ALL
SELECT 
    'SILVER' as source,
    COUNT(*) as nb_rows
FROM employee_records_clean;

--Affichage d'un extrait de la table : 
SELECT * FROM BRONZE.inventory_raw LIMIT 5;

--Création de la table inventory_clean avec les bons champs dans les bonnes colonnes 
CREATE OR REPLACE TABLE inventory_clean AS
SELECT 
    v:product_id::STRING as product_id,
    v:product_category::STRING as product_category,
    v:region::STRING as region,
    v:country::STRING as country,
    v:warehouse::STRING as warehouse,
    v:current_stock::INTEGER as current_stock,
    v:reorder_point::INTEGER as reorder_point,
    v:lead_time::INTEGER as lead_time,
    v:last_restock_date::DATE as last_restock_date
FROM BRONZE.inventory_raw
WHERE v:product_id IS NOT NULL;

--Affichage d'un extrait de la table
SELECT * FROM inventory_clean LIMIT 10;

--Affichage d'un extrait de la table
SELECT * FROM BRONZE.store_locations_raw LIMIT 5;

--Création de la table inventory_clean avec les bons champs dans les bonnes colonnes 
CREATE OR REPLACE TABLE store_locations_clean AS
SELECT 
    w:store_id::STRING as store_id,
    w:store_name::STRING as store_name,
    w:store_type::STRING as store_type,
    w:region::STRING as region,
    w:country::STRING as country,
    w:city::STRING as city,
    w:address::STRING as address,
    w:postal_code::STRING as postal_code,
    w:square_footage::NUMBER(10,2) as square_footage,
    w:employee_count::INTEGER as employee_count
FROM BRONZE.store_locations_raw
WHERE w:store_id IS NOT NULL;

--Affichage d'un extrait de la table
SELECT * FROM store_locations_clean LIMIT 10;

--Affichage d'un extrait de la table
SELECT raw_line FROM BRONZE.product_reviews_raw LIMIT 20;

--Analyse du nombre de champs 
SELECT
  COUNT(*) AS nb_lignes,
  MIN(ARRAY_SIZE(SPLIT(raw_line, '\t'))) AS min_nb_champs,
  MAX(ARRAY_SIZE(SPLIT(raw_line, '\t'))) AS max_nb_champs
FROM BRONZE.product_reviews_raw
WHERE raw_line IS NOT NULL;
--Les 1 000 lignes de product_reviews_raw présentent toutes exactement 13 champs, confirmant une structure homogène après identification du délimiteur.

--Verification valeur valeur nulle
WITH parsed AS (
  SELECT
    TRY_TO_NUMBER(SPLIT_PART(raw_line, '\t', 1))        AS review_seq,
    SPLIT_PART(raw_line, '\t', 2)                       AS product_id,
    SPLIT_PART(raw_line, '\t', 3)                       AS reviewer_id,
    SPLIT_PART(raw_line, '\t', 4)                       AS reviewer_name,
    TRY_TO_NUMBER(SPLIT_PART(raw_line, '\t', 5))        AS helpful_yes,
    TRY_TO_NUMBER(SPLIT_PART(raw_line, '\t', 6))        AS helpful_total,
    TRY_TO_NUMBER(SPLIT_PART(raw_line, '\t', 7))        AS rating,
    TRY_TO_TIMESTAMP_NTZ(SPLIT_PART(raw_line, '\t', 8)) AS review_ts,
    SPLIT_PART(raw_line, '\t', 9)                       AS review_title,
    SPLIT_PART(raw_line, '\t', 10)                      AS review_text,
    SPLIT_PART(raw_line, '\t', 11)                      AS category_lvl1,
    SPLIT_PART(raw_line, '\t', 12)                      AS category_lvl2,
    SPLIT_PART(raw_line, '\t', 13)                      AS category_description
  FROM BRONZE.product_reviews_raw
  WHERE raw_line IS NOT NULL
)
SELECT
  COUNT(*) AS total_rows,
  COUNT(*) - COUNT(review_seq)   AS review_seq_nulls,
  COUNT(*) - COUNT(product_id)   AS product_id_nulls,
  COUNT(*) - COUNT(reviewer_id)  AS reviewer_id_nulls,
  COUNT(*) - COUNT(rating)       AS rating_nulls,
  COUNT(*) - COUNT(review_ts)    AS review_ts_nulls
FROM parsed;
--Aucune valeur nulle détectée

--Verification des doublons 
WITH parsed AS (
  SELECT
    SPLIT_PART(raw_line, '\t', 2)                       AS product_id,
    SPLIT_PART(raw_line, '\t', 3)                       AS reviewer_id,
    TRY_TO_TIMESTAMP_NTZ(SPLIT_PART(raw_line, '\t', 8)) AS review_ts
  FROM BRONZE.product_reviews_raw
  WHERE raw_line IS NOT NULL
)
SELECT
  product_id,
  reviewer_id,
  review_ts,
  COUNT(*) AS nb_occurrences
FROM parsed
GROUP BY 1,2,3
HAVING COUNT(*) > 1;
--il y a 3 doublons qui ont été détectés 

--Valeur incohérente : 
WITH parsed AS (
  SELECT TRY_TO_NUMBER(SPLIT_PART(raw_line, '\t', 7)) AS rating
  FROM BRONZE.product_reviews_raw
  WHERE raw_line IS NOT NULL
)
SELECT COUNT(*) AS bad_rating
FROM parsed
WHERE rating < 1 OR rating > 5 OR rating IS NULL;

WITH parsed AS (
  SELECT
    TRY_TO_NUMBER(SPLIT_PART(raw_line, '\t', 5)) AS helpful_yes,
    TRY_TO_NUMBER(SPLIT_PART(raw_line, '\t', 6)) AS helpful_total
  FROM BRONZE.product_reviews_raw
  WHERE raw_line IS NOT NULL
)
SELECT COUNT(*) AS bad_votes
FROM parsed
WHERE helpful_yes < 0 OR helpful_total < 0;

WITH parsed AS (
  SELECT
    TRY_TO_NUMBER(SPLIT_PART(raw_line, '\t', 5)) AS helpful_yes,
    TRY_TO_NUMBER(SPLIT_PART(raw_line, '\t', 6)) AS helpful_total
  FROM BRONZE.product_reviews_raw
  WHERE raw_line IS NOT NULL
)
SELECT COUNT(*) AS bad_helpful_relation
FROM parsed
WHERE helpful_yes > helpful_total;
--Aucune valeur incohérente détectée

--vérification de la cohérence des données 
WITH parsed AS (
  SELECT TRY_TO_TIMESTAMP_NTZ(SPLIT_PART(raw_line, '\t', 8)) AS review_ts
  FROM BRONZE.product_reviews_raw
  WHERE raw_line IS NOT NULL
)
SELECT COUNT(*) AS nb_future_dates
FROM parsed
WHERE review_ts > CURRENT_TIMESTAMP();

WITH parsed AS (
  SELECT
    TRY_TO_NUMBER(SPLIT_PART(raw_line, '\t', 5)) AS helpful_yes,
    TRY_TO_NUMBER(SPLIT_PART(raw_line, '\t', 6)) AS helpful_total,
    TRY_TO_NUMBER(SPLIT_PART(raw_line, '\t', 7)) AS rating
  FROM BRONZE.product_reviews_raw
  WHERE raw_line IS NOT NULL
)
SELECT
  MIN(helpful_yes) AS min_helpful_yes,
  MAX(helpful_yes) AS max_helpful_yes,
  MIN(helpful_total) AS min_helpful_total,
  MAX(helpful_total) AS max_helpful_total,
  MIN(rating) AS min_rating,
  MAX(rating) AS max_rating
FROM parsed;

--Création de la table product_reviews_clean
CREATE OR REPLACE TABLE SILVER.product_reviews_clean AS
SELECT
  TRY_TO_NUMBER(SPLIT_PART(raw_line, '\t', 1))        AS review_seq,
  SPLIT_PART(raw_line, '\t', 2)                       AS product_id,
  SPLIT_PART(raw_line, '\t', 3)                       AS reviewer_id,
  SPLIT_PART(raw_line, '\t', 4)                       AS reviewer_name,
  TRY_TO_NUMBER(SPLIT_PART(raw_line, '\t', 5))        AS helpful_yes,
  TRY_TO_NUMBER(SPLIT_PART(raw_line, '\t', 6))        AS helpful_total,
  TRY_TO_NUMBER(SPLIT_PART(raw_line, '\t', 7))        AS rating,
  CAST(TRY_TO_TIMESTAMP_NTZ(SPLIT_PART(raw_line, '\t', 8)) AS DATE) AS review_ts,
  CAST(TRY_TO_TIMESTAMP_NTZ(SPLIT_PART(raw_line, '\t', 8)) AS DATE) AS review_date, 
  SPLIT_PART(raw_line, '\t', 9)                       AS review_title,
  SPLIT_PART(raw_line, '\t', 10)                      AS review_text,
  SPLIT_PART(raw_line, '\t', 11)                      AS category_lvl1,
  SPLIT_PART(raw_line, '\t', 12)                      AS category_lvl2,
  SPLIT_PART(raw_line, '\t', 13)                      AS category_description
FROM BRONZE.product_reviews_raw
WHERE raw_line IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY
    SPLIT_PART(raw_line, '\t', 2),                      -- product_id
    SPLIT_PART(raw_line, '\t', 3),                      -- reviewer_id
    TRY_TO_TIMESTAMP_NTZ(SPLIT_PART(raw_line, '\t', 8)) -- review_ts
  ORDER BY
    TRY_TO_NUMBER(SPLIT_PART(raw_line, '\t', 6)) DESC NULLS LAST, 
    TRY_TO_NUMBER(SPLIT_PART(raw_line, '\t', 5)) DESC NULLS LAST, 
    TRY_TO_NUMBER(SPLIT_PART(raw_line, '\t', 1)) DESC NULLS LAST 
) = 1;

--Affichage de nombre de lignes total de la table
SELECT COUNT(*) FROM SILVER.product_reviews_clean;

--Affichage de la table 
SELECT * FROM SILVER.product_reviews_clean;
