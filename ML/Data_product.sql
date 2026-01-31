 --PHASE 3 
-- PHASE 3.1 - CRÉATION DU DATA PRODUCT 

CREATE SCHEMA IF NOT EXISTS ANALYTICS;
USE SCHEMA ANALYTICS;

-- TABLE 1 : VENTES ENRICHIES 

CREATE OR REPLACE TABLE ANALYTICS.SALES_ENRICHED AS
WITH promo_distinct AS (
  SELECT DISTINCT
    ft.transaction_id,
    ft.transaction_date,
    DATE_TRUNC('MONTH', ft.transaction_date) AS mois,
    ft.region,
    ft.amount,
    p.promotion_id,
    p.discount_percentage,
    p.product_category
  FROM SILVER.FINANCIAL_TRANSACTIONS_CLEAN ft
  LEFT JOIN SILVER.PROMOTIONS_DATA_CLEAN p
    ON ft.transaction_date BETWEEN p.start_date AND p.end_date
   AND ft.region = p.region
  WHERE ft.transaction_type = 'Sale'
)
SELECT
  transaction_id,
  transaction_date,
  mois,
  region,
  amount,

  COUNT(DISTINCT promotion_id) AS nb_promotions_actives,
  (COUNT(DISTINCT promotion_id) > 0) AS is_promo,

  LISTAGG(promotion_id, ', ')
    WITHIN GROUP (ORDER BY promotion_id) AS promotion_ids,

  LISTAGG(discount_percentage::STRING, ', ')
    WITHIN GROUP (ORDER BY discount_percentage DESC NULLS LAST) AS discount_percentages,

  LISTAGG(product_category, ', ')
    WITHIN GROUP (ORDER BY product_category) AS product_categories

FROM promo_distinct
GROUP BY transaction_id, transaction_date, mois, region, amount;


SELECT * FROM ANALYTICS.SALES_ENRICHED;

SELECT COUNT(*) FROM sales_enriched;

SELECT * FROM sales_enriched ;

-- TABLE 2 : PROMOTIONS ACTIVES
CREATE OR REPLACE TABLE ANALYTICS.promotions_analytics AS
SELECT 
    p.promotion_id,
    p.product_category              AS categorie_produit,
    p.promotion_type                AS type_promotion,
    p.discount_percentage           AS taux_remise,
    p.region                        AS region,
    p.start_date                    AS date_debut,
    p.end_date                      AS date_fin,
    DATEDIFF(DAY, p.start_date, p.end_date) AS duree_jours,

    COUNT(se.transaction_id)        AS nb_ventes_pendant_promo,
    COALESCE(SUM(se.amount), 0)     AS montant_ventes_pendant_promo,
    AVG(se.amount) AS panier_moyen_pendant_promo

FROM SILVER.promotions_data_clean p
LEFT JOIN ANALYTICS.sales_enriched se
    ON se.transaction_date BETWEEN p.start_date AND p.end_date
    AND se.region = p.region

GROUP BY
    p.promotion_id,
    p.product_category,
    p.promotion_type,
    p.discount_percentage,
    p.region,
    p.start_date,
    p.end_date;

Select * from ANALYTICS.promotions_analytics;

-- TABLE 3 : CLIENTS ENRICHIS 
CREATE OR REPLACE TABLE ANALYTICS.customers_enriched AS
SELECT
    cd.customer_id,
    cd.customer_name,
    DATEDIFF(YEAR, cd.date_of_birth, CURRENT_DATE()) AS age,
    cd.gender,
    cd.region,
    cd.country,
    cd.annual_income,
    cd.marital_status,

    CASE
        WHEN cd.annual_income < 50000 THEN 'Bas'
        WHEN cd.annual_income < 100000 THEN 'Moyen'
        WHEN cd.annual_income < 150000 THEN 'Élevé'
        ELSE 'Très élevé'
    END AS segment_revenu,

    CASE
        WHEN DATEDIFF(YEAR, cd.date_of_birth, CURRENT_DATE()) < 35 THEN 'Jeunes'
        WHEN DATEDIFF(YEAR, cd.date_of_birth, CURRENT_DATE()) < 55 THEN 'Adultes'
        ELSE 'Seniors'
    END AS segment_age

FROM SILVER.customer_demographics_clean cd;

Select * from ANALYTICS.customers_enriched;

-- DOCUMENTATION DES TABLES

--table sales_enriched

COMMENT ON TABLE ANALYTICS.SALES_ENRICHED IS
'Data Product ANALYTICS - SALES_ENRICHED.
Grain: 1 ligne = 1 transaction de vente (transaction_id).
Enrichissement promotions: promotions actives au moment de la vente (transaction_date BETWEEN start_date AND end_date) et même region.
Usages: KPI ventes, analyse promo vs non-promo.';

-- Documentation colonnes
COMMENT ON COLUMN ANALYTICS.SALES_ENRICHED.TRANSACTION_ID IS 'Identifiant unique de la transaction (grain de la table).';
COMMENT ON COLUMN ANALYTICS.SALES_ENRICHED.TRANSACTION_DATE IS 'Date de la transaction.';
COMMENT ON COLUMN ANALYTICS.SALES_ENRICHED.MOIS IS 'Mois de transaction (DATE_TRUNC(MONTH)).';
COMMENT ON COLUMN ANALYTICS.SALES_ENRICHED.REGION IS 'Région de vente (utilisée pour l’association aux promotions).';
COMMENT ON COLUMN ANALYTICS.SALES_ENRICHED.AMOUNT IS 'Montant de la vente.';
COMMENT ON COLUMN ANALYTICS.SALES_ENRICHED.NB_PROMOTIONS_ACTIVES IS 'Nombre de promotions distinctes actives à la date de vente (par région).';
COMMENT ON COLUMN ANALYTICS.SALES_ENRICHED.IS_PROMO IS 'Vrai si au moins une promotion active au moment de la vente.';
COMMENT ON COLUMN ANALYTICS.SALES_ENRICHED.PROMOTION_IDS IS 'Liste (texte) des promotion_id actives sur la transaction (multi-valeurs).';
COMMENT ON COLUMN ANALYTICS.SALES_ENRICHED.DISCOUNT_PERCENTAGES IS 'Liste (texte) des taux de remise associés aux promos actives (tri décroissant).';
COMMENT ON COLUMN ANALYTICS.SALES_ENRICHED.PRODUCT_CATEGORIES IS 'Liste (texte) des catégories produit associées aux promos actives.';

--PROMOTIONS_ANALYTICS
-- Documentation de la table promotions_analytics
COMMENT ON TABLE ANALYTICS.PROMOTIONS_ANALYTICS IS
'Data Product ANALYTICS - PROMOTIONS_ANALYTICS.
Grain: 1 ligne = 1 promotion (promotion_id).
KPIs calculés sur les ventes pendant la période active (date_debut/date_fin) et même region.
Usages: évaluer performance des campagnes, comparaison régions/types/catégories.';

-- Documentation colonnes
COMMENT ON COLUMN ANALYTICS.PROMOTIONS_ANALYTICS.PROMOTION_ID IS 'Identifiant unique de la promotion.';
COMMENT ON COLUMN ANALYTICS.PROMOTIONS_ANALYTICS.CATEGORIE_PRODUIT IS 'Catégorie produit ciblée par la promotion.';
COMMENT ON COLUMN ANALYTICS.PROMOTIONS_ANALYTICS.TYPE_PROMOTION IS 'Type de promotion (ex: discount, bundle...).';
COMMENT ON COLUMN ANALYTICS.PROMOTIONS_ANALYTICS.TAUX_REMISE IS 'Taux de remise appliqué par la promotion.';
COMMENT ON COLUMN ANALYTICS.PROMOTIONS_ANALYTICS.REGION IS 'Région où la promotion est active.';
COMMENT ON COLUMN ANALYTICS.PROMOTIONS_ANALYTICS.DATE_DEBUT IS 'Date de début de la promotion.';
COMMENT ON COLUMN ANALYTICS.PROMOTIONS_ANALYTICS.DATE_FIN IS 'Date de fin de la promotion.';
COMMENT ON COLUMN ANALYTICS.PROMOTIONS_ANALYTICS.DUREE_JOURS IS 'Durée de la promotion en jours (DATEDIFF).';
COMMENT ON COLUMN ANALYTICS.PROMOTIONS_ANALYTICS.NB_VENTES_PENDANT_PROMO IS 'Nombre de transactions pendant la période de la promo (par region).';
COMMENT ON COLUMN ANALYTICS.PROMOTIONS_ANALYTICS.MONTANT_VENTES_PENDANT_PROMO IS 'Somme des montants de vente pendant la promotion.';
COMMENT ON COLUMN ANALYTICS.PROMOTIONS_ANALYTICS.PANIER_MOYEN_PENDANT_PROMO IS 'Montant moyen des ventes pendant la promotion.';

--CUSTOMERS_ENRICHED

-- Documentation de la table customers_enriched
COMMENT ON TABLE ANALYTICS.CUSTOMERS_ENRICHED IS
'Data Product ANALYTICS - CUSTOMERS_ENRICHED.
Grain: 1 ligne = 1 client (customer_id).
Enrichissements: âge (calculé) + segmentations revenu et âge.
Usages: segmentation marketing, analyses clients.';

-- Documentation colonnes
COMMENT ON COLUMN ANALYTICS.CUSTOMERS_ENRICHED.CUSTOMER_ID IS 'Identifiant unique du client.';
COMMENT ON COLUMN ANALYTICS.CUSTOMERS_ENRICHED.CUSTOMER_NAME IS 'Nom du client.';
COMMENT ON COLUMN ANALYTICS.CUSTOMERS_ENRICHED.AGE IS 'Âge calculé à partir de date_of_birth et CURRENT_DATE.';
COMMENT ON COLUMN ANALYTICS.CUSTOMERS_ENRICHED.GENDER IS 'Genre du client.';
COMMENT ON COLUMN ANALYTICS.CUSTOMERS_ENRICHED.REGION IS 'Région du client.';
COMMENT ON COLUMN ANALYTICS.CUSTOMERS_ENRICHED.COUNTRY IS 'Pays du client.';
COMMENT ON COLUMN ANALYTICS.CUSTOMERS_ENRICHED.ANNUAL_INCOME IS 'Revenu annuel déclaré.';
COMMENT ON COLUMN ANALYTICS.CUSTOMERS_ENRICHED.MARITAL_STATUS IS 'Statut marital.';
COMMENT ON COLUMN ANALYTICS.CUSTOMERS_ENRICHED.SEGMENT_REVENU IS 'Segment revenu (Bas/Moyen/Élevé/Très élevé) selon annual_income.';
COMMENT ON COLUMN ANALYTICS.CUSTOMERS_ENRICHED.SEGMENT_AGE IS 'Segment âge (Jeunes/Adultes/Seniors) selon l’âge calculé.';

--CONTROLES DE COHERENCE METIER – DATA PRODUCT ANALYTICS

-- SALES_ENRICHED
-- Unicité des transactions
SELECT COUNT(*) - COUNT(DISTINCT transaction_id) AS nb_doublons
FROM ANALYTICS.SALES_ENRICHED;

-- Cohérence flag promo
SELECT COUNT(*) AS nb_incoherences_promo
FROM ANALYTICS.SALES_ENRICHED
WHERE (nb_promotions_actives > 0) <> is_promo;

-- Montants invalides
SELECT COUNT(*) AS nb_amount_invalid
FROM ANALYTICS.SALES_ENRICHED
WHERE amount IS NULL OR amount < 0;

-- Mois vs date transaction
SELECT COUNT(*) AS nb_mois_incoherents
FROM ANALYTICS.SALES_ENRICHED
WHERE mois <> DATE_TRUNC('MONTH', transaction_date);

-- PROMOTIONS_ANALYTICS

-- Dates promo invalides
SELECT COUNT(*) AS nb_dates_invalides
FROM ANALYTICS.PROMOTIONS_ANALYTICS
WHERE date_fin < date_debut;

-- KPI ventes incohérents
SELECT COUNT(*) AS nb_kpi_invalides
FROM ANALYTICS.PROMOTIONS_ANALYTICS
WHERE nb_ventes_pendant_promo = 0
  AND montant_ventes_pendant_promo <> 0;

-- Taux remise hors bornes
SELECT COUNT(*) AS nb_taux_invalides
FROM ANALYTICS.PROMOTIONS_ANALYTICS
WHERE taux_remise < 0 OR taux_remise > 1;

-- CUSTOMERS_ENRICHED
-- Unicité clients
SELECT COUNT(*) - COUNT(DISTINCT customer_id) AS nb_doublons
FROM ANALYTICS.CUSTOMERS_ENRICHED;

-- Ages aberrants
SELECT COUNT(*) AS nb_age_invalid
FROM ANALYTICS.CUSTOMERS_ENRICHED
WHERE age < 0 OR age > 120;

-- Segments revenu incohérents
SELECT COUNT(*) AS nb_segment_revenu_invalid
FROM ANALYTICS.CUSTOMERS_ENRICHED
WHERE (annual_income < 50000 AND segment_revenu <> 'Bas')
   OR (annual_income >= 50000 AND annual_income < 100000 AND segment_revenu <> 'Moyen')
   OR (annual_income >= 100000 AND annual_income < 150000 AND segment_revenu <> 'Élevé')
   OR (annual_income >= 150000 AND segment_revenu <> 'Très élevé');

-- Segments âge incohérents
SELECT COUNT(*) AS nb_segment_age_invalid
FROM ANALYTICS.CUSTOMERS_ENRICHED
WHERE (age < 35 AND segment_age <> 'Jeunes')
   OR (age >= 35 AND age < 55 AND segment_age <> 'Adultes')
   OR (age >= 55 AND segment_age <> 'Seniors');

