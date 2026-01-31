--Partie 2.1 – Compréhension des jeux de données 

SELECT
  'customer_demographics_clean' AS table_name,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT customer_id) AS unique_customers,
  MIN(date_of_birth) AS periode_debut,
  MAX(date_of_birth) AS periode_fin,
  COUNT(DISTINCT region) AS nb_regions,
  COUNT(DISTINCT country) AS nb_countries,
  ROUND(AVG(annual_income), 2) AS avg_income,
  MIN(annual_income) AS min_income,
  MAX(annual_income) AS max_income
FROM SILVER.customer_demographics_clean;
--La table customer_demographics_clean contient 5 000 clients uniques, répartis sur 7 régions et 33 pays, avec des dates de naissance entre 1944 et 2007.Le revenu annuel moyen est d’environ 109 k, avec des valeurs de salaire allant de 20 k à 199 k.

SELECT 
    'customer_service_interactions_clean' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT interaction_id) AS unique_interactions,
    MIN(interaction_date) AS periode_debut,
    MAX(interaction_date) AS periode_fin,
    COUNT(DISTINCT interaction_type) AS nb_types,
    COUNT(DISTINCT issue_category) AS nb_categories,
    COUNT(DISTINCT resolution_status) AS nb_status,
    ROUND(AVG(duration_minutes), 2) AS avg_duration,
    ROUND(AVG(customer_satisfaction), 2) AS avg_satisfaction
FROM SILVER.customer_service_interactions_clean;
--La table contient 4 997 interactions uniques couvrant la période 2010–2023, avec 4 types d’interactions, 5 catégories de problèmes et 3 statuts de résolution. La durée moyenne est d’environ 30,6 minutes et la satisfaction moyenne est proche de 3.

SELECT 
    'financial_transactions_clean' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT transaction_id) AS unique_transactions,
    MIN(transaction_date) AS periode_debut,
    MAX(transaction_date) AS periode_fin,
    COUNT(DISTINCT transaction_type) AS nb_types,
    COUNT(DISTINCT region) AS nb_regions,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    MIN(amount) AS min_amount,
    MAX(amount) AS max_amount
FROM financial_transactions_clean;
--La table contient 5 000 transactions couvrant la période 2010–2023, réparties sur 5 types et 7 régions. Le montant total des transactions dépasse 25 M, avec une moyenne d’environ 5 016, et des valeurs comprises entre 12 et 9 998 par transaction.

SELECT 
    'marketing_campaigns_clean' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT campaign_id) AS unique_campaigns,
    MIN(start_date) AS periode_debut,
    MAX(end_date) AS periode_fin,
    COUNT(DISTINCT campaign_type) AS nb_types,
    COUNT(DISTINCT product_category) AS nb_categories,
    COUNT(DISTINCT region) AS nb_regions,
    SUM(budget) AS total_budget,
    AVG(budget) AS avg_budget,
    AVG(conversion_rate) AS avg_conversion,
    SUM(reach) AS total_reach
FROM marketing_campaigns_clean;
--La table recense 4 861 campagnes marketing entre 2010 et 2018, couvrant 7 types, 7 catégories produits et 7 régions. Le budget total des campagnes dépasse 1,23 milliard, avec un budget moyen d’environ 254 k par campagne et un taux de conversion moyen de 5,5 %.

SELECT 
    'promotions_data_clean' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT promotion_id) AS unique_promotions,
    MIN(start_date) AS periode_debut,
    MAX(end_date) AS periode_fin,
    COUNT(DISTINCT product_category) AS nb_categories,
    COUNT(DISTINCT promotion_type) AS nb_types,
    COUNT(DISTINCT region) AS nb_regions,
    AVG(discount_percentage) AS avg_discount,
    MIN(discount_percentage) AS min_discount,
    MAX(discount_percentage) AS max_discount
FROM promotions_data_clean;
--La table recense 87 promotions sur la période 2020–2025, couvrant 3 catégories de produits, 75 types de promotion et 9 régions. Le taux de remise moyen est d’environ 15 %, avec des valeurs comprises entre 5 % et 25 %.

SELECT 
    'logistics_and_shipping_clean' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT shipment_id) AS unique_shipments,
    COUNT(DISTINCT order_id) AS unique_orders,
    MIN(ship_date) AS periode_debut,
    MAX(ship_date) AS periode_fin,
    COUNT(DISTINCT shipping_method) AS nb_shipping_methods,
    COUNT(DISTINCT status) AS nb_status,
    COUNT(DISTINCT destination_region) AS nb_regions,
    COUNT(DISTINCT carrier) AS nb_carriers,
    AVG(shipping_cost) AS avg_shipping_cost,
    SUM(shipping_cost) AS total_shipping_cost
FROM logistics_and_shipping_clean;
--La table contient 4 999 expéditions couvrant la période 2010–2023, réparties sur 4 modes de livraison, 5 statuts, 7 régions et plus de 4 500 transporteurs. Le coût moyen d’expédition est d’environ 52 par envoi, pour un coût total cumulé proche de 259 k.

SELECT 
    'supplier_information_clean' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT supplier_id) AS unique_suppliers,
    COUNT(DISTINCT product_category) AS nb_categories,
    COUNT(DISTINCT region) AS nb_regions,
    COUNT(DISTINCT country) AS nb_countries,
    AVG(lead_time) AS avg_lead_time,
    AVG(reliability_score) AS avg_reliability,
    COUNT(DISTINCT quality_rating) AS nb_quality_ratings
FROM supplier_information_clean;
--La table contient 4 865 fournisseurs uniques répartis sur 7 catégories de produits, 7 régions et 33 pays. Le délai moyen de livraison est d’environ 15 jours et le score de fiabilité moyen atteint 0,71, avec 3 niveaux de notation qualité.

SELECT 
    'employee_records_clean' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT employee_id) AS unique_employees,
    MIN(hire_date) AS periode_debut,
    MAX(hire_date) AS periode_fin,
    COUNT(DISTINCT department) AS nb_departments,
    COUNT(DISTINCT job_title) AS nb_job_titles,
    COUNT(DISTINCT region) AS nb_regions,
    COUNT(DISTINCT country) AS nb_countries,
    AVG(salary) AS avg_salary,
    MIN(salary) AS min_salary,
    MAX(salary) AS max_salary
FROM employee_records_clean;

--Les employés sont répartis sur 7 départements et 42 postes entre 2000 et 2023, avec un salaire moyen proche de 89 800 et des valeurs allant d’environ 30 000 à 150 000.

SELECT 
    'inventory_clean' AS table_name,
    COUNT(DISTINCT product_id) AS unique_products,
    MIN(last_restock_date) AS periode_debut,
    MAX(last_restock_date) AS periode_fin,
    COUNT(DISTINCT product_category) AS nb_categories,
    COUNT(DISTINCT warehouse) AS nb_warehouses,
    COUNT(DISTINCT region) AS nb_regions,
    COUNT(DISTINCT country) AS nb_countries,
    SUM(current_stock) AS total_stock,
    AVG(current_stock) AS avg_stock,
    COUNT(CASE WHEN current_stock <= reorder_point THEN 1 END) AS nb_low_stock
FROM inventory_clean;
--La table contient 4 986 produits répartis sur 7 catégories, 7 régions et 33 pays, avec un stock total d’environ 25 millions d’unités et une moyenne proche de 5 000 par produit. Seuls 97 articles sont actuellement sous le seuil de réapprovisionnement, indiquant une situation globale de stock maîtrisée.

SELECT 
    'store_locations_clean' AS table_name,
    COUNT(DISTINCT store_id) AS unique_stores,
    COUNT(DISTINCT store_type) AS nb_store_types,
    COUNT(DISTINCT region) AS nb_regions,
    COUNT(DISTINCT country) AS nb_countries,
    COUNT(DISTINCT city) AS nb_cities,
    AVG(square_footage) AS avg_square_footage,
    AVG(employee_count) AS avg_employees
FROM store_locations_clean;
--La table recense 897 magasins répartis dans 7 régions et 33 pays, couvrant 5 types de points de vente et plus de 4 200 villes. La surface moyenne est d’environ 5 487 m² avec près de 27 employés en moyenne.


SELECT 
    'product_reviews_clean' AS table_name,
    COUNT(DISTINCT product_id) AS unique_products,
    MIN(review_date) AS periode_debut,
    MAX(review_date) AS periode_fin,
    COUNT(DISTINCT reviewer_id) AS unique_reviewers,
    COUNT(DISTINCT category_lvl1) AS nb_categories_lvl1,
    AVG(rating) AS avg_rating,
    MIN(rating) AS min_rating,
    MAX(rating) AS max_rating,
    AVG(helpful_total) AS avg_helpful_votes
FROM product_reviews_clean;
--La table contient des avis pour 250 produits sur la période 2020–2025, rédigés par 966 utilisateurs uniques et répartis sur 7 catégories principales. La note moyenne est élevée (4,08/5), avec en moyenne un peu plus de 2 votes « utile » par avis, indiquant une satisfaction globale forte.

-- Partie 2.2 – Analyses exploratoires descriptives
-- 1. ANALYSE DE L'ÉVOLUTION DES VENTES DANS LE TEMPS

-- Évolution des ventes par mois
SELECT
  DATE_TRUNC('MONTH', transaction_date) AS mois,
  COUNT(DISTINCT transaction_id) AS nb_transactions,
  SUM(amount) AS total_ventes,
  AVG(amount) AS montant_moyen
FROM SILVER.financial_transactions_clean
WHERE transaction_type = 'Sale'
GROUP BY 1
ORDER BY 1;
--La comparaison mois par mois du nombre de transactions et du total des ventes met en évidence des pics d’activité et des baisses régulières au fil du temps, suggérant une saisonnalité dans les ventes.

-- 1.2 Évolution des ventes par trimestre
SELECT 
    DATE_TRUNC('QUARTER', transaction_date) AS trimestre,
    COUNT(DISTINCT transaction_id) AS nb_transactions,
    SUM(amount) AS total_ventes,
    AVG(amount) AS montant_moyen
FROM financial_transactions_clean
WHERE transaction_type = 'Sale'
GROUP BY 1
ORDER BY 1;
--L’analyse trimestrielle met en évidence des fluctuations du chiffre d’affaires au cours du temps. Ces variations s’expliquent à la fois par l’évolution du nombre de transactions et du montant moyen par vente, ce qui permet de distinguer les effets de volume des effets de valeur.

-- 1.3 Évolution des ventes par année
SELECT 
    YEAR(transaction_date) AS annee,
    COUNT(*) AS nb_transactions,
    SUM(amount) AS total_ventes,
    AVG(amount) AS montant_moyen,
    MIN(amount) AS vente_min,
    MAX(amount) AS vente_max
FROM financial_transactions_clean
WHERE transaction_type = 'Sale'
GROUP BY YEAR(transaction_date)
ORDER BY annee;
--L’analyse annuelle met en évidence une variabilité du chiffre d’affaires selon les années, avec un pic observé autour de 2018. Cette évolution s’explique à la fois par le nombre de transactions et par le montant moyen par vente. Le panier moyen reste relativement stable dans le temps, tandis que les montants maximaux annuels sont proches, suggérant une politique de prix constante sur les ventes de grande valeur.

-- 1.4 Tendance des ventes : comparaison année sur année
WITH ventes_annuelles AS (
    SELECT 
        YEAR(transaction_date) AS annee,
        SUM(amount) AS total_ventes
    FROM financial_transactions_clean
    WHERE transaction_type = 'Sale'
    GROUP BY YEAR(transaction_date)
)
SELECT 
    annee,
    total_ventes,
    LAG(total_ventes) OVER (ORDER BY annee) AS ventes_annee_precedente,
    total_ventes - LAG(total_ventes) OVER (ORDER BY annee) AS evolution_absolue,
    ROUND(((total_ventes - LAG(total_ventes) OVER (ORDER BY annee)) / 
           NULLIF(LAG(total_ventes) OVER (ORDER BY annee), 0)) * 100, 2) AS evolution_pct
FROM ventes_annuelles
ORDER BY annee;
--La comparaison année sur année met en évidence une forte variabilité du chiffre d’affaires. Certaines années connaissent des progressions significatives, tandis que d’autres enregistrent des baisses marquées pouvant dépasser 15 %. Cette volatilité suggère une activité sensible au contexte économique, à la saisonnalité longue ou à des évolutions de stratégie commerciale.

WITH ventes_annuelles AS (
    SELECT 
        YEAR(transaction_date) AS annee,
        SUM(amount) AS total_ventes
    FROM financial_transactions_clean
    WHERE transaction_type = 'Sale'
    GROUP BY YEAR(transaction_date)
),
evolutions AS (
    SELECT 
        annee,
        total_ventes,
        LAG(total_ventes) OVER (ORDER BY annee) AS ventes_annee_precedente,
        total_ventes - LAG(total_ventes) OVER (ORDER BY annee) AS evolution_absolue,
        ROUND(
            ((total_ventes - LAG(total_ventes) OVER (ORDER BY annee)) /
             NULLIF(LAG(total_ventes) OVER (ORDER BY annee), 0)) * 100,
            2
        ) AS evolution_pct
    FROM ventes_annuelles
)

SELECT
    AVG(evolution_pct) AS moyenne_evolution_pct,
    MIN(evolution_pct) AS minimum_evolution_pct,
    MAX(evolution_pct) AS meilleure_evolution_pct
FROM evolutions;
--L’analyse des évolutions annuelles met en évidence une croissance moyenne modérée de l’ordre de 1 %, indiquant une progression globale relativement stable sur la période étudiée. Néanmoins, cette tendance masque une forte variabilité d’une année sur l’autre, avec des baisses marquées pouvant atteindre −16,8 % ainsi que des phases de forte expansion culminant à +24,5 %. Ces fluctuations traduisent une sensibilité importante de l’activité aux conditions économiques ou commerciales.

-- Partie 2.2 – Analyses exploratoires descriptives
-- 1. ANALYSE DE L'ÉVOLUTION DES VENTES DANS LE TEMPS

-- Évolution des ventes par mois
SELECT
  DATE_TRUNC('MONTH', transaction_date) AS mois,
  COUNT(DISTINCT transaction_id) AS nb_transactions,
  SUM(amount) AS total_ventes,
  AVG(amount) AS montant_moyen
FROM SILVER.financial_transactions_clean
WHERE transaction_type = 'Sale'
GROUP BY 1
ORDER BY 1;
--La comparaison mois par mois du nombre de transactions et du total des ventes met en évidence des pics d’activité et des baisses régulières au fil du temps, suggérant une saisonnalité dans les ventes.

-- 1.2 Évolution des ventes par trimestre
SELECT 
    DATE_TRUNC('QUARTER', transaction_date) AS trimestre,
    COUNT(DISTINCT transaction_id) AS nb_transactions,
    SUM(amount) AS total_ventes,
    AVG(amount) AS montant_moyen
FROM financial_transactions_clean
WHERE transaction_type = 'Sale'
GROUP BY 1
ORDER BY 1;
--L’analyse trimestrielle met en évidence des fluctuations du chiffre d’affaires au cours du temps. Ces variations s’expliquent à la fois par l’évolution du nombre de transactions et du montant moyen par vente, ce qui permet de distinguer les effets de volume des effets de valeur.

-- 1.3 Évolution des ventes par année
SELECT 
    YEAR(transaction_date) AS annee,
    COUNT(*) AS nb_transactions,
    SUM(amount) AS total_ventes,
    AVG(amount) AS montant_moyen,
    MIN(amount) AS vente_min,
    MAX(amount) AS vente_max
FROM financial_transactions_clean
WHERE transaction_type = 'Sale'
GROUP BY YEAR(transaction_date)
ORDER BY annee;
--L’analyse annuelle met en évidence une variabilité du chiffre d’affaires selon les années, avec un pic observé autour de 2018. Cette évolution s’explique à la fois par le nombre de transactions et par le montant moyen par vente. Le panier moyen reste relativement stable dans le temps, tandis que les montants maximaux annuels sont proches, suggérant une politique de prix constante sur les ventes de grande valeur.

-- 1.4 Tendance des ventes : comparaison année sur année
WITH ventes_annuelles AS (
    SELECT 
        YEAR(transaction_date) AS annee,
        SUM(amount) AS total_ventes
    FROM financial_transactions_clean
    WHERE transaction_type = 'Sale'
    GROUP BY YEAR(transaction_date)
)
SELECT 
    annee,
    total_ventes,
    LAG(total_ventes) OVER (ORDER BY annee) AS ventes_annee_precedente,
    total_ventes - LAG(total_ventes) OVER (ORDER BY annee) AS evolution_absolue,
    ROUND(((total_ventes - LAG(total_ventes) OVER (ORDER BY annee)) / 
           NULLIF(LAG(total_ventes) OVER (ORDER BY annee), 0)) * 100, 2) AS evolution_pct
FROM ventes_annuelles
ORDER BY annee;
--La comparaison année sur année met en évidence une forte variabilité du chiffre d’affaires. Certaines années connaissent des progressions significatives, tandis que d’autres enregistrent des baisses marquées pouvant dépasser 15 %. Cette volatilité suggère une activité sensible au contexte économique, à la saisonnalité longue ou à des évolutions de stratégie commerciale.

WITH ventes_annuelles AS (
    SELECT 
        YEAR(transaction_date) AS annee,
        SUM(amount) AS total_ventes
    FROM financial_transactions_clean
    WHERE transaction_type = 'Sale'
    GROUP BY YEAR(transaction_date)
),
evolutions AS (
    SELECT 
        annee,
        total_ventes,
        LAG(total_ventes) OVER (ORDER BY annee) AS ventes_annee_precedente,
        total_ventes - LAG(total_ventes) OVER (ORDER BY annee) AS evolution_absolue,
        ROUND(
            ((total_ventes - LAG(total_ventes) OVER (ORDER BY annee)) /
             NULLIF(LAG(total_ventes) OVER (ORDER BY annee), 0)) * 100,
            2
        ) AS evolution_pct
    FROM ventes_annuelles
)

SELECT
    AVG(evolution_pct) AS moyenne_evolution_pct,
    MIN(evolution_pct) AS minimum_evolution_pct,
    MAX(evolution_pct) AS meilleure_evolution_pct
FROM evolutions;
--L’analyse des évolutions annuelles met en évidence une croissance moyenne modérée de l’ordre de 1 %, indiquant une progression globale relativement stable sur la période étudiée. Néanmoins, cette tendance masque une forte variabilité d’une année sur l’autre, avec des baisses marquées pouvant atteindre −16,8 % ainsi que des phases de forte expansion culminant à +24,5 %. Ces fluctuations traduisent une sensibilité importante de l’activité aux conditions économiques ou commerciales.

-- 2. PERFORMANCE PAR PRODUIT, CATÉGORIE ET RÉGION
-- 2.1 Top 10 produits les plus commentés
SELECT 
    p.product_id,
    p.category_lvl1 AS categorie,
    COUNT(DISTINCT p.reviewer_id) AS nb_avis,
    AVG(p.rating) AS note_moyenne,
FROM product_reviews_clean p
GROUP BY p.product_id, p.category_lvl1
ORDER BY nb_avis DESC
LIMIT 10;
--Cette analyse met en évidence les produits ayant généré le plus d’avis clients.Ces produits sont probablement très populaires ou très diffusés, mais cela ne reflète pas directement leur volume de ventes.

-- 2.2 Performance par catégorie de produit (perception client)
SELECT 
    category_lvl1 AS categorie,
    COUNT(DISTINCT product_id) AS nb_produits,
    COUNT(*) AS nb_avis_total,
    AVG(rating) AS note_moyenne,
    COUNT(DISTINCT reviewer_id) AS nb_clients_uniques,
    SUM(helpful_total) AS total_votes_utilite
FROM product_reviews_clean
GROUP BY category_lvl1
ORDER BY nb_avis_total DESC;
--Les catégories Plant-based Milk Alternatives et Cold-pressed Juices concentrent le plus d’avis, signe d’une forte visibilité auprès des clients.Cette analyse met en évidence que popularité et satisfaction ne sont pas toujours alignées, ce qui justifie une analyse complémentaire basée sur les ventes. Certaines catégories comme Gluten-free Crackers ou Ready-to-eat Organic Salads affichent toutefois une meilleure satisfaction moyenne malgré un volume plus faible.


-- 2.3 Performance des ventes par région
SELECT 
    region,
    COUNT(*) AS nb_transactions,
    SUM(amount) AS total_ventes,
    AVG(amount) AS montant_moyen,
    MIN(amount) AS vente_min,
    MAX(amount) AS vente_max
FROM financial_transactions_clean
WHERE transaction_type = 'Sale'
GROUP BY region
ORDER BY total_ventes DESC;
--Les ventes sont relativement bien réparties entre les régions, avec toutefois une légère domination de l’Amérique du Nord et de l’Amérique du Sud en termes de chiffre d’affaires total. L’Europe et l’Océanie suivent de près, tandis que l’Afrique présente un volume légèrement inférieur. Le panier moyen reste globalement homogène entre les régions.

--Les transactions financières ne sont pas reliées aux produits dans le modèle de données disponible. Il est donc impossible de calculer le chiffre d’affaires par catégorie ou de croiser ventes et avis clients à ce stade.

-- 3. RÉPARTITION DES CLIENTS PAR SEGMENTS DÉMOGRAPHIQUES
--Répartition par genre
SELECT 
    gender AS genre,
    COUNT(*) AS nb_clients,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 2) AS pourcentage,
    AVG(annual_income) AS revenu_moyen,
    MIN(annual_income) AS revenu_min,
    MAX(annual_income) AS revenu_max
FROM customer_demographics_clean
GROUP BY gender
ORDER BY nb_clients DESC;
--La répartition par genre est particulièrement équilibrée, chaque segment représentant environ un tiers de la clientèle. Les revenus moyens sont très proches entre les groupes, ce qui suggère une structure socio-économique homogène et l’absence de segment dominant en termes de pouvoir d’achat.

-- 3.2 Répartition par région géographique
SELECT 
    region,
    COUNT(*) AS nb_clients,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 2) AS pourcentage,
    AVG(annual_income) AS revenu_moyen,
    COUNT(DISTINCT country) AS nb_pays
FROM customer_demographics_clean
GROUP BY region
ORDER BY nb_clients DESC;
--La clientèle est répartie de manière très équilibrée entre les régions (≈14 % chacune). South America et Asia concentrent légèrement plus de clients, mais les écarts restent faibles. Les revenus moyens sont homogènes d’une région à l’autre, suggérant une base client comparable en termes de pouvoir d’achat.

-- 3.3 Répartition par pays (Top 10)
SELECT 
    country AS pays,
    region,
    COUNT(*) AS nb_clients,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 2) AS pourcentage,
    AVG(annual_income) AS revenu_moyen
FROM customer_demographics_clean
GROUP BY country, region
ORDER BY nb_clients DESC
LIMIT 10;
--Le Top 10 des pays représente chacun entre 3 % et 4 % de la clientèle totale, indiquant une forte dispersion géographique sans concentration majeure. Les pays les plus représentés se situent principalement en Amérique du Nord et en Océanie. Les revenus moyens restent relativement homogènes entre pays, confirmant une structure socio-économique similaire à l’échelle internationale.

-- 3.4 Répartition par statut marital
SELECT 
    marital_status AS statut_marital,
    COUNT(*) AS nb_clients,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 2) AS pourcentage,
    AVG(annual_income) AS revenu_moyen
FROM customer_demographics_clean
GROUP BY marital_status
ORDER BY nb_clients DESC;
--La clientèle est répartie de manière homogène entre les différents statuts maritaux, chacun représentant environ un quart des clients. Les revenus moyens sont très proches d’un groupe à l’autre, suggérant que le statut marital n’est pas un facteur discriminant majeur en termes de pouvoir d’achat.

-- 3.5 Segmentation par tranche de revenus
SELECT 
    CASE 
        WHEN annual_income < 50000 THEN '0-50K'
        WHEN annual_income < 100000 THEN '50K-100K'
        WHEN annual_income < 150000 THEN '100K-150K'
        WHEN annual_income < 200000 THEN '150K-200K'
        ELSE '200K+'
    END AS tranche_revenus,
    COUNT(*) AS nb_clients,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 2) AS pourcentage,
    AVG(annual_income) AS revenu_moyen,
    MIN(annual_income) AS revenu_min,
    MAX(annual_income) AS revenu_max
FROM customer_demographics_clean
GROUP BY 
    CASE 
        WHEN annual_income < 50000 THEN '0-50K'
        WHEN annual_income < 100000 THEN '50K-100K'
        WHEN annual_income < 150000 THEN '100K-150K'
        WHEN annual_income < 200000 THEN '150K-200K'
        ELSE '200K+'
    END
ORDER BY revenu_moyen;
--La clientèle se concentre majoritairement dans les tranches de revenus intermédiaires et élevées, avec plus de 80 % des clients gagnant entre 50K et 200K par an. La tranche 0–50K reste minoritaire, ce qui suggère un positionnement orienté vers des consommateurs à pouvoir d’achat moyen à élevé.

-- 3.6 Segmentation par tranche d'âge
SELECT 
    CASE 
        WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE()) < 25 THEN '18-24'
        WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE()) < 35 THEN '25-34'
        WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE()) < 45 THEN '35-44'
        WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE()) < 55 THEN '45-54'
        WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE()) < 65 THEN '55-64'
        ELSE '65+'
    END AS tranche_age,
    COUNT(*) AS nb_clients,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 2) AS pourcentage,
    AVG(annual_income) AS revenu_moyen
FROM customer_demographics_clean
GROUP BY 
    CASE 
        WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE()) < 25 THEN '18-24'
        WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE()) < 35 THEN '25-34'
        WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE()) < 45 THEN '35-44'
        WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE()) < 55 THEN '45-54'
        WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE()) < 65 THEN '55-64'
        ELSE '65+'
    END
ORDER BY tranche_age;
--La population clients est majoritairement composée de profils âgés, avec une forte représentation des 65 ans et plus (28 %). Les autres tranches d’âge adultes sont relativement bien réparties, tandis que les 18–24 ans restent minoritaires. Le revenu moyen est homogène entre les groupes d’âge, suggérant que le pouvoir d’achat ne dépend pas fortement de l’âge dans cet échantillon.

-- 3. EXPÉRIENCE CLIENT
--Impact des avis produits sur les ventes (analyse par catégorie)
WITH avis_par_categorie AS (
    SELECT 
        category_lvl1 AS categorie,
        COUNT(*) AS nb_avis,
        AVG(rating) AS note_moyenne,
        COUNT(DISTINCT product_id) AS nb_produits,
        SUM(helpful_total) AS total_votes_utilite
    FROM product_reviews_clean
    GROUP BY category_lvl1
)
SELECT 
    categorie,
    nb_avis,
    nb_produits,
    ROUND(nb_avis::FLOAT / NULLIF(nb_produits, 0), 2) AS avis_par_produit,
    ROUND(note_moyenne, 2) AS note_moyenne,
    total_votes_utilite,
    CASE 
        WHEN note_moyenne >= 4.5 THEN 'Excellent'
        WHEN note_moyenne >= 4.0 THEN 'Très bon'
        WHEN note_moyenne >= 3.5 THEN 'Bon'
        WHEN note_moyenne >= 3.0 THEN 'Moyen'
        ELSE 'Faible'
    END AS perception_qualite
FROM avis_par_categorie
ORDER BY note_moyenne DESC;

--Certaines catégories comme Ready-to-eat Organic Salads et Gluten-free Crackers affichent des notes moyennes élevées tout en générant un volume important d’avis, suggérant une forte satisfaction client sur des produits largement diffusés. À l’inverse, Cold-pressed Juices concentre beaucoup d’avis mais avec une note plus modérée, indiquant une popularité élevée accompagnée d’une perception plus mitigée.


--Produits les mieux notés vs les moins bien notés
(
    SELECT 
        'Top 10 - Mieux notés' AS classement,
        product_id,
        category_lvl1 AS categorie,
        COUNT(*) AS nb_avis,
        AVG(rating) AS note_moyenne,
        SUM(helpful_total) AS votes_utilite
    FROM product_reviews_clean
    GROUP BY product_id, category_lvl1
    HAVING COUNT(*) >= 5
    ORDER BY note_moyenne DESC, nb_avis DESC
    LIMIT 10
)
UNION ALL
(
    SELECT 
        'Top 10 - Moins bien notés' AS classement,
        product_id,
        category_lvl1 AS categorie,
        COUNT(*) AS nb_avis,
        AVG(rating) AS note_moyenne,
        SUM(helpful_total) AS votes_utilite
    FROM product_reviews_clean
    GROUP BY product_id, category_lvl1
    HAVING COUNT(*) >= 5
    ORDER BY note_moyenne ASC, nb_avis DESC
    LIMIT 10
);
--Cette requête identifie les produits les mieux et les moins bien évalués, afin de repérer les références à fort potentiel et celles présentant des signaux d’alerte côté satisfaction client.

--Influence des interactions service client
SELECT 
    interaction_type,
    issue_category,
    COUNT(*) AS nb_interactions,
    AVG(duration_minutes) AS duree_moyenne,
    AVG(customer_satisfaction) AS satisfaction_moyenne,
    COUNT(CASE WHEN resolution_status = 'Resolved' THEN 1 END) AS nb_resolus,
    ROUND((COUNT(CASE WHEN resolution_status = 'Resolved' THEN 1 END) * 100.0 / COUNT(*)), 2) AS taux_resolution,
    COUNT(CASE WHEN follow_up_required = TRUE THEN 1 END) AS nb_suivi_requis
FROM customer_service_interactions_clean
GROUP BY interaction_type, issue_category
ORDER BY nb_interactions DESC;
--Cette analyse met en évidence les types d’interactions et motifs de contact les plus fréquents avec le service client, ainsi que leur impact sur la satisfaction et le taux de résolution. Elle permet d’identifier les canaux et problématiques générant le plus de volume ainsi que ceux nécessitant des améliorations opérationnelles.

--Évolution de la satisfaction client dans le temps
SELECT 
    DATE_TRUNC('QUARTER', interaction_date) AS trimestre,
    COUNT(*) AS nb_interactions,
    AVG(customer_satisfaction) AS satisfaction_moyenne,
    AVG(duration_minutes) AS duree_moyenne,
    COUNT(DISTINCT issue_category) AS nb_types_problemes,
    ROUND((COUNT(CASE WHEN resolution_status = 'Resolved' THEN 1 END) * 100.0 / COUNT(*)), 2) AS taux_resolution
FROM customer_service_interactions_clean
GROUP BY DATE_TRUNC('QUARTER', interaction_date)
ORDER BY trimestre;

WITH ventes_par_categorie AS (
    SELECT 
        region,
        SUM(amount) AS total_ventes
    FROM financial_transactions_clean
    WHERE transaction_type = 'Sale'
    GROUP BY region
),
avis_par_categorie AS (
    SELECT 
        category_lvl1 AS categorie,
        AVG(rating) AS note_moyenne,
        COUNT(*) AS nb_avis
    FROM product_reviews_clean
    GROUP BY category_lvl1
)
SELECT 
    a.categorie,
    a.note_moyenne,
    a.nb_avis
FROM avis_par_categorie a
ORDER BY note_moyenne DESC;
--Certaines catégories combinent forte satisfaction et volume d’avis élevé, tandis que d’autres restent très visibles mais moins bien perçues.