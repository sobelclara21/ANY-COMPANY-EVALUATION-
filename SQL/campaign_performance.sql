-- 2. MARKETING ET PERFORMANCE COMMERCIALE
-- Lien campagnes & ventes (analyse temporelle)
WITH ventes_mensuelles AS (
  SELECT
    DATE_TRUNC('MONTH', transaction_date) AS mois,
    SUM(amount) AS total_ventes,
    COUNT(*) AS nb_transactions
  FROM SILVER.financial_transactions_clean
  WHERE transaction_type = 'Sale'
  GROUP BY 1
),

mois AS (
  SELECT DATEADD(MONTH, SEQ4(), '2010-01-01'::DATE) AS mois
  FROM TABLE(GENERATOR(ROWCOUNT => 2000)) 
),

campagnes_actives_mensuelles AS (
  SELECT
    m.mois,
    COUNT(DISTINCT c.campaign_id) AS nb_campagnes_actives,
    SUM(c.budget) AS budget_total_actif,
    SUM(c.reach) AS reach_total_actif,
    AVG(c.conversion_rate) AS conversion_moyenne_actif
  FROM mois m
  LEFT JOIN SILVER.marketing_campaigns_clean c
    ON m.mois BETWEEN DATE_TRUNC('MONTH', c.start_date) AND DATE_TRUNC('MONTH', c.end_date)
  GROUP BY 1
)

SELECT
  v.mois,
  v.total_ventes,
  v.nb_transactions,
  COALESCE(ca.nb_campagnes_actives, 0) AS nb_campagnes_actives,
  COALESCE(ca.budget_total_actif, 0) AS budget_marketing_actif,
  COALESCE(ca.reach_total_actif, 0) AS reach_total_actif,
  COALESCE(ca.conversion_moyenne_actif, 0) AS conversion_moyenne_actif,
  CASE
    WHEN ca.budget_total_actif > 0 THEN v.total_ventes / ca.budget_total_actif
    ELSE NULL
  END AS ratio_ventes_sur_budget
FROM ventes_mensuelles v
LEFT JOIN campagnes_actives_mensuelles ca
  ON v.mois = ca.mois
ORDER BY v.mois;
/*L’analyse mensuelle montre que les périodes avec un nombre élevé de campagnes actives s’accompagnent généralement d’un budget marketing et d’un 
reach plus importants, sans pour autant entraîner mécaniquement une hausse proportionnelle des ventes.*/
/*Le ratio ventes / budget marketing reste globalement faible et très variable selon les mois, ce qui suggère que l’intensité marketing seule n’explique 
pas entièrement la performance commerciale. L’efficacité semble dépendre davantage de la qualité des campagnes que de leur volume.*/

-- 2.2 Identification des campagnes les plus efficaces
SELECT
  campaign_id,
  campaign_name,
  campaign_type,
  product_category,
  region,
  budget,
  reach,
  conversion_rate,
  ROUND(budget / NULLIF(reach, 0), 2) AS cout_par_personne_atteinte,
  ROUND(reach * conversion_rate, 0) AS conversions_estimees,
  ROUND(budget / NULLIF(reach * conversion_rate, 0), 2) AS cout_par_conversion
FROM SILVER.marketing_campaigns_clean
WHERE reach > 0
  AND conversion_rate > 0
  AND budget > 0
ORDER BY cout_par_conversion ASC, conversions_estimees DESC
LIMIT 20;
/*Cette requête identifie les campagnes ayant le meilleur coût par conversion, tout en tenant compte du volume de conversions estimées. Elle permet de 
repérer les actions marketing les plus efficientes en termes de rentabilité et d’orienter les futurs investissements.*/

-- 2.3 Performance par type de campagne
SELECT
  campaign_type,
  COUNT(*) AS nb_campagnes,
  SUM(budget) AS budget_total,
  AVG(budget) AS budget_moyen,
  SUM(reach) AS reach_total,
  AVG(reach) AS reach_moyen,
  AVG(conversion_rate) AS conversion_moyenne,
  ROUND(SUM(reach * conversion_rate) / NULLIF(SUM(reach), 0), 4) AS conversion_ponderee_reach,
  ROUND(SUM(budget) / NULLIF(SUM(reach), 0), 2) AS cout_moyen_par_personne,
  ROUND(SUM(budget) / NULLIF(SUM(reach * conversion_rate), 0), 2) AS cout_par_conversion_global,
  COUNT(DISTINCT product_category) AS nb_categories,
  COUNT(DISTINCT region) AS nb_regions
FROM SILVER.marketing_campaigns_clean
GROUP BY campaign_type
ORDER BY cout_par_conversion_global ASC NULLS LAST;
/*Les performances sont relativement homogènes entre les différents types de campagnes, tant en termes de taux de conversion que de coût par conversion. 
Aucun levier ne se démarque fortement, ce qui suggère une efficacité comparable des canaux marketing à ce stade de l’analyse.*/

--Performance par catégorie de produit (marketing) 
SELECT
  product_category,
  COUNT(*) AS nb_campagnes,
  SUM(budget) AS budget_total,
  SUM(reach) AS reach_total,
  AVG(conversion_rate) AS conversion_moyenne,
  SUM(reach * conversion_rate) AS conversions_estimees_total,
  ROUND(SUM(budget) / NULLIF(SUM(reach * conversion_rate), 0), 2) AS cout_par_conversion
FROM SILVER.marketing_campaigns_clean
GROUP BY product_category
ORDER BY cout_par_conversion ASC NULLS LAST;
/*Les performances marketing sont globalement proches entre les catégories de produits. Clothing et Baby Food présentent les coûts par conversion les 
plus faibles, tandis que Household et Electronics apparaissent légèrement moins rentables. Aucun écart majeur ne se dégage à ce stade.*/

-- Sensibilité des catégories produits aux promotions (category_lvl2)

SELECT
    pr.category_lvl2 AS categorie_lvl2,
    COUNT(DISTINCT p.promotion_id) AS nb_promotions,
    AVG(p.discount_percentage) AS discount_moyen,
    MIN(p.discount_percentage) AS discount_min,
    MAX(p.discount_percentage) AS discount_max,
    COUNT(DISTINCT p.region) AS nb_regions,
    AVG(DATEDIFF(DAY, p.start_date, p.end_date)) AS duree_moyenne_jours
FROM SILVER.promotions_data_clean p
JOIN SILVER.product_reviews_clean pr
    ON p.product_category = pr.category_lvl2
GROUP BY pr.category_lvl2
ORDER BY nb_promotions DESC;
/*Les promotions se concentrent principalement sur Organic Meal Solutions, suivie de Organic Beverages et Organic Snacks. Les niveaux de remise 
sont proches entre catégories (autour de 14–16 %), mais les campagnes sur les repas préparés durent plus longtemps et couvrent davantage de régions, 
traduisant un effort marketing prioritaire sur ce segment.*/

-- 4. OPÉRATIONS ET LOGISTIQUE

--4.1 Stock global : 
SELECT
  COUNT(*) AS nb_produits_total,
  COUNT(CASE WHEN current_stock <= 0 THEN 1 END) AS nb_ruptures_totales,
  COUNT(CASE WHEN current_stock > 0 AND current_stock <= reorder_point THEN 1 END) AS nb_stock_faible,
  ROUND(COUNT(CASE WHEN current_stock <= 0 THEN 1 END) * 100.0 / COUNT(*), 2) AS taux_rupture_totale_pct,
  ROUND(COUNT(CASE WHEN current_stock > 0 AND current_stock <= reorder_point THEN 1 END) * 100.0 / COUNT(*), 2) AS taux_stock_faible_pct
FROM SILVER.inventory_clean;
/*Aucun produit n’est totalement en rupture, mais près de 2 % présentent un stock faible, signalant un risque de tension à anticiper.*/

--4.2 Liste des produits en risque (stock faible + rupture)
SELECT 
  product_id,
  product_category,
  warehouse,
  region,
  country,
  current_stock,
  reorder_point,
  current_stock - reorder_point AS stock_disponible,
  CASE 
    WHEN current_stock <= 0 THEN 'Rupture totale'
    WHEN current_stock <= reorder_point THEN 'Stock faible'
    ELSE 'Stock OK'
  END AS statut_stock,
  lead_time AS delai_reappro_jours,
  last_restock_date,
  DATEDIFF(DAY, last_restock_date, CURRENT_DATE()) AS jours_depuis_dernier_restock
FROM SILVER.inventory_clean
WHERE current_stock <= reorder_point
ORDER BY statut_stock, stock_disponible ASC;
/*Les produits en stock faible concernent plusieurs catégories (boissons, baby food, household…) et différentes régions. Certains articles présentent 
un écart important entre stock actuel et seuil de réapprovisionnement, combiné à des délais logistiques élevés, ce qui augmente le risque opérationnel 
à court terme.*/

--4.3 Ruptures / stock faible par catégorie et région
SELECT 
  product_category,
  region,
  COUNT(*) AS nb_produits,
  COUNT(CASE WHEN current_stock <= 0 THEN 1 END) AS nb_ruptures_totales,
  COUNT(CASE WHEN current_stock > 0 AND current_stock <= reorder_point THEN 1 END) AS nb_stock_faible,
  ROUND(COUNT(CASE WHEN current_stock <= 0 THEN 1 END) * 100.0 / COUNT(*), 2) AS taux_rupture_totale_pct
FROM SILVER.inventory_clean
GROUP BY product_category, region
ORDER BY taux_rupture_totale_pct DESC, nb_stock_faible DESC;
/*Aucune combinaison catégorie–région ne présente de rupture totale.En revanche, plusieurs segments affichent des cas de stock faible, notamment dans les 
catégories Beverages, Baby Food et Snacks en Afrique et Océanie. Ces zones constituent des points de vigilance opérationnelle à court terme.*/

--4.4 Délais “promis” + taux de retour par méthode d’expédition
SELECT
  shipping_method,
  COUNT(*) AS nb_envois,
  AVG(shipping_cost) AS cout_moyen,
  AVG(DATEDIFF(DAY, ship_date, estimated_delivery)) AS delai_moyen_jours,
  MIN(DATEDIFF(DAY, ship_date, estimated_delivery)) AS delai_min_jours,
  MAX(DATEDIFF(DAY, ship_date, estimated_delivery)) AS delai_max_jours,
  ROUND(COUNT(CASE WHEN status = 'Returned' THEN 1 END) * 100.0 / COUNT(*), 2) AS taux_retour_pct
FROM SILVER.logistics_and_shipping_clean
GROUP BY shipping_method
ORDER BY nb_envois DESC;
/*Les différentes méthodes d’expédition présentent des délais moyens très similaires (environ 7,5 jours) et des coûts proches. Les taux de retour sont également
 comparables, autour de 20–22 %, sans différence majeure entre Standard, Express, Next Day et International.*/

--4.5 Transporteurs
SELECT
    carrier,
    COUNT(*) AS nb_envois,
    AVG(shipping_cost) AS cout_moyen,
    ROUND(COUNT(CASE WHEN status = 'Delivered' THEN 1 END) * 100.0 / COUNT(*), 2) AS taux_livraison_pct,
    ROUND(COUNT(CASE WHEN status = 'Returned' THEN 1 END) * 100.0 / COUNT(*), 2) AS taux_retour_pct,
    COUNT(DISTINCT destination_region) AS nb_regions_couvertes
FROM SILVER.logistics_and_shipping_clean
GROUP BY carrier
ORDER BY nb_envois DESC;
/*Les volumes par transporteur sont faibles et très dispersés, ce qui rend les taux de livraison et de retour peu stables. Aucun acteur ne se démarque 
clairement pour l’instant, les performances semblant hétérogènes et dépendantes de petits volumes d’expédition.*/

--4.6 Retours par région et méthode
SELECT 
  destination_region,
  shipping_method,
  COUNT(*) AS nb_envois,
  COUNT(CASE WHEN status = 'Returned' THEN 1 END) AS nb_retours,
  ROUND(COUNT(CASE WHEN status = 'Returned' THEN 1 END) * 100.0 / COUNT(*), 2) AS taux_retour_pct,
  AVG(shipping_cost) AS cout_moyen
FROM SILVER.logistics_and_shipping_clean
GROUP BY destination_region, shipping_method
HAVING COUNT(*) >= 10
ORDER BY taux_retour_pct DESC, nb_envois DESC;
/*Les taux de retour par région et méthode d’expédition varient environ entre 15 % et 25 %. Aucune méthode ne se démarque 
nettement : Standard, Express, Next Day et International présentent des niveaux proches selon les zones. Certaines régions 
comme l’Europe ou l’Océanie apparaissent plus exposées, mais l’ensemble reste relativement homogène.*/
