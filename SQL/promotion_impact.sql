-- PARTIE 2.3 - ANALYSES BUSINESS TRANSVERSES
-- 1. VENTES ET PROMOTIONS

-- Comparaison ventes avec/sans promotion (par période)
WITH ventes_par_date AS (
    SELECT 
        transaction_date,
        SUM(amount) AS total_ventes,
        COUNT(*) AS nb_transactions
    FROM financial_transactions_clean
    WHERE transaction_type = 'Sale'
    GROUP BY transaction_date
),
promotions_actives AS (
    SELECT DISTINCT
        d.date_value,
        COUNT(DISTINCT p.promotion_id) AS nb_promotions_actives
    FROM (
        SELECT DATEADD(DAY, SEQ4(), '2010-01-01'::DATE) AS date_value
        FROM TABLE(GENERATOR(ROWCOUNT => 5500))
    ) d
    LEFT JOIN promotions_data_clean p
        ON d.date_value BETWEEN p.start_date AND p.end_date
    GROUP BY d.date_value
)
SELECT 
    CASE 
        WHEN pa.nb_promotions_actives > 0 THEN 'Avec promotion'
        ELSE 'Sans promotion'
    END AS avec_promotion,
    COUNT(DISTINCT v.transaction_date) AS nb_jours,
    SUM(v.nb_transactions) AS total_transactions,
    SUM(v.total_ventes) AS total_ventes,
    AVG(v.total_ventes) AS ventes_moyennes_par_jour,
    AVG(v.nb_transactions) AS transactions_moyennes_par_jour
FROM ventes_par_date v
LEFT JOIN promotions_actives pa ON v.transaction_date = pa.date_value
GROUP BY 
    CASE 
        WHEN pa.nb_promotions_actives > 0 THEN 'Avec promotion'
        ELSE 'Sans promotion'
    END;
/*La comparaison entre les périodes avec et sans promotion ne met pas en évidence d’augmentation significative des ventes journalières lors des 
campagnes promotionnelles. Le chiffre d’affaires moyen par jour ainsi que le volume de transactions apparaissent légèrement supérieurs 
hors promotion, suggérant une efficacité limitée des promotions dans leur forme actuelle.*/

-- Impact des promotions par mois
WITH ventes_mensuelles AS (
    SELECT 
        DATE_TRUNC('MONTH', transaction_date) AS mois,
        SUM(amount) AS total_ventes
    FROM financial_transactions_clean
    WHERE transaction_type = 'Sale'
    GROUP BY DATE_TRUNC('MONTH', transaction_date)
),
promotions_mensuelles AS (
    SELECT 
        DATE_TRUNC('MONTH', start_date) AS mois,
        COUNT(DISTINCT promotion_id) AS nb_promotions,
        AVG(discount_percentage) AS discount_moyen
    FROM promotions_data_clean
    GROUP BY DATE_TRUNC('MONTH', start_date)
)
SELECT 
    v.mois,
    v.total_ventes,
    COALESCE(p.nb_promotions, 0) AS nb_promotions,
    COALESCE(p.discount_moyen, 0) AS discount_moyen
FROM ventes_mensuelles v
LEFT JOIN promotions_mensuelles p ON v.mois = p.mois
ORDER BY v.mois;
/*L’analyse mensuelle croisant chiffre d’affaires et activité promotionnelle ne met pas en évidence de relation directe entre le nombre de 
promotions ou le niveau de remise et le volume des ventes. Plusieurs mois sans promotion présentent des performances comparables, voire 
supérieures, à des périodes promotionnelles actives, suggérant un impact global limité des campagnes promotionnelles sur le chiffre d’affaires.*/

--Analyse promotions par catégorie
SELECT
    product_category AS categorie,
    COUNT(DISTINCT promotion_id) AS nb_promotions,
    AVG(discount_percentage) AS discount_moyen,
    MIN(discount_percentage) AS discount_min,
    MAX(discount_percentage) AS discount_max,
    COUNT(DISTINCT region) AS nb_regions_couvertes
FROM promotions_data_clean
GROUP BY product_category
ORDER BY nb_promotions DESC;
/*L’analyse des promotions par catégorie montre que Organic Meal Solutions concentre le plus grand nombre de campagnes, avec une couverture 
géographique étendue et des niveaux de remise moyens autour de 15 %. Organic Beverages fait également l’objet de nombreuses opérations promotionnelles, 
tandis que Organic Snacks est moins fréquemment promu mais bénéficie de remises légèrement plus élevées. Ces résultats suggèrent une priorisation 
stratégique de certaines familles de produits dans les actions commerciales, indépendamment de leur volume réel de ventes, qui ne peut être 
évalué faute de lien direct entre promotions et transactions.*/
