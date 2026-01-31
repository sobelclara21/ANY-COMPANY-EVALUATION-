# Projet Data Analytics – AnyCompany Food & Beverage

## Contexte du projet

Ce projet s'inscrit dans le cadre du cours d'Architecture Big Data du MBA ESG.  
Il vise à appliquer les compétences en analyse de données et business intelligence sur un cas d'entreprise réaliste.

AnyCompany Food & Beverage est une entreprise fictive du secteur agroalimentaire, présente sur le marché depuis plus de 25 ans.  
Elle distribue des produits alimentaires et de boissons premium à l'international.

## Problématique

L'entreprise fait face à une situation critique :

    - Part de marché en chute : passage de 28% à 22% en seulement 8 mois (perte de 6 points) 
    - Budget marketing réduit : baisse de 30% des ressources disponibles  
    - Concurrence agressive : arrivée de marques digitales proposant des prix inférieurs de 5 à 15%

Face à ces défis, le PDG a lancé une initiative de transformation digitale en confiant à Sarah (Senior Marketing Executive) la mission de piloter une stratégie data-driven.

## Objectif

Reconquérir 10 points de part de marché d'ici T4 2025 (passer de 22% à 32%), dans un contexte de budget contraint.

Pour y parvenir, l'équipe doit :

    - Analyser les performances actuelles (ventes, promotions, marketing)  
    - Identifier les segments et produits à fort potentiel  
    - Optimiser l'allocation du budget marketing  
    - Proposer un plan d'action chiffré et opérationnel

## Infrastructure

    - Snowflake : Plateforme cloud pour le stockage et l'analyse des données  
    - Amazon S3 : Stockage des fichiers sources (s3://logbrain-datalake/datasets/food-beverage/)  
    - Streamlit : Dashboards interactifs pour la visualisation

## Méthodologie

Phase 1 : Préparation des données (ETL - Bronze)

    - Création de la base de données FOOD_BEVERAGE_LAB  
    - Création des schémas BRONZE, SILVER et ANALYTICS  
    - Création du stage S3 pointant vers le datalake  
    - Définition des file formats (CSV avec délimiteur, JSON)  
    - Création des tables dans le schéma BRONZE  
    - Chargement des données avec COPY INTO  
    - Vérification des volumes chargés

Phase 2 : Nettoyage des données (Silver)

    - Analyse de la qualité (valeurs nulles, doublons, formats)  
    - Identification des clés primaires et des anomalies  
    - Déduplication avec QUALIFY et ROW_NUMBER()  
    - Validation des contraintes métier (dates cohérentes, montants positifs)  
    - Création des tables SILVER nettoyées  
    - Contrôles de cohérence entre BRONZE et SILVER

Phase 3 : Modélisation analytique (Analytics)

    - Identification des besoins métier  
    - Création de SALES_ENRICHED (ventes + promotions actives)  
    - Création de PROMOTIONS_ANALYTICS (KPIs par promotion)  
    - Création de CUSTOMERS_ENRICHED (segmentation clients)  
    - Documentation des tables avec COMMENT ON

## Difficultés techniques rencontrées

1. Parsing du fichier product_reviews.csv

    - Problème : Caractères spéciaux et retours à la ligne dans les commentaires provoquaient des erreurs de parsing avec le format CSV standard.
    - Solution : Chargement en mode texte brut (FIELD_DELIMITER = 'NONE') dans une table temporaire, puis parsing manuel avec SPLIT_PART() sur le délimiteur tab.

2. Doublons dans les tables sources
    - Problème : Plusieurs tables contenaient des doublons 
    - Solution : Utilisation de QUALIFY avec ROW_NUMBER() OVER (PARTITION BY clé ORDER BY critères prioritaires) = 1 pour ne garder qu'une occurrence en priorisant les données les plus récentes ou complètes.

3. Alignement temporel ventes/promotions

    - Problème : Complexité pour identifier quelles promotions étaient actives au moment exact de chaque transaction car aucune clé jointure d’identifier la transaction avec la promotion avec le produit avec le client.
    - Solution : Jointure conditionnelle avec transaction_date BETWEEN start_date AND end_date ET correspondance de la région. Utilisation de LISTAGG pour agréger les multiples promotions actives.

4. Formatage des dates dans Streamlit

    - Problème : Erreurs SQL lors de la construction dynamique des clauses WHERE avec les dates sélectionnées.
    - Solution : Fonction utilitaire escape_sql_string() et formatage explicite avec TO_DATE(date_str, 'YYYY-MM-DD').
