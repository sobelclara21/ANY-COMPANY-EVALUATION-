# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import altair as alt
from datetime import datetime

# Get the current credentials
session = get_active_session()

st.set_page_config(
    page_title="Promotion Analytics",
    page_icon="🎁",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============= FONCTION UTILITAIRE POUR SQL SÉCURISÉ =============
def escape_sql_string(value):
    """Échappe les apostrophes pour éviter les erreurs SQL"""
    if value is None:
        return ""
    return str(value).replace("'", "''")

def build_where_clause(date_debut, date_fin, use_all_dates, selected_regions, selected_categories, selected_types):
    """Construit une clause WHERE sécurisée"""
    where_conditions = []
    
    # Filtre de dates uniquement si pas "toutes les dates"
    if not use_all_dates:
        # Conversion en format YYYY-MM-DD
        date_debut_str = date_debut.strftime('%Y-%m-%d') if hasattr(date_debut, 'strftime') else str(date_debut)
        date_fin_str = date_fin.strftime('%Y-%m-%d') if hasattr(date_fin, 'strftime') else str(date_fin)
        where_conditions.append(f"date_debut >= TO_DATE('{date_debut_str}', 'YYYY-MM-DD')")
        where_conditions.append(f"date_fin <= TO_DATE('{date_fin_str}', 'YYYY-MM-DD')")
    
    # Filtre régions
    if selected_regions and len(selected_regions) > 0:
        regions_escaped = [escape_sql_string(r) for r in selected_regions]
        regions_str = "','".join(regions_escaped)
        where_conditions.append(f"region IN ('{regions_str}')")
    elif selected_regions is not None and len(selected_regions) == 0:
        # Aucune région sélectionnée = pas de résultat
        where_conditions.append("1=0")
    
    # Filtre catégories
    if selected_categories and len(selected_categories) > 0:
        cats_escaped = [escape_sql_string(c) for c in selected_categories]
        cats_str = "','".join(cats_escaped)
        where_conditions.append(f"categorie_produit IN ('{cats_str}')")
    elif selected_categories is not None and len(selected_categories) == 0:
        where_conditions.append("1=0")
    
    # Filtre types
    if selected_types and len(selected_types) > 0:
        types_escaped = [escape_sql_string(t) for t in selected_types]
        types_str = "','".join(types_escaped)
        where_conditions.append(f"type_promotion IN ('{types_str}')")
    elif selected_types is not None and len(selected_types) == 0:
        where_conditions.append("1=0")
    
    # Construction finale
    if where_conditions:
        return "WHERE " + " AND ".join(where_conditions)
    else:
        return ""

# CSS personnalisé
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #e74c3c;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# En-tête
st.markdown('<p class="main-header">🎁 Promotion Performance Analysis</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Analyse détaillée de l\'efficacité des campagnes promotionnelles</p>', unsafe_allow_html=True)

# ============= SIDEBAR - FILTRES =============
st.sidebar.header("🎯 Filtres de Sélection")

# Récupération du VRAI nombre total de promotions (SANS AUCUN FILTRE)
try:
    total_promos_base = session.sql("""
        SELECT COUNT(*) as total 
        FROM ANALYTICS.promotions_analytics
    """).collect()[0]['TOTAL']
    
    st.sidebar.success(f"✅ **{total_promos_base} promotions** au total dans la base")
except Exception as e:
    st.sidebar.error(f"❌ Erreur de connexion à la base : {str(e)}")
    st.stop()

# Filtre de dates
try:
    date_range = session.sql("""
        SELECT 
            MIN(date_debut) as min_date,
            MAX(date_fin) as max_date
        FROM ANALYTICS.promotions_analytics
    """).collect()[0]
    
    min_date = date_range['MIN_DATE']
    max_date = date_range['MAX_DATE']
except Exception as e:
    st.sidebar.error(f"❌ Erreur lors de la récupération des dates : {str(e)}")
    st.stop()

st.sidebar.subheader("📅 Période des promotions")

# Option "Toutes les dates" par défaut
use_all_dates = st.sidebar.checkbox("Utiliser toutes les dates", value=True)

if use_all_dates:
    date_debut = min_date
    date_fin = max_date
    st.sidebar.info(f"✅ Toutes les dates : {min_date} → {max_date}")
else:
    date_debut = st.sidebar.date_input(
        "Date de début",
        value=min_date,
        min_value=min_date,
        max_value=max_date
    )
    
    date_fin = st.sidebar.date_input(
        "Date de fin",
        value=max_date,
        min_value=min_date,
        max_value=max_date
    )

# Filtre régions - TOUTES PAR DÉFAUT
st.sidebar.subheader("🌍 Régions")
try:
    regions = session.sql("""
        SELECT DISTINCT region 
        FROM ANALYTICS.promotions_analytics 
        WHERE region IS NOT NULL
        ORDER BY region
    """).to_pandas()
    
    selected_regions = st.sidebar.multiselect(
        "Sélectionner les régions",
        options=regions['REGION'].tolist(),
        default=regions['REGION'].tolist()  # ✅ TOUTES sélectionnées
    )
except Exception as e:
    st.sidebar.error(f"Erreur régions : {str(e)}")
    selected_regions = []

# Filtre catégories - TOUTES par défaut
st.sidebar.subheader("🏷️ Catégories")
try:
    categories = session.sql("""
        SELECT DISTINCT categorie_produit 
        FROM ANALYTICS.promotions_analytics 
        WHERE categorie_produit IS NOT NULL
        ORDER BY categorie_produit
    """).to_pandas()
    
    selected_categories = st.sidebar.multiselect(
        "Catégories de produits",
        options=categories['CATEGORIE_PRODUIT'].tolist(),
        default=categories['CATEGORIE_PRODUIT'].tolist()  # ✅ TOUTES sélectionnées
    )
except Exception as e:
    st.sidebar.error(f"Erreur catégories : {str(e)}")
    selected_categories = []

# Filtre types - TOUS par défaut
st.sidebar.subheader("🎯 Types de promotion")
try:
    types = session.sql("""
        SELECT DISTINCT type_promotion 
        FROM ANALYTICS.promotions_analytics 
        WHERE type_promotion IS NOT NULL
        ORDER BY type_promotion
    """).to_pandas()
    
    # ✅ TOUS sélectionnés par défaut
    selected_types = st.sidebar.multiselect(
        "Types de promotion",
        options=types['TYPE_PROMOTION'].tolist(),
        default=types['TYPE_PROMOTION'].tolist()  # ✅ TOUS sélectionnés
    )
except Exception as e:
    st.sidebar.error(f"Erreur types : {str(e)}")
    selected_types = []

# Bouton pour réinitialiser
if st.sidebar.button("🔄 Réinitialiser tous les filtres"):
    st.rerun()

# Construction WHERE avec la fonction sécurisée
where_clause = build_where_clause(
    date_debut, date_fin, use_all_dates,
    selected_regions, selected_categories, selected_types
)

st.sidebar.markdown("---")
st.sidebar.info(f"📊 {len(selected_regions)} région(s) | {len(selected_categories)} catégorie(s) | {len(selected_types)} type(s)")

# Affichage debug (à commenter en prod)
with st.sidebar.expander("🔍 Debug SQL"):
    st.code(where_clause if where_clause else "Aucun filtre", language="sql")

# ============= KPIs =============
st.markdown("---")
st.subheader("📊 Indicateurs Clés des Promotions")

col1, col2, col3, col4 = st.columns(4)

# Total promotions APRÈS filtres
try:
    query = f"""
        SELECT COUNT(*) as total 
        FROM ANALYTICS.promotions_analytics
        {where_clause}
    """
    total_promos_filtrees = session.sql(query).collect()[0]['TOTAL']
except Exception as e:
    st.error(f"❌ Erreur SQL : {str(e)}")
    st.code(query, language="sql")
    st.stop()

# ✅ Affichage corrigé : filtré / total base
col1.metric(
    "🎁 Promotions Analysées", 
    f"{total_promos_filtrees:,}", 
    f"sur {total_promos_base:,} total"
)

# Discount moyen
try:
    avg_discount = session.sql(f"""
        SELECT AVG(taux_remise) * 100 as avg_discount
        FROM ANALYTICS.promotions_analytics
        {where_clause}
    """).collect()[0]['AVG_DISCOUNT']
    
    col2.metric("💰 Discount Moyen", f"{avg_discount:.1f}%" if avg_discount else "N/A")
except:
    col2.metric("💰 Discount Moyen", "N/A")

# Ventes pendant promos
try:
    total_sales_promo = session.sql(f"""
        SELECT COALESCE(SUM(montant_ventes_pendant_promo), 0) as total
        FROM ANALYTICS.promotions_analytics
        {where_clause}
    """).collect()[0]['TOTAL']
    
    col3.metric("💵 CA Promotions", f"${total_sales_promo:,.0f}")
except:
    col3.metric("💵 CA Promotions", "N/A")

# Durée moyenne
try:
    avg_duration = session.sql(f"""
        SELECT AVG(duree_jours) as avg_days
        FROM ANALYTICS.promotions_analytics
        {where_clause}
    """).collect()[0]['AVG_DAYS']
    
    col4.metric("⏱️ Durée Moyenne", f"{avg_duration:.0f} jours" if avg_duration else "N/A")
except:
    col4.metric("⏱️ Durée Moyenne", "N/A")

# Message d'alerte si trop peu de résultats
if total_promos_filtrees == 0:
    st.error("""
    ❌ **Aucune promotion** ne correspond aux filtres sélectionnés.
    - Vérifiez vos sélections dans la sidebar
    - Essayez de réinitialiser les filtres
    """)
    st.stop()

if total_promos_filtrees < 10:
    st.warning(f"""
    ⚠️ **Attention** : Seulement {total_promos_filtrees} promotions correspondent aux filtres.
    - Essayez d'élargir les filtres (régions, catégories, types)
    - Ou cochez "Utiliser toutes les dates" dans la sidebar
    """)

# ============= TOP PROMOTIONS =============
st.markdown("---")
st.subheader("🏆 Top 20 Promotions par Performance")

try:
    top_promos = session.sql(f"""
        SELECT 
            promotion_id,
            categorie_produit,
            type_promotion,
            region,
            ROUND(taux_remise * 100, 1) as discount_pct,
            montant_ventes_pendant_promo as ventes,
            nb_ventes_pendant_promo as transactions,
            panier_moyen_pendant_promo as panier_moyen,
            duree_jours,
            date_debut,
            date_fin
        FROM ANALYTICS.promotions_analytics
        {where_clause}
        ORDER BY montant_ventes_pendant_promo DESC
        LIMIT 20
    """).to_pandas()
    
    if len(top_promos) > 0:
        st.dataframe(
            top_promos.style.format({
                'DISCOUNT_PCT': '{:.1f}%',
                'VENTES': '${:,.0f}',
                'TRANSACTIONS': '{:,.0f}',
                'PANIER_MOYEN': '${:,.2f}',
                'DUREE_JOURS': '{:.0f}',
                'DATE_DEBUT': lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else '',
                'DATE_FIN': lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else ''
            }),
            use_container_width=True,
            height=400
        )
    else:
        st.warning("Aucune promotion ne correspond aux filtres sélectionnés")
except Exception as e:
    st.error(f"Erreur lors du chargement des top promotions : {str(e)}")

# ============= PERFORMANCE PAR CATEGORIE =============
st.markdown("---")
st.subheader("📦 Performance par Catégorie de Produit")

try:
    promo_by_category = session.sql(f"""
        SELECT 
            categorie_produit,
            COUNT(*) as nb_promotions,
            AVG(taux_remise) * 100 as discount_moyen,
            SUM(montant_ventes_pendant_promo) as total_ventes,
            SUM(nb_ventes_pendant_promo) as total_transactions,
            AVG(panier_moyen_pendant_promo) as panier_moyen,
            AVG(duree_jours) as duree_moyenne
        FROM ANALYTICS.promotions_analytics
        {where_clause}
        GROUP BY categorie_produit
        ORDER BY total_ventes DESC
    """).to_pandas()
    
    if len(promo_by_category) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 💵 CA généré par catégorie")
            
            chart_ventes = alt.Chart(promo_by_category).mark_bar().encode(
                x=alt.X('TOTAL_VENTES:Q', title='Total Ventes ($)', axis=alt.Axis(format='$,.0f')),
                y=alt.Y('CATEGORIE_PRODUIT:N', sort='-x', title=''),
                color=alt.Color('DISCOUNT_MOYEN:Q',
                               scale=alt.Scale(scheme='reds'),
                               legend=alt.Legend(title='Discount %')),
                tooltip=[
                    alt.Tooltip('CATEGORIE_PRODUIT:N', title='Catégorie'),
                    alt.Tooltip('TOTAL_VENTES:Q', title='CA', format='$,.0f'),
                    alt.Tooltip('DISCOUNT_MOYEN:Q', title='Discount', format='.1f'),
                    alt.Tooltip('NB_PROMOTIONS:Q', title='Nb promos'),
                    alt.Tooltip('PANIER_MOYEN:Q', title='Panier', format='$,.0f')
                ]
            ).properties(height=400)
            
            st.altair_chart(chart_ventes, use_container_width=True)
        
        with col2:
            st.markdown("#### 🎯 Nombre de promotions")
            
            chart_nb = alt.Chart(promo_by_category).mark_bar().encode(
                x=alt.X('NB_PROMOTIONS:Q', title='Nombre'),
                y=alt.Y('CATEGORIE_PRODUIT:N', sort='-x', title=''),
                color=alt.Color('DUREE_MOYENNE:Q',
                               scale=alt.Scale(scheme='blues'),
                               legend=alt.Legend(title='Durée (j)')),
                tooltip=[
                    alt.Tooltip('CATEGORIE_PRODUIT:N', title='Catégorie'),
                    alt.Tooltip('NB_PROMOTIONS:Q', title='Nb promos'),
                    alt.Tooltip('DUREE_MOYENNE:Q', title='Durée', format='.0f'),
                    alt.Tooltip('DISCOUNT_MOYEN:Q', title='Discount', format='.1f')
                ]
            ).properties(height=400)
            
            st.altair_chart(chart_nb, use_container_width=True)
        
        # Tableau détaillé
        st.markdown("#### 📋 Vue d'ensemble par catégorie")
        st.dataframe(
            promo_by_category.style.format({
                'NB_PROMOTIONS': '{:,.0f}',
                'DISCOUNT_MOYEN': '{:.1f}%',
                'TOTAL_VENTES': '${:,.0f}',
                'TOTAL_TRANSACTIONS': '{:,.0f}',
                'PANIER_MOYEN': '${:,.0f}',
                'DUREE_MOYENNE': '{:.0f} j'
            }),
            use_container_width=True
        )
    else:
        st.warning("Aucune catégorie ne correspond aux filtres")
except Exception as e:
    st.error(f"Erreur performance par catégorie : {str(e)}")

# ============= CORRELATION DISCOUNT VS VENTES =============
st.markdown("---")
st.subheader("🔍 Corrélation : Taux de Remise vs Ventes Générées")

try:
    correlation_data = session.sql(f"""
        SELECT 
            taux_remise * 100 as discount,
            montant_ventes_pendant_promo as ventes,
            nb_ventes_pendant_promo as transactions,
            categorie_produit,
            type_promotion,
            region
        FROM ANALYTICS.promotions_analytics
        {where_clause}
        AND montant_ventes_pendant_promo > 0
        LIMIT 500
    """).to_pandas()
    
    if len(correlation_data) > 0:
        scatter = alt.Chart(correlation_data).mark_circle(size=120, opacity=0.6).encode(
            x=alt.X('DISCOUNT:Q',
                    title='Taux de Remise (%)',
                    scale=alt.Scale(domain=[0, max(25, correlation_data['DISCOUNT'].max() * 1.1)])),
            y=alt.Y('VENTES:Q',
                    title='Ventes Générées ($)',
                    axis=alt.Axis(format='$,.0f')),
            size=alt.Size('TRANSACTIONS:Q',
                          scale=alt.Scale(range=[50, 600]),
                          legend=alt.Legend(title='Transactions')),
            color=alt.Color('CATEGORIE_PRODUIT:N',
                           legend=alt.Legend(title='Catégorie')),
            tooltip=[
                alt.Tooltip('CATEGORIE_PRODUIT:N', title='Catégorie'),
                alt.Tooltip('TYPE_PROMOTION:N', title='Type'),
                alt.Tooltip('REGION:N', title='Région'),
                alt.Tooltip('DISCOUNT:Q', title='Discount', format='.1f'),
                alt.Tooltip('VENTES:Q', title='Ventes', format='$,.0f'),
                alt.Tooltip('TRANSACTIONS:Q', title='Transactions', format=',')
            ]
        ).properties(
            height=500,
            title='Chaque bulle = une promotion | Taille = nombre de transactions'
        ).configure_title(fontSize=16, anchor='start').interactive()
        
        st.altair_chart(scatter, use_container_width=True)
        
        # Analyse corrélation
        if len(correlation_data) > 2:
            correlation = correlation_data['DISCOUNT'].corr(correlation_data['VENTES'])
            
            if abs(correlation) < 0.2:
                st.info(f"""
                ℹ️ **Corrélation très faible** ({correlation:.2f})
                - Pas de relation linéaire claire entre le niveau de remise et les ventes
                - D'autres facteurs (catégorie, région, durée) influencent davantage la performance
                """)
            elif correlation > 0.3:
                st.success(f"""
                ✅ **Corrélation positive** ({correlation:.2f})
                - Les remises plus élevées tendent à générer plus de ventes
                - Relation modérée mais observable
                """)
            elif correlation < -0.3:
                st.warning(f"""
                ⚠️ **Corrélation négative** ({correlation:.2f})
                - Les remises les plus élevées ne génèrent pas nécessairement plus de ventes
                - Revoir la stratégie de pricing promotionnel
                """)
    else:
        st.warning("Aucune donnée de corrélation disponible avec les filtres sélectionnés")
except Exception as e:
    st.error(f"Erreur corrélation : {str(e)}")

# ============= PERFORMANCE REGIONALE =============
st.markdown("---")
st.subheader("🌍 Performance par Région")

try:
    promo_by_region = session.sql(f"""
        SELECT 
            region,
            COUNT(*) as nb_promotions,
            AVG(taux_remise) * 100 as discount_moyen,
            SUM(montant_ventes_pendant_promo) as total_ventes,
            SUM(nb_ventes_pendant_promo) as total_transactions,
            AVG(panier_moyen_pendant_promo) as panier_moyen,
            AVG(duree_jours) as duree_moyenne
        FROM ANALYTICS.promotions_analytics
        {where_clause}
        GROUP BY region
        ORDER BY total_ventes DESC
    """).to_pandas()
    
    if len(promo_by_region) > 0:
        chart_region = alt.Chart(promo_by_region).mark_bar().encode(
            x=alt.X('REGION:N', title='Région', axis=alt.Axis(labelAngle=-45)),
            y=alt.Y('TOTAL_VENTES:Q', title='CA Promotions ($)', axis=alt.Axis(format='$,.0f')),
            color=alt.Color('DISCOUNT_MOYEN:Q',
                           scale=alt.Scale(scheme='redyellowgreen', reverse=True),
                           legend=alt.Legend(title='Discount %')),
            tooltip=[
                alt.Tooltip('REGION:N', title='Région'),
                alt.Tooltip('TOTAL_VENTES:Q', title='CA', format='$,.0f'),
                alt.Tooltip('NB_PROMOTIONS:Q', title='Nb promos'),
                alt.Tooltip('TOTAL_TRANSACTIONS:Q', title='Transactions', format=','),
                alt.Tooltip('DISCOUNT_MOYEN:Q', title='Discount', format='.1f'),
                alt.Tooltip('DUREE_MOYENNE:Q', title='Durée', format='.0f')
            ]
        ).properties(height=400)
        
        st.altair_chart(chart_region, use_container_width=True)
        
        st.dataframe(
            promo_by_region.style.format({
                'NB_PROMOTIONS': '{:,.0f}',
                'DISCOUNT_MOYEN': '{:.1f}%',
                'TOTAL_VENTES': '${:,.0f}',
                'TOTAL_TRANSACTIONS': '{:,.0f}',
                'PANIER_MOYEN': '${:,.0f}',
                'DUREE_MOYENNE': '{:.0f} j'
            }),
            use_container_width=True
        )
    else:
        st.warning("Aucune région ne correspond aux filtres")
except Exception as e:
    st.error(f"Erreur performance régionale : {str(e)}")

# ============= INSIGHTS =============
st.markdown("---")
st.subheader("💡 Insights Clés & Recommandations")

col1, col2 = st.columns(2)

with col1:
    st.success("""
    **✅ Points forts identifiés** :
    - Catégories les plus performantes repérées
    - Durée optimale observable selon les résultats
    - Régions réceptives aux promotions identifiées
    """)

with col2:
    st.warning("""
    **⚠️ Points d'attention** :
    - Efficacité variable selon catégories/types
    - Impact discount pas toujours linéaire
    - Opportunités d'optimisation géographique
    """)

# Recommandations automatiques
try:
    if len(promo_by_category) > 0 and len(promo_by_region) > 0 and avg_discount and avg_duration:
        best_category = promo_by_category.loc[promo_by_category['TOTAL_VENTES'].idxmax(), 'CATEGORIE_PRODUIT']
        best_region = promo_by_region.loc[promo_by_region['TOTAL_VENTES'].idxmax(), 'REGION']
        
        st.info(f"""
📊 **Recommandations Data-Driven** :

1. **Focus stratégique** : Catégorie **{best_category}** + Région **{best_region}** = combinaison gagnante

2. **Optimisation discount** : Le taux optimal se situe autour de **{avg_discount:.0f}%** (moyenne observée)

3. **Durée** : Les promotions de **{avg_duration:.0f} jours** semblent être un bon compromis

4. **Tests A/B** : Lancer des expérimentations sur différents niveaux de remise pour affiner

5. **Élargissement** : Répliquer les mécaniques performantes sur d'autres catégories/régions
        """)
    else:
        st.info("Sélectionnez plus de données pour voir les recommandations personnalisées")
except Exception as e:
    st.info("Pas assez de données pour générer des recommandations")

# Footer
st.markdown("---")
st.caption("🎁 Promotion Analytics Dashboard - Données temps réel depuis Snowflake")
st.caption(f"📊 {total_promos_filtrees:,} promotions analysées (sur {total_promos_base:,} total) | Période : {date_debut} → {date_fin}")