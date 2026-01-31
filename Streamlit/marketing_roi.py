# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import altair as alt

# Get the current credentials
session = get_active_session()

st.set_page_config(
    page_title="Marketing ROI Analytics",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personnalisé
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #27ae60;
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

st.markdown('<p class="main-header">💰 Marketing ROI & Campaign Performance</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Analyse du retour sur investissement des campagnes marketing</p>', unsafe_allow_html=True)

# ============= SIDEBAR - FILTRES =============
st.sidebar.header("🎯 Filtres d'Analyse")

# Filtre de dates
date_range = session.sql("""
    SELECT 
        MIN(start_date) as min_date,
        MAX(end_date) as max_date
    FROM SILVER.marketing_campaigns_clean
""").collect()[0]

min_date = date_range['MIN_DATE']
max_date = date_range['MAX_DATE']

st.sidebar.subheader("📅 Période des campagnes")
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

# Filtre types de campagne
st.sidebar.subheader("📢 Types de campagne")
campaign_types = session.sql("""
    SELECT DISTINCT campaign_type 
    FROM SILVER.marketing_campaigns_clean 
    WHERE campaign_type IS NOT NULL
    ORDER BY campaign_type
""").to_pandas()

selected_types = st.sidebar.multiselect(
    "Sélectionner les types",
    options=campaign_types['CAMPAIGN_TYPE'].tolist(),
    default=campaign_types['CAMPAIGN_TYPE'].tolist()
)

# Filtre régions
st.sidebar.subheader("🌍 Régions")
regions = session.sql("""
    SELECT DISTINCT region 
    FROM SILVER.marketing_campaigns_clean 
    WHERE region IS NOT NULL
    ORDER BY region
""").to_pandas()

selected_regions = st.sidebar.multiselect(
    "Sélectionner les régions",
    options=regions['REGION'].tolist(),
    default=regions['REGION'].tolist()
)

# Filtre catégories
st.sidebar.subheader("🏷️ Catégories produits")
categories = session.sql("""
    SELECT DISTINCT product_category 
    FROM SILVER.marketing_campaigns_clean 
    WHERE product_category IS NOT NULL
    ORDER BY product_category
""").to_pandas()

selected_categories = st.sidebar.multiselect(
    "Sélectionner les catégories",
    options=categories['PRODUCT_CATEGORY'].tolist(),
    default=categories['PRODUCT_CATEGORY'].tolist()
)

# Construction WHERE
where_conditions = [
    f"start_date >= '{date_debut}'",
    f"end_date <= '{date_fin}'"
]

if selected_types:
    types_str = "','".join(selected_types)
    where_conditions.append(f"campaign_type IN ('{types_str}')")

if selected_regions:
    regions_str = "','".join(selected_regions)
    where_conditions.append(f"region IN ('{regions_str}')")

if selected_categories:
    cats_str = "','".join(selected_categories)
    where_conditions.append(f"product_category IN ('{cats_str}')")

where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else "WHERE 1=1"

st.sidebar.markdown("---")
st.sidebar.info(f"📊 {len(selected_types)} type(s) | {len(selected_regions)} région(s)")

# ============= KPIs PRINCIPAUX =============
st.markdown("---")
st.subheader("📊 Indicateurs Clés Marketing")

col1, col2, col3, col4 = st.columns(4)

# Budget total
total_budget = session.sql(f"""
    SELECT COALESCE(SUM(budget), 0) as total 
    FROM SILVER.marketing_campaigns_clean
    {where_clause}
""").collect()[0]['TOTAL']

col1.metric("💵 Budget Total", f"${total_budget:,.0f}")

# Reach total
total_reach = session.sql(f"""
    SELECT COALESCE(SUM(reach), 0) as total 
    FROM SILVER.marketing_campaigns_clean
    {where_clause}
""").collect()[0]['TOTAL']

col2.metric("👥 Reach Total", f"{total_reach:,.0f}")

# Conversion moyenne
avg_conversion = session.sql(f"""
    SELECT AVG(conversion_rate) * 100 as avg_conv 
    FROM SILVER.marketing_campaigns_clean
    {where_clause}
""").collect()[0]['AVG_CONV']

col3.metric("📈 Conversion Moy.", f"{avg_conversion:.2f}%")

# Coût par conversion
cost_per_conversion = session.sql(f"""
    SELECT 
        SUM(budget) / NULLIF(SUM(reach * conversion_rate), 0) as cpc
    FROM SILVER.marketing_campaigns_clean
    {where_clause}
""").collect()[0]['CPC']

col4.metric("💰 Coût/Conversion", f"${cost_per_conversion:.2f}")

# ============= PERFORMANCE PAR TYPE =============
st.markdown("---")
st.subheader("📊 Performance par Type de Campagne")

campaign_performance = session.sql(f"""
    SELECT 
        campaign_type,
        COUNT(*) as nb_campagnes,
        SUM(budget) as budget_total,
        SUM(reach) as reach_total,
        AVG(conversion_rate) * 100 as conversion_moyenne,
        SUM(reach * conversion_rate) as conversions_totales,
        SUM(budget) / NULLIF(SUM(reach * conversion_rate), 0) as cout_par_conversion
    FROM SILVER.marketing_campaigns_clean
    {where_clause}
    GROUP BY campaign_type
    ORDER BY cout_par_conversion ASC NULLS LAST
""").to_pandas()

col1, col2 = st.columns([2, 1])

with col1:
    chart_perf = alt.Chart(campaign_performance).mark_bar().encode(
        x=alt.X('CAMPAIGN_TYPE:N', title='Type de Campagne', axis=alt.Axis(labelAngle=-45)),
        y=alt.Y('COUT_PAR_CONVERSION:Q', title='Coût/Conversion ($)'),
        color=alt.Color('CONVERSION_MOYENNE:Q',
                       scale=alt.Scale(scheme='redyellowgreen'),
                       legend=alt.Legend(title='Conv. %')),
        tooltip=[
            alt.Tooltip('CAMPAIGN_TYPE:N', title='Type'),
            alt.Tooltip('NB_CAMPAGNES:Q', title='Nb campagnes'),
            alt.Tooltip('BUDGET_TOTAL:Q', title='Budget', format='$,.0f'),
            alt.Tooltip('CONVERSIONS_TOTALES:Q', title='Conversions', format=',.0f'),
            alt.Tooltip('CONVERSION_MOYENNE:Q', title='Conv. %', format='.2f'),
            alt.Tooltip('COUT_PAR_CONVERSION:Q', title='Coût/Conv', format='$,.2f')
        ]
    ).properties(height=400)
    
    st.altair_chart(chart_perf, use_container_width=True)

with col2:
    if len(campaign_performance) > 0:
        best_idx = campaign_performance['COUT_PAR_CONVERSION'].idxmin()
        best_type = campaign_performance.loc[best_idx, 'CAMPAIGN_TYPE']
        best_cpc = campaign_performance.loc[best_idx, 'COUT_PAR_CONVERSION']
        best_conv = campaign_performance.loc[best_idx, 'CONVERSION_MOYENNE']
        
        st.success("🏆 **Meilleur ROI**")
        st.metric("Type", best_type)
        st.metric("Coût/Conv", f"${best_cpc:.2f}")
        st.metric("Conversion", f"{best_conv:.2f}%")

st.dataframe(
    campaign_performance.style.format({
        'NB_CAMPAGNES': '{:,.0f}',
        'BUDGET_TOTAL': '${:,.0f}',
        'REACH_TOTAL': '{:,.0f}',
        'CONVERSION_MOYENNE': '{:.2f}%',
        'CONVERSIONS_TOTALES': '{:,.0f}',
        'COUT_PAR_CONVERSION': '${:.2f}'
    }),
    use_container_width=True
)

# ============= ROI PAR CATEGORIE =============
st.markdown("---")
st.subheader("🎯 ROI par Catégorie de Produit")

roi_by_category = session.sql(f"""
    SELECT 
        product_category,
        COUNT(*) as nb_campagnes,
        SUM(budget) as budget_total,
        SUM(reach * conversion_rate) as conversions_totales,
        AVG(conversion_rate) * 100 as conversion_moyenne,
        SUM(budget) / NULLIF(SUM(reach * conversion_rate), 0) as cout_par_conversion
    FROM SILVER.marketing_campaigns_clean
    {where_clause}
    GROUP BY product_category
    ORDER BY cout_par_conversion ASC NULLS LAST
""").to_pandas()

chart_roi = alt.Chart(roi_by_category).mark_bar().encode(
    x=alt.X('PRODUCT_CATEGORY:N', title='Catégorie', axis=alt.Axis(labelAngle=-45)),
    y=alt.Y('COUT_PAR_CONVERSION:Q', title='Coût/Conversion ($)', axis=alt.Axis(format='$,.0f')),
    color=alt.Color('CONVERSION_MOYENNE:Q',
                   scale=alt.Scale(scheme='redyellowgreen'),
                   legend=alt.Legend(title='Conv. %')),
    tooltip=[
        alt.Tooltip('PRODUCT_CATEGORY:N', title='Catégorie'),
        alt.Tooltip('NB_CAMPAGNES:Q', title='Nb campagnes'),
        alt.Tooltip('BUDGET_TOTAL:Q', title='Budget', format='$,.0f'),
        alt.Tooltip('CONVERSIONS_TOTALES:Q', title='Conversions', format=',.0f'),
        alt.Tooltip('COUT_PAR_CONVERSION:Q', title='Coût/Conv', format='$,.2f')
    ]
).properties(
    height=400,
    title="Plus le coût est bas, meilleur est le ROI"
).configure_title(fontSize=16, anchor='start')

st.altair_chart(chart_roi, use_container_width=True)

# ============= TOP & FLOP CAMPAGNES =============
st.markdown("---")
st.subheader("🏆 Meilleures et Pires Campagnes")

col1, col2 = st.columns(2)

with col1:
    st.markdown("#### ✅ Top 15 (Meilleur ROI)")
    
    top_campaigns = session.sql(f"""
        SELECT 
            campaign_name,
            campaign_type,
            product_category,
            budget,
            conversion_rate * 100 as conv_pct,
            reach * conversion_rate as conversions,
            budget / NULLIF(reach * conversion_rate, 0) as cpc
        FROM SILVER.marketing_campaigns_clean
        {where_clause}
        AND reach > 0 AND conversion_rate > 0
        ORDER BY cpc ASC
        LIMIT 15
    """).to_pandas()
    
    st.dataframe(
        top_campaigns.style.format({
            'BUDGET': '${:,.0f}',
            'CONV_PCT': '{:.2f}%',
            'CONVERSIONS': '{:,.0f}',
            'CPC': '${:.2f}'
        }),
        use_container_width=True,
        height=500
    )

with col2:
    st.markdown("#### ⚠️ Top 15 (Pire ROI)")
    
    worst_campaigns = session.sql(f"""
        SELECT 
            campaign_name,
            campaign_type,
            product_category,
            budget,
            conversion_rate * 100 as conv_pct,
            reach * conversion_rate as conversions,
            budget / NULLIF(reach * conversion_rate, 0) as cpc
        FROM SILVER.marketing_campaigns_clean
        {where_clause}
        AND reach > 0 AND conversion_rate > 0
        ORDER BY cpc DESC
        LIMIT 15
    """).to_pandas()
    
    st.dataframe(
        worst_campaigns.style.format({
            'BUDGET': '${:,.0f}',
            'CONV_PCT': '{:.2f}%',
            'CONVERSIONS': '{:,.0f}',
            'CPC': '${:.2f}'
        }),
        use_container_width=True,
        height=500
    )

# ============= MATRICE EFFICACITE =============
st.markdown("---")
st.subheader("🎯 Matrice d'Efficacité : Budget vs Conversion")

efficiency_matrix = session.sql(f"""
    SELECT 
        campaign_type,
        budget,
        conversion_rate * 100 as conversion_pct,
        reach * conversion_rate as conversions
    FROM SILVER.marketing_campaigns_clean
    {where_clause}
    AND reach > 0 
    AND conversion_rate > 0
    LIMIT 500
""").to_pandas()

if len(efficiency_matrix) > 0:
    avg_budget = efficiency_matrix['BUDGET'].median()
    avg_conv = efficiency_matrix['CONVERSION_PCT'].median()

    scatter = alt.Chart(efficiency_matrix).mark_circle(opacity=0.6).encode(
        x=alt.X('BUDGET:Q',
                title='Budget ($)',
                scale=alt.Scale(type='log'),
                axis=alt.Axis(format='$,.0f')),
        y=alt.Y('CONVERSION_PCT:Q',
                title='Taux de Conversion (%)'),
        size=alt.Size('CONVERSIONS:Q',
                      scale=alt.Scale(range=[50, 500]),
                      legend=alt.Legend(title='Conversions')),
        color=alt.Color('CAMPAIGN_TYPE:N',
                       legend=alt.Legend(title='Type')),
        tooltip=[
            alt.Tooltip('CAMPAIGN_TYPE:N', title='Type'),
            alt.Tooltip('BUDGET:Q', title='Budget', format='$,.0f'),
            alt.Tooltip('CONVERSION_PCT:Q', title='Conversion', format='.2f'),
            alt.Tooltip('CONVERSIONS:Q', title='Conversions', format=',.0f')
        ]
    ).properties(
        height=500,
        title='Chaque bulle = une campagne | Taille = nombre de conversions'
    ).configure_title(fontSize=16, anchor='start').interactive()

    st.altair_chart(scatter, use_container_width=True)

    # Quadrants
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        q1 = len(efficiency_matrix[(efficiency_matrix['BUDGET'] < avg_budget) & (efficiency_matrix['CONVERSION_PCT'] > avg_conv)])
        st.success(f"""
        🟢 **Pépites**
        
        {q1} campagnes
        
        Budget faible
        Conv. élevée
        
        → À AMPLIFIER
        """)

    with col2:
        q2 = len(efficiency_matrix[(efficiency_matrix['BUDGET'] >= avg_budget) & (efficiency_matrix['CONVERSION_PCT'] > avg_conv)])
        st.info(f"""
        🔵 **Stars**
        
        {q2} campagnes
        
        Budget élevé
        Conv. élevée
        
        → À MAINTENIR
        """)

    with col3:
        q3 = len(efficiency_matrix[(efficiency_matrix['BUDGET'] < avg_budget) & (efficiency_matrix['CONVERSION_PCT'] <= avg_conv)])
        st.warning(f"""
        🟡 **À Optimiser**
        
        {q3} campagnes
        
        Budget faible
        Conv. faible
        
        → À TESTER
        """)

    with col4:
        q4 = len(efficiency_matrix[(efficiency_matrix['BUDGET'] >= avg_budget) & (efficiency_matrix['CONVERSION_PCT'] <= avg_conv)])
        st.error(f"""
        🔴 **À Stopper**
        
        {q4} campagnes
        
        Budget élevé
        Conv. faible
        
        → À ARRÊTER
        """)

# ============= PERFORMANCE REGIONALE =============
st.markdown("---")
st.subheader("🌍 Performance par Région")

roi_by_region = session.sql(f"""
    SELECT 
        region,
        COUNT(*) as nb_campagnes,
        SUM(budget) as budget_total,
        SUM(reach) as reach_total,
        AVG(conversion_rate) * 100 as conversion_moyenne,
        SUM(reach * conversion_rate) as conversions_totales,
        SUM(budget) / NULLIF(SUM(reach * conversion_rate), 0) as cout_par_conversion
    FROM SILVER.marketing_campaigns_clean
    {where_clause}
    GROUP BY region
    ORDER BY cout_par_conversion ASC NULLS LAST
""").to_pandas()

chart_region = alt.Chart(roi_by_region).mark_bar().encode(
    x=alt.X('REGION:N', title='Région', axis=alt.Axis(labelAngle=-45)),
    y=alt.Y('COUT_PAR_CONVERSION:Q', title='Coût/Conversion ($)'),
    color=alt.Color('CONVERSION_MOYENNE:Q',
                   scale=alt.Scale(scheme='redyellowgreen'),
                   legend=alt.Legend(title='Conv. %')),
    tooltip=[
        alt.Tooltip('REGION:N', title='Région'),
        alt.Tooltip('NB_CAMPAGNES:Q', title='Nb campagnes'),
        alt.Tooltip('BUDGET_TOTAL:Q', title='Budget', format='$,.0f'),
        alt.Tooltip('CONVERSIONS_TOTALES:Q', title='Conversions', format=',.0f'),
        alt.Tooltip('COUT_PAR_CONVERSION:Q', title='Coût/Conv', format='$,.2f')
    ]
).properties(height=400)

st.altair_chart(chart_region, use_container_width=True)

st.dataframe(
    roi_by_region.style.format({
        'NB_CAMPAGNES': '{:,.0f}',
        'BUDGET_TOTAL': '${:,.0f}',
        'REACH_TOTAL': '{:,.0f}',
        'CONVERSION_MOYENNE': '{:.2f}%',
        'CONVERSIONS_TOTALES': '{:,.0f}',
        'COUT_PAR_CONVERSION': '${:.2f}'
    }),
    use_container_width=True
)

# ============= RECOMMANDATIONS =============
st.markdown("---")
st.subheader("💡 Recommandations Stratégiques")

if len(campaign_performance) > 0 and len(efficiency_matrix) > 0:
    best_type = campaign_performance.loc[campaign_performance['COUT_PAR_CONVERSION'].idxmin(), 'CAMPAIGN_TYPE']
    best_cpc = campaign_performance['COUT_PAR_CONVERSION'].min()
    avg_cpc = campaign_performance['COUT_PAR_CONVERSION'].mean()
    potential_savings = ((avg_cpc - best_cpc) / avg_cpc * 100)

    col1, col2 = st.columns(2)

    with col1:
        st.success(f"""
        **✅ Actions Prioritaires** :
        
        1. **Amplifier** : Type **{best_type}** (ROI optimal : ${best_cpc:.2f}/conv)
        
        2. **Scaler** : {q1} campagnes "pépites" identifiées
        
        3. **Répliquer** : Best practices des Top 15 campagnes
        
        4. **Économiser** : Potentiel **{potential_savings:.1f}%** sur budget global
        """)

    with col2:
        st.warning(f"""
        **⚠️ Actions Correctives** :
        
        1. **Stopper** : {q4} campagnes fort budget/faible conversion
        
        2. **Réallouer** : Budget des campagnes inefficaces vers pépites
        
        3. **Optimiser** : {q3} campagnes à faible coût mais sous-performantes
        
        4. **Auditer** : Campagnes coût > ${cost_per_conversion:.2f}/conv
        """)

    # Résumé exécutif
    st.info(f"""
    📊 **Résumé Exécutif** :

    - 💰 Budget marketing déployé : **${total_budget:,.0f}**
    - 👥 Reach total cumulé : **{total_reach:,.0f}** personnes
    - 📈 Taux de conversion moyen : **{avg_conversion:.2f}%**
    - 💵 Coût moyen par conversion : **${cost_per_conversion:.2f}**

    **Opportunités d'Optimisation Identifiées** :
    - **{q1}** campagnes "pépites" (faible coût, haute conversion) → Investir massivement
    - **{q4}** campagnes "à risque" (fort coût, faible conversion) → Arrêter immédiatement
    - Économie potentielle totale : **{potential_savings:.1f}%** du budget marketing
    
    **Impact Estimé** : Réallocation optimale = **+{potential_savings*2:.0f}%** de conversions avec même budget
    """)
else:
    st.warning("Sélectionnez des filtres pour voir les recommandations personnalisées")

# Footer
st.markdown("---")
st.caption("💰 Marketing ROI Dashboard - Powered by Snowflake ANALYTICS")
st.caption(f"📊 Période : {date_debut} → {date_fin} | Budget analysé : ${total_budget:,.0f}")

