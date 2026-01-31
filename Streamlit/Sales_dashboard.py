# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import altair as alt
from datetime import datetime, timedelta

# Get the current credentials
session = get_active_session()

# Configuration de la page
st.set_page_config(
    page_title="Sales Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personnalisé pour un design moderne
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-container {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .filter-box {
        background-color: #f8f9fa;
        padding: 1.5rem;
        border-radius: 10px;
        border-left: 4px solid #1f77b4;
        margin-bottom: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# En-tête principal
st.markdown('<p class="main-header">📊 AnyCompany Sales Analytics Dashboard</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Analyse complète des performances de ventes et impact promotionnel</p>', unsafe_allow_html=True)

# ============= SIDEBAR - FILTRES =============
st.sidebar.header("🎯 Filtres d'Analyse")

# Récupération des dates min/max
date_range = session.sql("""
    SELECT 
        MIN(transaction_date) as min_date,
        MAX(transaction_date) as max_date
    FROM ANALYTICS.sales_enriched
""").collect()[0]

min_date = date_range['MIN_DATE']
max_date = date_range['MAX_DATE']

# Filtre de dates
st.sidebar.subheader("📅 Période")
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

# Filtre régions
st.sidebar.subheader("🌍 Régions")
regions = session.sql("""
    SELECT DISTINCT region 
    FROM ANALYTICS.sales_enriched 
    WHERE region IS NOT NULL
    ORDER BY region
""").to_pandas()

selected_regions = st.sidebar.multiselect(
    "Sélectionner les régions",
    options=regions['REGION'].tolist(),
    default=regions['REGION'].tolist()
)

# Filtre promotions
st.sidebar.subheader("🎁 Promotions")
promo_filter = st.sidebar.radio(
    "Filtrer par promotion",
    options=["Toutes les ventes", "Avec promotion uniquement", "Sans promotion uniquement"],
    index=0
)

# Construction de la clause WHERE dynamique
where_conditions = [
    f"transaction_date BETWEEN '{date_debut}' AND '{date_fin}'"
]

if selected_regions:
    regions_str = "','".join(selected_regions)
    where_conditions.append(f"region IN ('{regions_str}')")

if promo_filter == "Avec promotion uniquement":
    where_conditions.append("is_promo = TRUE")
elif promo_filter == "Sans promotion uniquement":
    where_conditions.append("is_promo = FALSE")

where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

st.sidebar.markdown("---")
st.sidebar.info(f"📊 Période analysée : {(date_fin - date_debut).days} jours")

# ============= SECTION KPIs =============
st.markdown("---")
st.subheader("📈 Indicateurs Clés de Performance")

col1, col2, col3, col4 = st.columns(4)

# KPI 1: Total des ventes
total_sales = session.sql(f"""
    SELECT COALESCE(SUM(amount), 0) as total 
    FROM ANALYTICS.sales_enriched
    {where_clause}
""").collect()[0]['TOTAL']

with col1:
    st.metric(
        label="💰 Total Ventes",
        value=f"${total_sales:,.0f}",
        delta=None
    )

# KPI 2: Nombre de transactions
total_transactions = session.sql(f"""
    SELECT COUNT(*) as total 
    FROM ANALYTICS.sales_enriched
    {where_clause}
""").collect()[0]['TOTAL']

with col2:
    st.metric(
        label="🛒 Transactions",
        value=f"{total_transactions:,}",
        delta=None
    )

# KPI 3: Panier moyen
avg_basket = total_sales / total_transactions if total_transactions > 0 else 0

with col3:
    st.metric(
        label="📦 Panier Moyen",
        value=f"${avg_basket:,.2f}",
        delta=None
    )

# KPI 4: Taux de promotion
promo_sales = session.sql(f"""
    SELECT COUNT(*) as total 
    FROM ANALYTICS.sales_enriched
    {where_clause}
    AND is_promo = TRUE
""").collect()[0]['TOTAL']

promo_rate = (promo_sales / total_transactions * 100) if total_transactions > 0 else 0

with col4:
    st.metric(
        label="🎁 Taux Promotion",
        value=f"{promo_rate:.1f}%",
        delta=f"{promo_sales:,} ventes"
    )

# ============= EVOLUTION TEMPORELLE =============
st.markdown("---")
st.subheader("📈 Évolution Mensuelle des Ventes")

sales_evolution = session.sql(f"""
    SELECT 
        mois,
        SUM(amount) as total_ventes,
        COUNT(*) as nb_transactions,
        AVG(amount) as panier_moyen,
        SUM(CASE WHEN is_promo THEN amount ELSE 0 END) as ventes_promo,
        SUM(CASE WHEN is_promo THEN 1 ELSE 0 END) as trans_promo
    FROM ANALYTICS.sales_enriched
    {where_clause}
    GROUP BY mois
    ORDER BY mois
""").to_pandas()

if len(sales_evolution) > 0:
    # Graphique principal - Area chart
    base = alt.Chart(sales_evolution).encode(
        x=alt.X('MOIS:T', title='Mois', axis=alt.Axis(format='%b %Y'))
    )

    area = base.mark_area(
        line={'color':'#1f77b4', 'size': 3},
        color=alt.Gradient(
            gradient='linear',
            stops=[
                alt.GradientStop(color='white', offset=0),
                alt.GradientStop(color='lightblue', offset=0.5),
                alt.GradientStop(color='#1f77b4', offset=1)
            ],
            x1=1, x2=1, y1=1, y2=0
        )
    ).encode(
        y=alt.Y('TOTAL_VENTES:Q', title='Ventes Totales ($)', axis=alt.Axis(format='$,.0f'))
    )

    line_promo = base.mark_line(
        color='#ff7f0e',
        size=2,
        strokeDash=[5, 5]
    ).encode(
        y=alt.Y('VENTES_PROMO:Q', title='Ventes avec Promo ($)')
    )

    chart_evolution = (area + line_promo).properties(
        height=400,
        title='Ventes totales (bleu) vs Ventes avec promotion (orange pointillé)'
    ).configure_title(
        fontSize=16,
        anchor='start'
    ).interactive()

    st.altair_chart(chart_evolution, use_container_width=True)

    # Statistiques additionnelles
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        best_month = sales_evolution.loc[sales_evolution['TOTAL_VENTES'].idxmax(), 'MOIS']
        st.metric(
            "📅 Meilleur mois",
            best_month.strftime('%B %Y'),
            f"${sales_evolution['TOTAL_VENTES'].max():,.0f}"
        )
    
    with col2:
        worst_month = sales_evolution.loc[sales_evolution['TOTAL_VENTES'].idxmin(), 'MOIS']
        st.metric(
            "📉 Mois le plus faible",
            worst_month.strftime('%B %Y'),
            f"${sales_evolution['TOTAL_VENTES'].min():,.0f}"
        )
    
    with col3:
        if len(sales_evolution) > 1:
            growth = ((sales_evolution['TOTAL_VENTES'].iloc[-1] - sales_evolution['TOTAL_VENTES'].iloc[0]) 
                      / sales_evolution['TOTAL_VENTES'].iloc[0] * 100)
            st.metric("📊 Croissance période", f"{growth:+.1f}%")
        else:
            st.metric("📊 Croissance période", "N/A")
    
    with col4:
        avg_monthly = sales_evolution['TOTAL_VENTES'].mean()
        st.metric("💵 CA moyen/mois", f"${avg_monthly:,.0f}")

# ============= PERFORMANCE REGIONALE =============
st.markdown("---")
st.subheader("🌍 Performance par Région")

sales_by_region = session.sql(f"""
    SELECT 
        region,
        SUM(amount) as total_ventes,
        COUNT(*) as nb_transactions,
        AVG(amount) as panier_moyen,
        SUM(CASE WHEN is_promo THEN 1 ELSE 0 END) as trans_avec_promo,
        ROUND(SUM(CASE WHEN is_promo THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_promo
    FROM ANALYTICS.sales_enriched
    {where_clause}
    GROUP BY region
    ORDER BY total_ventes DESC
""").to_pandas()

col1, col2 = st.columns([2, 1])

with col1:
    st.markdown("#### 💵 Chiffre d'affaires par région")
    
    chart_region = alt.Chart(sales_by_region).mark_bar().encode(
        x=alt.X('TOTAL_VENTES:Q', title='Total Ventes ($)', axis=alt.Axis(format='$,.0f')),
        y=alt.Y('REGION:N', sort='-x', title=''),
        color=alt.Color('TAUX_PROMO:Q',
                       scale=alt.Scale(scheme='oranges'),
                       legend=alt.Legend(title='Taux Promo %')),
        tooltip=[
            alt.Tooltip('REGION:N', title='Région'),
            alt.Tooltip('TOTAL_VENTES:Q', title='Ventes', format='$,.2f'),
            alt.Tooltip('NB_TRANSACTIONS:Q', title='Transactions', format=','),
            alt.Tooltip('PANIER_MOYEN:Q', title='Panier moyen', format='$,.2f'),
            alt.Tooltip('TAUX_PROMO:Q', title='Taux promo', format='.1f')
        ]
    ).properties(height=400)
    
    st.altair_chart(chart_region, use_container_width=True)

with col2:
    st.markdown("#### 📊 Top 3 Régions")
    
    for idx, row in sales_by_region.head(3).iterrows():
        with st.container():
            st.markdown(f"""
            <div style='background-color: #f0f2f6; padding: 1rem; border-radius: 8px; margin-bottom: 0.5rem;'>
                <h4 style='margin: 0; color: #1f77b4;'>🏆 {row['REGION']}</h4>
                <p style='margin: 0.5rem 0 0 0; font-size: 1.2rem;'><b>${row['TOTAL_VENTES']:,.0f}</b></p>
                <p style='margin: 0; color: #666; font-size: 0.9rem;'>{row['NB_TRANSACTIONS']:,} transactions</p>
            </div>
            """, unsafe_allow_html=True)

# ============= IMPACT DES PROMOTIONS =============
st.markdown("---")
st.subheader("🎁 Analyse Détaillée de l'Impact Promotionnel")

promo_impact = session.sql(f"""
    SELECT 
        is_promo,
        COUNT(*) as nb_transactions,
        SUM(amount) as total_ventes,
        AVG(amount) as panier_moyen,
        MIN(amount) as vente_min,
        MAX(amount) as vente_max
    FROM ANALYTICS.sales_enriched
    {where_clause}
    GROUP BY is_promo
    ORDER BY is_promo DESC
""").to_pandas()

# Renommer pour affichage
promo_impact['STATUT'] = promo_impact['IS_PROMO'].map({True: 'Avec Promotion', False: 'Sans Promotion'})

col1, col2 = st.columns([3, 2])

with col1:
    # Graphique comparatif
    chart_promo = alt.Chart(promo_impact).mark_bar(size=80).encode(
        x=alt.X('TOTAL_VENTES:Q', title='Total Ventes ($)', axis=alt.Axis(format='$,.0f')),
        y=alt.Y('STATUT:N', title='', axis=alt.Axis(labelFontSize=14)),
        color=alt.Color('STATUT:N',
                       scale=alt.Scale(domain=['Sans Promotion', 'Avec Promotion'],
                                     range=['#ff7f0e', '#2ca02c']),
                       legend=None),
        tooltip=[
            alt.Tooltip('STATUT:N', title='Statut'),
            alt.Tooltip('TOTAL_VENTES:Q', title='Ventes totales', format='$,.0f'),
            alt.Tooltip('NB_TRANSACTIONS:Q', title='Transactions', format=','),
            alt.Tooltip('PANIER_MOYEN:Q', title='Panier moyen', format='$,.2f')
        ]
    ).properties(height=250)
    
    st.altair_chart(chart_promo, use_container_width=True)

with col2:
    if len(promo_impact) >= 2:
        avec_promo = promo_impact[promo_impact['IS_PROMO'] == True]
        sans_promo = promo_impact[promo_impact['IS_PROMO'] == False]
        
        if len(avec_promo) > 0 and len(sans_promo) > 0:
            panier_avec = avec_promo['PANIER_MOYEN'].values[0]
            panier_sans = sans_promo['PANIER_MOYEN'].values[0]
            diff_panier = ((panier_avec - panier_sans) / panier_sans * 100)
            
            st.metric(
                "📊 Impact Panier Moyen",
                f"${panier_avec:,.2f}",
                f"{diff_panier:+.1f}% vs sans promo"
            )
            
            trans_avec = avec_promo['NB_TRANSACTIONS'].values[0]
            st.metric(
                "🎁 Ventes avec Promo",
                f"{trans_avec:,}",
                f"{(trans_avec/total_transactions*100):.1f}% du total"
            )

# Tableau détaillé
st.markdown("#### 📋 Comparaison Détaillée")
st.dataframe(
    promo_impact[['STATUT', 'NB_TRANSACTIONS', 'TOTAL_VENTES', 'PANIER_MOYEN', 'VENTE_MIN', 'VENTE_MAX']].style.format({
        'NB_TRANSACTIONS': '{:,.0f}',
        'TOTAL_VENTES': '${:,.2f}',
        'PANIER_MOYEN': '${:,.2f}',
        'VENTE_MIN': '${:,.2f}',
        'VENTE_MAX': '${:,.2f}'
    }),
    use_container_width=True
)

# Analyse automatique
if len(promo_impact) >= 2:
    avec_promo_row = promo_impact[promo_impact['IS_PROMO'] == True]
    sans_promo_row = promo_impact[promo_impact['IS_PROMO'] == False]
    
    if len(avec_promo_row) > 0 and len(sans_promo_row) > 0:
        trans_avec = avec_promo_row['NB_TRANSACTIONS'].values[0]
        trans_sans = sans_promo_row['NB_TRANSACTIONS'].values[0]
        panier_avec = avec_promo_row['PANIER_MOYEN'].values[0]
        panier_sans = sans_promo_row['PANIER_MOYEN'].values[0]
        
        couverture = (trans_avec / total_transactions * 100)
        diff_panier = ((panier_avec - panier_sans) / panier_sans * 100)
        
        if couverture < 5:
            st.error(f"""
            ⚠️ **ALERTE : Couverture promotionnelle très faible**
            - Seulement **{couverture:.2f}%** des transactions bénéficient d'une promotion
            - Sur {total_transactions:,} ventes, seules {trans_avec:,} ont une promotion active
            - **Impact panier moyen** : {diff_panier:+.1f}%
            
            **Recommandation** : Élargir significativement la couverture temporelle et géographique
            """)
        elif diff_panier > 5:
            st.success(f"""
            ✅ **Impact positif des promotions**
            - Couverture : {couverture:.1f}% des ventes
            - Augmentation panier moyen : **+{diff_panier:.1f}%**
            - Effet mesurable sur le comportement d'achat
            """)
        else:
            st.warning(f"""
            ⚠️ **Impact limité des promotions**
            - Couverture : {couverture:.1f}% des ventes
            - Impact panier moyen : {diff_panier:+.1f}%
            - Revoir la stratégie promotionnelle pour maximiser l'efficacité
            """)

# ============= TENDANCE ANNUELLE =============
st.markdown("---")
st.subheader("📅 Performance Annuelle")

yearly_trend = session.sql(f"""
    SELECT 
        YEAR(transaction_date) as annee,
        COUNT(*) as nb_transactions,
        SUM(amount) as total_ventes,
        AVG(amount) as panier_moyen,
        SUM(CASE WHEN is_promo THEN amount ELSE 0 END) as ventes_promo
    FROM ANALYTICS.sales_enriched
    {where_clause}
    GROUP BY YEAR(transaction_date)
    ORDER BY annee
""").to_pandas()

if len(yearly_trend) > 0:
    # Graphique combiné barres + ligne
    base = alt.Chart(yearly_trend).encode(
        x=alt.X('ANNEE:O', title='Année', axis=alt.Axis(labelAngle=0))
    )

    bars = base.mark_bar(color='steelblue', opacity=0.7).encode(
        y=alt.Y('TOTAL_VENTES:Q', title='Ventes Totales ($)', axis=alt.Axis(format='$,.0f')),
        tooltip=[
            alt.Tooltip('ANNEE:O', title='Année'),
            alt.Tooltip('TOTAL_VENTES:Q', title='Ventes', format='$,.0f'),
            alt.Tooltip('NB_TRANSACTIONS:Q', title='Transactions', format=','),
            alt.Tooltip('PANIER_MOYEN:Q', title='Panier moyen', format='$,.2f'),
            alt.Tooltip('VENTES_PROMO:Q', title='Ventes promo', format='$,.0f')
        ]
    )

    line = base.mark_line(color='red', size=3).encode(
        y='TOTAL_VENTES:Q'
    )

    points = base.mark_point(color='red', size=100, filled=True).encode(
        y='TOTAL_VENTES:Q'
    )

    chart_yearly = (bars + line + points).properties(
        height=400,
        title='Évolution du chiffre d\'affaires annuel'
    ).configure_title(fontSize=16, anchor='start')

    st.altair_chart(chart_yearly, use_container_width=True)

    # Statistiques annuelles
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        best_year = yearly_trend.loc[yearly_trend['TOTAL_VENTES'].idxmax(), 'ANNEE']
        st.metric("🏆 Meilleure année", int(best_year))
    
    with col2:
        worst_year = yearly_trend.loc[yearly_trend['TOTAL_VENTES'].idxmin(), 'ANNEE']
        st.metric("📉 Année la plus faible", int(worst_year))
    
    with col3:
        if len(yearly_trend) > 1:
            avg_growth = yearly_trend['TOTAL_VENTES'].pct_change().mean() * 100
            st.metric("📊 Croissance moyenne", f"{avg_growth:+.1f}%")
        else:
            st.metric("📊 Croissance moyenne", "N/A")
    
    with col4:
        volatility = yearly_trend['TOTAL_VENTES'].std() / yearly_trend['TOTAL_VENTES'].mean() * 100
        st.metric("📈 Volatilité", f"{volatility:.1f}%")

# Footer
st.markdown("---")
st.caption("📊 Dashboard Sales Analytics - Données temps réel depuis Snowflake ANALYTICS layer")
st.caption(f"🔄 Période analysée : {date_debut} → {date_fin} | {len(selected_regions)} région(s) sélectionnée(s)")

