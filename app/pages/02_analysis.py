"""Page 2 — Análise de Preços: opportunity ranking by neighborhood."""

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import polars as pl
import streamlit as st

from app.components.listing_table import render_listing_table
from app.components.opportunity_chart import render_opportunity_chart, render_opportunity_score_badge
from app.config import LISTING_TYPE_OPTIONS, PROPERTY_TYPE_OPTIONS
from houseflip.services.analysis_service import AnalysisService
from houseflip.storage.database import get_db
from houseflip.storage.repository import ListingRepository

st.title(":material/trending_down: Análise de Oportunidades")
st.caption(
    "Identifique os imóveis com o maior desvio de preço em relação ao bairro. "
    "Score positivo = abaixo da mediana = melhor oportunidade."
)

# ── Init service ──────────────────────────────────────────────────────────────
try:
    db = get_db()
    service = AnalysisService(db)
    cities = service.available_cities()
    total = service.total_listings()
except Exception as exc:
    st.error(f"Erro ao conectar ao banco de dados: {exc}")
    st.info("Se o banco está bloqueado por outro processo, feche outras abas do app e tente novamente.")
    st.stop()

if total == 0:
    st.warning(
        "Nenhum anúncio na base de dados. Execute o scraping primeiro na página **Scraping**."
    )
    st.stop()

# ── Filters — row 1: cidade, tipo, tipo de imóvel, mín. anúncios ──────────────
col1, col2, col3, col4 = st.columns([2, 1, 1, 1])

with col1:
    selected_city = st.selectbox("Cidade", ["Todas"] + cities)
    city_filter = None if selected_city == "Todas" else selected_city

with col2:
    listing_type_label = st.radio("Tipo", list(LISTING_TYPE_OPTIONS.keys()), horizontal=True)
    listing_type_filter = LISTING_TYPE_OPTIONS[listing_type_label]

with col3:
    property_type_label = st.selectbox("Tipo de imóvel", list(PROPERTY_TYPE_OPTIONS.keys()))
    property_type_filter = PROPERTY_TYPE_OPTIONS[property_type_label]

with col4:
    min_listings = st.number_input(
        "Mín. anúncios por bairro", min_value=2, max_value=20, value=3
    )

# ── Filters — row 2: bairro, metragem, data do scraping ───────────────────────
with st.expander("Filtros avançados", expanded=False):
    fa1, fa2, fa3, fa4 = st.columns([2, 2, 2, 2])

    with fa1:
        all_neighborhoods = service.available_neighborhoods(city_filter)
        selected_neighborhoods = st.multiselect(
            "Bairros",
            options=all_neighborhoods,
            default=[],
            placeholder="Todos os bairros",
        )
        neighborhoods_filter = selected_neighborhoods if selected_neighborhoods else None

    with fa4:
        all_sources = service.available_sources()
        selected_sources = st.multiselect(
            "Fonte",
            options=all_sources,
            default=[],
            placeholder="Todas as fontes",
        )
        sources_filter = selected_sources if selected_sources else None

    with fa2:
        area_col1, area_col2 = st.columns(2)
        with area_col1:
            area_min = st.number_input("Área mín. (m²)", min_value=0, max_value=10000, value=0, step=10)
        with area_col2:
            area_max = st.number_input("Área máx. (m²)", min_value=0, max_value=10000, value=0, step=10)
        area_min_filter = float(area_min) if area_min > 0 else None
        area_max_filter = float(area_max) if area_max > 0 else None

    with fa3:
        scraped_after_date = st.date_input(
            "Scraping a partir de",
            value=None,
            help="Filtra anúncios coletados a partir desta data",
        )
        scraped_after_filter: datetime | None = None
        if scraped_after_date:
            scraped_after_filter = datetime(
                scraped_after_date.year,
                scraped_after_date.month,
                scraped_after_date.day,
                tzinfo=timezone.utc,
            )

st.divider()

# ── Neighborhood summary chart ────────────────────────────────────────────────
st.subheader("Score de Oportunidade por Bairro")

with st.spinner("Calculando..."):
    try:
        summary = service.get_neighborhood_summary(
            city=city_filter,
            listing_type=listing_type_filter,
            property_type=property_type_filter,
            min_listings=int(min_listings),
            scraped_after=scraped_after_filter,
            neighborhoods=neighborhoods_filter,
            area_min=area_min_filter,
            area_max=area_max_filter,
            sources=sources_filter,
        )
    except Exception as exc:
        st.error(f"Erro ao calcular análise: {exc}")
        st.stop()

if summary.is_empty():
    st.info("Sem dados suficientes com os filtros selecionados.")
    st.stop()

# Summary metrics
m1, m2, m3 = st.columns(3)
m1.metric("Bairros analisados", len(summary))
m2.metric(
    "Melhor bairro",
    summary["neighborhood"][0] if not summary.is_empty() else "—",
)

best_score = float(summary["best_opportunity_score"][0]) if not summary.is_empty() else 0.0
m3.metric("Melhor score", f"{best_score:.2f}")

render_opportunity_chart(summary)

st.divider()

# ── Per-neighborhood detail ────────────────────────────────────────────────────
st.subheader("Anúncios por Bairro — Melhores Oportunidades")

neighborhoods = summary["neighborhood"].to_list()
selected_neighborhood = st.selectbox(
    "Selecione um bairro para ver os anúncios",
    neighborhoods,
)

if selected_neighborhood:
    with st.spinner("Carregando anúncios..."):
        listings_df = service.get_opportunities(
            city=city_filter,
            listing_type=listing_type_filter,
            property_type=property_type_filter,
            neighborhood=selected_neighborhood,
            min_listings=int(min_listings),
            scraped_after=scraped_after_filter,
            area_min=area_min_filter,
            area_max=area_max_filter,
            sources=sources_filter,
        )

    if not listings_df.is_empty():
        # Show top metrics for this neighborhood
        neigh_stats = summary.filter(pl.col("neighborhood") == selected_neighborhood)
        if not neigh_stats.is_empty():
            row = neigh_stats.row(0, named=True)
            nc1, nc2, nc3 = st.columns(3)
            nc1.metric(
                "Mediana de preço",
                f"R$ {float(row['median_price']):,.0f}",
            )
            nc2.metric("Anúncios", int(row["listing_count"]))
            nc3.markdown(
                "Melhor score: " + render_opportunity_score_badge(float(row["best_opportunity_score"]))
            )

        # Favorite buttons
        if "id" in listings_df.columns:
            repo = ListingRepository(get_db())
            fav_ids = {r["id"] for r in repo.query_favorites()}
            st.caption("Favoritar anúncios:")
            btn_cols = st.columns(min(len(listings_df), 4))
            for idx, row in enumerate(listings_df.head(12).iter_rows(named=True)):
                label = (row.get("title") or row.get("url") or row["id"])[:35]
                icon = "★" if row["id"] in fav_ids else "☆"
                with btn_cols[idx % 4]:
                    if st.button(f"{icon} {label}", key=f"fav_{row['id']}", use_container_width=True):
                        repo.toggle_favorite(row["id"])
                        st.rerun()

        # Opportunity table with score columns
        display_cols = [
            "title", "price_brl", "area_m2", "price_per_m2",
            "bedrooms", "parking_spots", "opportunity_score", "pct_vs_median", "url",
        ]
        available = [c for c in display_cols if c in listings_df.columns]
        subset = listings_df.select(available).rename({
            "title": "Título",
            "price_brl": "Preço (R$)",
            "area_m2": "Área (m²)",
            "price_per_m2": "R$/m²",
            "bedrooms": "Quartos",
            "parking_spots": "Vagas",
            "opportunity_score": "Score",
            "pct_vs_median": "% vs Mediana",
            "url": "Link",
        })

        column_config: dict = {
            "Link": st.column_config.LinkColumn("Link", display_text="Abrir"),
            "Preço (R$)": st.column_config.NumberColumn(format="R$ %.2f"),
            "R$/m²": st.column_config.NumberColumn(format="R$ %.2f"),
            "Score": st.column_config.NumberColumn(format="%.2f", help="Maior = melhor oportunidade"),
            "% vs Mediana": st.column_config.NumberColumn(format="%.1f%%"),
        }

        st.dataframe(
            subset,
            use_container_width=True,
            height=400,
            column_config=column_config,
            hide_index=True,
        )
    else:
        st.info("Sem anúncios para este bairro com os filtros selecionados.")

# ── Methodology explanation ────────────────────────────────────────────────────
with st.sidebar:
    with st.expander("Como funciona o Score?", expanded=False):
        st.markdown(
            """
            **Score de Oportunidade**

            Cada anúncio é comparado com os demais do mesmo bairro,
            tipo de imóvel e modalidade (venda/aluguel).

            O score combina dois z-scores:
            - **Preço absoluto** (peso 35%)
            - **Preço por m²** (peso 65%)

            Um score **positivo** significa que o imóvel está
            abaixo da média do bairro — quanto maior, melhor.

            Um score **negativo** indica preço acima da média.

            *Mínimo de 3 anúncios por bairro para o cálculo.*
            """
        )
