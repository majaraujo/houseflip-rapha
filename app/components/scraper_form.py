"""Reusable component: scraper configuration form."""

import streamlit as st

from app.config import LISTING_TYPE_OPTIONS, PROPERTY_TYPE_OPTIONS, SOURCE_OPTIONS
from houseflip.models.listing import ListingSource, ListingType, PropertyType
from houseflip.models.scrape_config import ScrapeJob


def render_scraper_form() -> ScrapeJob | None:
    """Renders the scraping configuration form. Returns a ScrapeJob or None."""

    with st.form("scraper_form"):
        st.subheader("Configurar Scraping")

        col1, col2 = st.columns(2)
        with col1:
            city = st.text_input("Cidade", value="São Paulo", placeholder="Ex: São Paulo")
            state = st.text_input("Estado (sigla)", value="sp", placeholder="Ex: sp, rj, mg")
            neighborhood = st.text_input(
                "Bairro (opcional)", placeholder="Ex: Pinheiros"
            )
            source_label = st.selectbox("Fonte", list(SOURCE_OPTIONS.keys()))
            if source_label == "Chaves na Mão":
                st.caption("ℹ️ Substituto do ImovelWeb (bloqueado por Cloudflare). Retorna ~15 anúncios por busca.")

        with col2:
            listing_type_label = st.radio(
                "Tipo", list(LISTING_TYPE_OPTIONS.keys()), horizontal=True
            )
            property_type_label = st.selectbox(
                "Tipo de imóvel", list(PROPERTY_TYPE_OPTIONS.keys())
            )
            is_chavesnamao = source_label == "Chaves na Mão"
            if is_chavesnamao:
                st.caption("Máximo de páginas: **1** (limitação da fonte)")
                max_pages = 1
            else:
                max_pages = st.slider("Máximo de páginas", min_value=1, max_value=20, value=3)

        submitted = st.form_submit_button("Iniciar Scraping", type="primary", use_container_width=True)

    if submitted:
        if not city.strip():
            st.error("Informe a cidade.")
            return None

        return ScrapeJob(
            source=ListingSource(SOURCE_OPTIONS[source_label]),
            city=city.strip(),
            state=state.strip().lower() or "sp",
            neighborhood=neighborhood.strip() or None,
            listing_type=ListingType(LISTING_TYPE_OPTIONS[listing_type_label]),
            property_type=PropertyType(PROPERTY_TYPE_OPTIONS[property_type_label]),
            max_pages=max_pages,
        )

    return None
