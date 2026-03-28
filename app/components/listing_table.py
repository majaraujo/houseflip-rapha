"""Reusable component: renders a listings DataFrame as a formatted st.dataframe."""

import polars as pl
import streamlit as st


def render_listing_table(df: pl.DataFrame | list[dict], height: int = 400) -> None:
    if isinstance(df, list):
        if not df:
            st.info("Nenhum anúncio encontrado.")
            return
        df = pl.DataFrame(df)

    if df.is_empty():
        st.info("Nenhum anúncio encontrado.")
        return

    # Select and rename display columns
    display_cols = {
        "title": "Título",
        "neighborhood": "Bairro",
        "city": "Cidade",
        "price_brl": "Preço (R$)",
        "area_m2": "Área (m²)",
        "price_per_m2": "R$/m²",
        "bedrooms": "Quartos",
        "bathrooms": "Banheiros",
        "parking_spots": "Vagas",
        "source": "Fonte",
        "url": "Link",
    }

    available = [c for c in display_cols if c in df.columns]
    subset = df.select(available).rename({k: v for k, v in display_cols.items() if k in available})

    column_config: dict = {}
    if "Link" in subset.columns:
        column_config["Link"] = st.column_config.LinkColumn("Link", display_text="Abrir")
    if "Preço (R$)" in subset.columns:
        column_config["Preço (R$)"] = st.column_config.NumberColumn(
            "Preço (R$)", format="R$ %.2f"
        )
    if "R$/m²" in subset.columns:
        column_config["R$/m²"] = st.column_config.NumberColumn("R$/m²", format="R$ %.2f")

    st.dataframe(
        subset,
        use_container_width=True,
        height=height,
        column_config=column_config,
        hide_index=True,
    )
