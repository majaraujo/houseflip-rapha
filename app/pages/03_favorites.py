"""Page 3 — Favoritos: anúncios marcados como favoritos."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import polars as pl
import streamlit as st

from houseflip.storage.database import get_db
from houseflip.storage.repository import ListingRepository

st.title(":material/favorite: Anúncios Favoritos")
st.caption("Imóveis que você marcou como favoritos.")

db = get_db()
repo = ListingRepository(db)
favorites = repo.query_favorites()

if not favorites:
    st.info("Você ainda não favoritou nenhum anúncio. Na página de **Scraping** ou **Análise**, clique no ★ ao lado de um anúncio.")
    st.stop()

st.metric("Total de favoritos", len(favorites))
st.divider()

df = pl.DataFrame(favorites)

for row in df.iter_rows(named=True):
    with st.container(border=True):
        col_info, col_btn = st.columns([5, 1])

        with col_info:
            title = row.get("title") or "Sem título"
            st.markdown(f"**{title}**")

            parts = []
            if row.get("neighborhood"):
                parts.append(row["neighborhood"])
            if row.get("city"):
                parts.append(row["city"])
            if parts:
                st.caption(" · ".join(parts))

            metrics_cols = st.columns(4)
            if row.get("price_brl"):
                metrics_cols[0].metric("Preço", f"R$ {row['price_brl']:,.0f}")
            if row.get("area_m2"):
                metrics_cols[1].metric("Área", f"{row['area_m2']:.0f} m²")
            if row.get("bedrooms"):
                metrics_cols[2].metric("Quartos", row["bedrooms"])
            if row.get("parking_spots"):
                metrics_cols[3].metric("Vagas", row["parking_spots"])

            if row.get("url"):
                st.link_button("Abrir anúncio", row["url"])

        with col_btn:
            if st.button("★ Remover", key=f"unfav_{row['id']}", use_container_width=True):
                repo.toggle_favorite(row["id"])
                st.rerun()
