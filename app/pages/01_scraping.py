"""Page 1 — Scraping: configure and run scrapers, view results."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st

from app.components.listing_table import render_listing_table
from app.components.scraper_form import render_scraper_form
from houseflip.services.scrape_service import ScrapeService
from houseflip.storage.database import get_db
from houseflip.storage.repository import ListingRepository


def _get_db_stats() -> dict:
    db = get_db()
    repo = ListingRepository(db)
    runs = repo.list_scrape_runs(limit=5)
    total = repo.total_listings()
    return {"total": total, "runs": runs}


st.title(":material/travel_explore: Scraping de Anúncios")
st.caption("Configure e execute o scraping de imóveis nos principais portais.")


# ── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Configurações")
    job = render_scraper_form()

    st.divider()
    st.subheader("Gerenciamento")
    if st.button("Limpar todos os anúncios", use_container_width=True, type="secondary"):
        st.session_state["confirm_clear"] = True

    if st.session_state.get("confirm_clear"):
        st.warning("Isso apagará **todos** os anúncios e histórico de scraping. Confirma?")
        col_yes, col_no = st.columns(2)
        with col_yes:
            if st.button("Sim, apagar", type="primary", use_container_width=True):
                db = get_db()
                repo = ListingRepository(db)
                deleted = repo.clear_all_listings()
                st.session_state.pop("confirm_clear", None)
                st.session_state.pop("last_scrape_listings", None)
                st.session_state.pop("last_scrape_total", None)
                st.success(f"{deleted} anúncios removidos.")
                st.rerun()
        with col_no:
            if st.button("Cancelar", use_container_width=True):
                st.session_state.pop("confirm_clear", None)
                st.rerun()

# ── Database stats ────────────────────────────────────────────────────────────
stats = _get_db_stats()

col1, col2, col3 = st.columns(3)
col1.metric("Total de anúncios", f"{stats['total']:,}")
col2.metric("Execuções recentes", len(stats["runs"]))
col3.metric(
    "Última execução",
    stats["runs"][0]["status"].upper() if stats["runs"] else "—",
)

st.divider()

# ── Scraping execution ────────────────────────────────────────────────────────
if job is not None:
    st.subheader(f"Executando scraping — {job.source.value} / {job.city}")

    progress_bar = st.progress(0, text="Iniciando...")
    status_box = st.status("Coletando anúncios...", expanded=True)
    results_placeholder = st.empty()

    db = get_db()
    service = ScrapeService(db)

    all_listings: list[dict] = []
    pages_done = 0

    with status_box:
        for update in service.run(job):
            if update.get("error"):
                st.error(f"Erro: {update['error']}")
                break

            pages_done = update["page"]
            all_listings.extend(update["listings"])
            total = update["total"]

            progress = min(pages_done / job.max_pages, 1.0)
            progress_bar.progress(
                progress,
                text=f"Página {pages_done}/{job.max_pages} — {total} anúncios coletados",
            )
            st.write(f"Página {pages_done}: +{update['found']} anúncios")

        st.success(f"Concluído! {len(all_listings)} anúncios coletados em {pages_done} página(s).")

    progress_bar.progress(1.0, text="Concluído!")
    st.session_state["last_scrape_listings"] = all_listings
    st.session_state["last_scrape_total"] = len(all_listings)

    st.subheader("Anúncios coletados nesta execução")
    render_listing_table(all_listings)

# ── Show last session results if no new scrape ────────────────────────────────
elif "last_scrape_listings" in st.session_state and st.session_state["last_scrape_listings"]:
    st.subheader(
        f"Última execução — {st.session_state.get('last_scrape_total', 0)} anúncios"
    )
    render_listing_table(st.session_state["last_scrape_listings"])
else:
    st.info("Configure o scraping na barra lateral e clique em **Iniciar Scraping**.")

# ── Recent scrape runs ─────────────────────────────────────────────────────────
if stats["runs"]:
    with st.expander("Histórico de execuções"):
        import polars as pl

        runs_df = pl.DataFrame(stats["runs"])
        st.dataframe(runs_df, use_container_width=True, hide_index=True)
