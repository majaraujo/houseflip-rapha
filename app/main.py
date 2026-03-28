"""Houseflip Streamlit app entrypoint.

Run with:
    streamlit run app/main.py
"""

import logging
import os
import sys
from pathlib import Path

# Make the src package and app package importable when running from the project root
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

load_dotenv(project_root / ".env")

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

import streamlit as st

from app.config import APP_ICON, APP_TITLE

st.set_page_config(
    page_title=APP_TITLE,
    page_icon=APP_ICON,
    layout="wide",
    initial_sidebar_state="expanded",
)

pages = {
    APP_TITLE: [
        st.Page("pages/01_scraping.py", title="Scraping", icon=":material/travel_explore:"),
        st.Page("pages/02_analysis.py", title="Análise de Preços", icon=":material/trending_down:"),
    ]
}

pg = st.navigation(pages)
pg.run()
