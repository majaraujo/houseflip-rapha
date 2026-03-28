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

from app.components.login import render_login, render_logout_button
from app.config import APP_ICON, APP_TITLE

st.set_page_config(
    page_title=APP_TITLE,
    page_icon=APP_ICON,
    layout="wide",
    initial_sidebar_state="expanded",
)

# Mobile-responsive global CSS
st.markdown(
    """
    <style>
    /* Padding reduzido em telas pequenas */
    @media (max-width: 768px) {
        .block-container {
            padding-left: 1rem !important;
            padding-right: 1rem !important;
            padding-top: 1rem !important;
        }
        /* Botões ocupam largura total no mobile */
        div[data-testid="stButton"] > button {
            width: 100%;
        }
    }

    /* Tabelas com scroll horizontal no mobile */
    div[data-testid="stDataFrame"] {
        overflow-x: auto;
    }

    /* Inputs com tamanho adequado para toque */
    input, select, textarea {
        font-size: 16px !important; /* Evita zoom automático no iOS */
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Autenticação — bloqueia o app se não estiver logado
if not render_login():
    st.stop()

# Botão de logout na sidebar
render_logout_button()

pages = {
    APP_TITLE: [
        st.Page("pages/01_scraping.py", title="Scraping", icon=":material/travel_explore:"),
        st.Page("pages/02_analysis.py", title="Análise de Preços", icon=":material/trending_down:"),
    ]
}

pg = st.navigation(pages)
pg.run()
