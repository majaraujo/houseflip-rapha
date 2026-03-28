"""Login component with session-state-based authentication and cookie persistence."""

import hashlib
import hmac
import secrets
from pathlib import Path

import streamlit as st
from streamlit_cookies_controller import CookieController

_LOGO_PATH = Path(__file__).parent.parent / "static" / "logo.png"
_COOKIE_NAME = "houseflip_session"
_COOKIE_MAX_AGE = 60 * 60 * 24 * 30  # 30 dias em segundos


def _cookie() -> CookieController:
    if "cookie_controller" not in st.session_state:
        st.session_state["cookie_controller"] = CookieController()
    return st.session_state["cookie_controller"]


def _check_credentials(username: str, password: str) -> bool:
    """Validate credentials against users table in Streamlit secrets."""
    try:
        expected_hash = st.secrets["users"][username]["password_hash"]
    except KeyError:
        return False
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    return hmac.compare_digest(password_hash, expected_hash)


def _make_token(username: str) -> str:
    """Generate a signed session token: username|random_token."""
    token = secrets.token_urlsafe(32)
    return f"{username}|{token}"


def _parse_token(token: str) -> str | None:
    """Extract username from session token. Returns None if invalid."""
    if not token or "|" not in token:
        return None
    username = token.split("|")[0]
    return username if username else None


def render_login() -> bool:
    """Render login if not authenticated. Returns True when authenticated."""
    controller = _cookie()

    # Já autenticado nesta sessão
    if st.session_state.get("authenticated"):
        return True

    # Verifica cookie de sessão persistente
    session_token = controller.get(_COOKIE_NAME)
    if session_token:
        username = _parse_token(session_token)
        if username:
            st.session_state["authenticated"] = True
            st.session_state["auth_username"] = username
            return True

    # Exibe tela de login
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"] { display: none; }
        [data-testid="collapsedControl"] { display: none; }
        section.main > div {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            min-height: 85vh;
        }
        div[data-testid="stForm"] {
            width: 100%;
            max-width: 420px;
            padding: 2rem 2.5rem;
            border: 1px solid #e0e0e0;
            border-radius: 16px;
            background: #ffffff;
            box-shadow: 0 4px 24px rgba(0,0,0,0.07);
        }
        @media (max-width: 480px) {
            div[data-testid="stForm"] {
                padding: 1.5rem 1rem;
                border: none;
                box-shadow: none;
                background: transparent;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    with st.form("login_form"):
        if _LOGO_PATH.exists():
            col_l, col_img, col_r = st.columns([1, 2, 1])
            with col_img:
                st.image(str(_LOGO_PATH), use_container_width=True)
        else:
            st.markdown("### ImobiSpy")
            st.caption("Inteligência imobiliária para houseflipping")

        st.divider()
        username = st.text_input("Usuário", placeholder="seu usuário")
        password = st.text_input("Senha", type="password", placeholder="sua senha")
        submitted = st.form_submit_button("Entrar", use_container_width=True, type="primary")

    if submitted:
        if _check_credentials(username, password):
            token = _make_token(username)
            controller.set(_COOKIE_NAME, token, max_age=_COOKIE_MAX_AGE)
            st.session_state["authenticated"] = True
            st.session_state["auth_username"] = username
            st.rerun()
        else:
            st.error("Usuário ou senha incorretos.")

    return False


def render_logout_button() -> None:
    """Render a logout button in the sidebar."""
    username = st.session_state.get("auth_username", "")
    st.sidebar.divider()
    st.sidebar.caption(f"Conectado como **{username}**")
    if st.sidebar.button("Sair", use_container_width=True):
        _cookie().remove(_COOKIE_NAME)
        st.session_state.clear()
        st.rerun()
