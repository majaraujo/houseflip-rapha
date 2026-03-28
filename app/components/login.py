"""Login component with session-state-based authentication."""

import hashlib
import hmac
from pathlib import Path

import streamlit as st

_LOGO_PATH = Path(__file__).parent.parent / "static" / "logo.png"


def _check_credentials(username: str, password: str) -> bool:
    """Validate credentials against users table in Streamlit secrets.

    Expected secrets format:
        [users.joao]
        password_hash = "sha256_hash_here"
    """
    try:
        expected_hash = st.secrets["users"][username]["password_hash"]
    except KeyError:
        return False
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    return hmac.compare_digest(password_hash, expected_hash)


def render_login() -> bool:
    """Render the login screen. Returns True if the user is authenticated."""
    if st.session_state.get("authenticated"):
        return True

    st.markdown(
        """
        <style>
        /* Esconde sidebar na tela de login */
        [data-testid="stSidebar"] { display: none; }
        [data-testid="collapsedControl"] { display: none; }

        /* Centraliza conteúdo verticalmente */
        section.main > div {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            min-height: 85vh;
        }

        /* Card do formulário */
        div[data-testid="stForm"] {
            width: 100%;
            max-width: 420px;
            padding: 2rem 2.5rem;
            border: 1px solid #e0e0e0;
            border-radius: 16px;
            background: #ffffff;
            box-shadow: 0 4px 24px rgba(0,0,0,0.07);
        }

        /* Mobile: remove bordas e padding lateral */
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
        # Logo
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
        submitted = st.form_submit_button(
            "Entrar", use_container_width=True, type="primary"
        )

    if submitted:
        import hashlib
        pwd_hash = hashlib.sha256(password.encode()).hexdigest()
        try:
            stored = st.secrets["users"][username]["password_hash"]
        except Exception as e:
            stored = f"ERRO: {e}"
        st.code(f"usuario digitado : '{username}'\nhash gerado      : {pwd_hash}\nhash no secrets  : {stored}\nbateu?           : {pwd_hash == stored}")

        if _check_credentials(username, password):
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
        st.session_state.clear()
        st.rerun()
