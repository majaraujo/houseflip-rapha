"""Reusable component: opportunity score chart per neighborhood."""

import altair as alt
import polars as pl
import streamlit as st

from app.config import COLOR_OPPORTUNITY_BAD, COLOR_OPPORTUNITY_GOOD


def render_opportunity_chart(summary_df: pl.DataFrame) -> None:
    """Renders a horizontal bar chart of best opportunity score per neighborhood."""
    if summary_df.is_empty():
        st.info("Sem dados suficientes para exibir o gráfico. Execute o scraping primeiro.")
        return

    df = summary_df.to_pandas()

    # Clip extreme scores for visualization
    df["best_opportunity_score"] = df["best_opportunity_score"].clip(-3, 3)

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(
                "best_opportunity_score:Q",
                title="Score de Oportunidade (maior = melhor negócio)",
                axis=alt.Axis(format=".2f"),
            ),
            y=alt.Y(
                "neighborhood:N",
                sort="-x",
                title="Bairro",
            ),
            color=alt.condition(
                alt.datum.best_opportunity_score > 0,
                alt.value(COLOR_OPPORTUNITY_GOOD),
                alt.value(COLOR_OPPORTUNITY_BAD),
            ),
            tooltip=[
                alt.Tooltip("neighborhood:N", title="Bairro"),
                alt.Tooltip("listing_count:Q", title="Anúncios"),
                alt.Tooltip("best_opportunity_score:Q", title="Melhor Score", format=".2f"),
                alt.Tooltip("median_price:Q", title="Preço Mediano (R$)", format=",.2f"),
                alt.Tooltip("best_pct_vs_median:Q", title="% vs Mediana", format=".1f"),
            ],
        )
        .properties(height=max(200, len(df) * 28))
    )

    st.altair_chart(chart, use_container_width=True)


def render_opportunity_score_badge(score: float) -> str:
    """Returns a colored markdown badge for the opportunity score."""
    if score >= 1.5:
        return f"🟢 **{score:.2f}** (ótimo negócio)"
    elif score >= 0.5:
        return f"🟡 **{score:.2f}** (acima da média)"
    elif score >= -0.5:
        return f"⚪ **{score:.2f}** (preço justo)"
    else:
        return f"🔴 **{score:.2f}** (acima do mercado)"
