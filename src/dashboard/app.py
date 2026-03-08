import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from pathlib import Path
import os

# --- Localizar raíz del proyecto ---
def find_project_root(start: Path) -> Path:
    env_root = os.environ.get("PROJECT_ROOT")
    if env_root:
        return Path(env_root)
    for parent in [start] + list(start.parents):
        if (parent / "README.md").exists() and (parent / "src").exists():
            return parent
    raise FileNotFoundError("No se encontró la raíz del proyecto")

PROJECT_ROOT = find_project_root(Path(__file__).resolve())
GOLD_PATH = PROJECT_ROOT / "data/aggregated/gold/metrics"

# --- Cargar datos Gold ---
df_hora     = pd.read_parquet(GOLD_PATH / "fraude_por_hora")
df_tarjeta  = pd.read_parquet(GOLD_PATH / "fraude_por_tipo_tarjeta")
df_producto = pd.read_parquet(GOLD_PATH / "fraude_por_categoria_producto")
df_mensual  = pd.read_parquet(GOLD_PATH / "evolucion_mensual_fraudes")

print(f"Datos cargados ✅ | Horas: {len(df_hora)} | Tarjetas: {len(df_tarjeta)}")

# --- Paleta corporativa ---
BG_MAIN    = "#0f1d2e"
BG_CARD    = "#162438"
BG_CARD2   = "#0b1622"
BORDER     = "#233652"
BLUE       = "#4a90e2"
BLUE_DARK  = "#2c6ab5"
TEXT_MAIN  = "#ffffff"
TEXT_MUTED = "#b0b8c4"
RED        = "#e25555"
GREEN      = "#43c59e"
YELLOW     = "#f0c040"

PLOTLY_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Oxygen, sans-serif", color=TEXT_MUTED, size=12),
    title_font=dict(family="Oxygen, sans-serif", color=TEXT_MAIN, size=15),
    margin=dict(l=16, r=16, t=48, b=16),
    coloraxis_showscale=False,
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=TEXT_MUTED)),
    xaxis=dict(gridcolor=BORDER, linecolor=BORDER, tickfont=dict(color=TEXT_MUTED)),
    yaxis=dict(gridcolor=BORDER, linecolor=BORDER, tickfont=dict(color=TEXT_MUTED)),
)

# --- Gráfica 1: Fraude por hora ---
fig_hora = px.bar(
    df_hora,
    x="transaction_hour",
    y="pct_fraude",
    color="pct_fraude",
    color_continuous_scale=[[0, BLUE_DARK], [0.5, BLUE], [1, RED]],
    labels={"transaction_hour": "Hora del día", "pct_fraude": "% Fraude"},
)
fig_hora.update_layout(**PLOTLY_LAYOUT, title="% Fraude por hora del día")
fig_hora.update_traces(marker_line_width=0)

# --- Gráfica 2: Fraude por tarjeta ---
df_tarjeta_f = df_tarjeta[df_tarjeta["total_transacciones"] > 100].copy()
df_tarjeta_f["etiqueta"] = df_tarjeta_f["red_tarjeta"].str.title() + "  ·  " + df_tarjeta_f["tipo_tarjeta"].str.title()
df_tarjeta_f = df_tarjeta_f.sort_values("pct_fraude", ascending=True).reset_index(drop=True)  # ← añadir esto

fig_tarjeta = px.bar(
    df_tarjeta_f,
    x="pct_fraude",
    y="etiqueta",
    orientation="h",
    color="pct_fraude",
    color_continuous_scale=[[0, BLUE_DARK], [0.5, BLUE], [1, RED]],
    labels={"pct_fraude": "% Fraude", "etiqueta": ""},
    text=[f"{x}%" for x in df_tarjeta_f["pct_fraude"]],  # ← también cambiar a lista
)

fig_tarjeta.update_layout(
    **PLOTLY_LAYOUT,
    title="% Fraude por tipo de tarjeta",
    yaxis=dict(gridcolor=BORDER, linecolor=BORDER, tickfont=dict(color=TEXT_MAIN, size=11)),
)
fig_tarjeta.update_traces(textposition="outside", textfont=dict(color=TEXT_MUTED, size=11), marker_line_width=0)

# --- Gráfica 3: Fraude por producto ---
fig_producto = px.scatter(
    df_producto,
    x="importe_medio",
    y="pct_fraude",
    size="total_transacciones",
    color="pct_fraude",
    text="ProductCD",
    color_continuous_scale=[[0, BLUE], [1, RED]],
    labels={"importe_medio": "Importe medio ($)", "pct_fraude": "% Fraude", "total_transacciones": "Volumen"},
    size_max=60,
)
fig_producto.update_traces(
    textposition="top center",
    textfont=dict(color=TEXT_MAIN, size=13, family="Oxygen, sans-serif"),
    marker=dict(line=dict(width=1, color=BORDER)),
)
fig_producto.update_layout(**PLOTLY_LAYOUT, title="Fraude por producto: % vs importe medio")

# --- Gráfica 4: Evolución mensual ---
fig_mensual = go.Figure()
fig_mensual.add_trace(go.Scatter(
    x=df_mensual["periodo"], y=df_mensual["total_transacciones"],
    name="Transacciones", mode="lines+markers",
    line=dict(color=BLUE, width=2),
    marker=dict(size=7, color=BLUE, line=dict(width=1, color=BG_CARD)),
))
fig_mensual.add_trace(go.Scatter(
    x=df_mensual["periodo"], y=df_mensual["total_fraudes"],
    name="Fraudes", mode="lines+markers",
    line=dict(color=RED, width=2),
    marker=dict(size=7, color=RED, line=dict(width=1, color=BG_CARD)),
))
fig_mensual.update_layout(
    **PLOTLY_LAYOUT,
    title="Evolución mensual: transacciones vs fraudes",
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=TEXT_MUTED), orientation="h", y=1.12),
)

# --- KPIs ---
total_transacciones = int(df_hora["total_transacciones"].sum())
total_fraudes       = int(df_hora["total_fraudes"].sum())
pct_fraude_global   = round(total_fraudes * 100 / total_transacciones, 2)
importe_medio       = round(df_producto["importe_medio"].mean(), 2)

# --- Componentes ---
def kpi_card(icono, titulo, valor, color):
    return dbc.Col(
        html.Div([
            html.Div(icono, style={
                "fontSize": "1.6rem", "marginBottom": "6px"
            }),
            html.Div(titulo, style={
                "fontSize": "0.75rem", "color": TEXT_MUTED,
                "textTransform": "uppercase", "letterSpacing": "0.08em",
                "marginBottom": "4px"
            }),
            html.Div(valor, style={
                "fontSize": "1.7rem", "fontWeight": "700",
                "color": color, "lineHeight": "1"
            }),
        ], style={
            "backgroundColor": BG_CARD,
            "border": f"1px solid {BORDER}",
            "borderTop": f"3px solid {color}",
            "borderRadius": "10px",
            "padding": "20px 16px",
            "textAlign": "center",
            "height": "100%",
        }),
        xs=6, md=3, className="mb-3"
    )

def graph_card(figure, min_height="340px"):
    return html.Div(
        dcc.Graph(
            figure=figure,
            config={"displayModeBar": False, "responsive": True},
            style={"minHeight": min_height},
        ),
        style={
            "backgroundColor": BG_CARD,
            "border": f"1px solid {BORDER}",
            "borderRadius": "10px",
            "padding": "8px",
            "height": "100%",
        }
    )

# --- App ---
app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://fonts.googleapis.com/css2?family=Oxygen:wght@300;400;700&display=swap",
    ],
    title="Financial Fraud Dashboard",
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)

app.layout = html.Div([

    # ── Header ──────────────────────────────────────────────
    html.Div([
        html.Div([
            html.Span("🏦", style={"fontSize": "1.6rem", "marginRight": "10px"}),
            html.Span("Financial Fraud", style={"color": TEXT_MAIN, "fontWeight": "700"}),
            html.Span(" Dashboard", style={"color": BLUE, "fontWeight": "300"}),
        ], style={"fontSize": "1.3rem", "fontFamily": "Oxygen, sans-serif"}),
        html.Div("IEEE-CIS · 590.540 transacciones · Dic 2017 – May 2018", style={
            "color": TEXT_MUTED, "fontSize": "0.78rem", "marginTop": "2px"
        }),
    ], style={
        "backgroundColor": BG_CARD2,
        "borderBottom": f"1px solid {BORDER}",
        "padding": "16px 24px",
    }),

    # ── Contenido principal ─────────────────────────────────
    html.Div([

        # KPIs
        dbc.Row([
            kpi_card("📊", "Transacciones", f"{total_transacciones:,}", BLUE),
            kpi_card("🚨", "Fraudes", f"{total_fraudes:,}", RED),
            kpi_card("📈", "% Fraude Global", f"{pct_fraude_global}%", YELLOW),
            kpi_card("💰", "Importe Medio", f"${importe_medio}", GREEN),
        ], className="mb-3"),

        # Fila 1: hora + tarjeta
        dbc.Row([
            dbc.Col(graph_card(fig_hora), xs=12, lg=6, className="mb-3"),
            dbc.Col(graph_card(fig_tarjeta, min_height="340px"), xs=12, lg=6, className="mb-3"),
        ]),

        # Fila 2: producto + mensual
        dbc.Row([
            dbc.Col(graph_card(fig_producto, min_height="340px"), xs=12, lg=5, className="mb-3"),
            dbc.Col(graph_card(fig_mensual, min_height="340px"), xs=12, lg=7, className="mb-3"),
        ]),

        # Footer
        html.Div([
            html.Span("Desarrollado por ", style={"color": TEXT_MUTED, "fontSize": "0.8rem"}),
            html.A("automaworks.es", href="https://automaworks.es", target="_blank", style={
                "color": BLUE, "fontSize": "0.8rem", "textDecoration": "none"
            }),
            html.Span("  ·  Pipeline ETL con PySpark · Arquitectura Medallion · Dash + Plotly",
                      style={"color": TEXT_MUTED, "fontSize": "0.8rem"}),
        ], style={
            "textAlign": "center", "padding": "20px 0 8px",
            "borderTop": f"1px solid {BORDER}", "marginTop": "8px"
        }),

    ], style={"padding": "24px 20px", "maxWidth": "1400px", "margin": "0 auto"}),

], style={
    "backgroundColor": BG_MAIN,
    "minHeight": "100vh",
    "fontFamily": "Oxygen, sans-serif",
})

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8050)