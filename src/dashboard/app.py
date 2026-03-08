import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
from pathlib import Path

# Encontramos la raíz del proyecto y configuramos las rutas
def find_project_root(start: Path) -> Path:
    for parent in [start] + list(start.parents):
        if (parent / "README.md").exists() and (parent / "src").exists():
            return parent
    raise FileNotFoundError("No se encontró la raíz del proyecto")

PROJECT_ROOT = find_project_root(Path(__file__).resolve())
GOLD_PATH = PROJECT_ROOT / "data/aggregated/gold/metrics"

# Cargamos los Parquet Gold directamente con Pandas+PyArrow
# No necesitamos Spark aquí, los datos Gold ya son pequeños
df_hora = pd.read_parquet(GOLD_PATH / "fraude_por_hora")
df_tarjeta = pd.read_parquet(GOLD_PATH / "fraude_por_tipo_tarjeta")
df_producto = pd.read_parquet(GOLD_PATH / "fraude_por_categoria_producto")
df_mensual = pd.read_parquet(GOLD_PATH / "evolucion_mensual_fraudes")

print(f"Datos cargados ✅ | Horas: {len(df_hora)} | Tarjetas: {len(df_tarjeta)}")

app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.DARKLY,
        "https://fonts.googleapis.com/css2?family=Oxygen:wght@300;400;700&display=swap"
    ],  # tema oscuro con colores más vivos
    title="Dashboard de Fraude con Spark y Dash"
)

# Gráfica 1: Fraude por hora
fig_hora = px.bar(
    df_hora,
    x="transaction_hour",
    y="pct_fraude",
    color="pct_fraude",
    color_continuous_scale="Reds",
    title="% Fraude por hora del día",
    labels={"transaction_hour": "Hora", "pct_fraude": "% Fraude"}
)
fig_hora.update_layout(template="plotly_dark", showlegend=False)

# Gráfica 2: Fraude por tarjeta
df_tarjeta_filtrado = df_tarjeta[df_tarjeta["total_transacciones"] > 100].copy()
df_tarjeta_filtrado["etiqueta"] = df_tarjeta_filtrado["red_tarjeta"] + " " + df_tarjeta_filtrado["tipo_tarjeta"]

fig_tarjeta = px.bar(
    df_tarjeta_filtrado.sort_values("pct_fraude", ascending=True),
    x="pct_fraude",
    y="etiqueta",
    orientation="h",
    color="pct_fraude",
    color_continuous_scale="Reds",
    title="% Fraude por tipo de tarjeta",
    labels={"pct_fraude": "% Fraude", "etiqueta": "Tarjeta"}
)
fig_tarjeta.update_layout(template="plotly_dark", showlegend=False)

# Gráfica 3: Fraude por producto
fig_producto = px.scatter(
    df_producto,
    x="importe_medio",
    y="pct_fraude",
    size="total_transacciones",
    color="pct_fraude",
    text="ProductCD",
    color_continuous_scale="Reds",
    title="Fraude por producto: % fraude vs importe medio",
    labels={"importe_medio": "Importe medio ($)", "pct_fraude": "% Fraude"}
)
fig_producto.update_traces(textposition="top center")
fig_producto.update_layout(template="plotly_dark", showlegend=False)

# Gráfica 4: Evolución mensual
fig_mensual = px.line(
    df_mensual,
    x="periodo",
    y=["total_transacciones", "total_fraudes"],
    title="Evolución mensual: transacciones vs fraudes (Dic 2017 - May 2018)",
    labels={"periodo": "Mes", "value": "Cantidad"},
    markers=True
)
fig_mensual.update_layout(template="plotly_dark")

# KPIs
total_transacciones = df_hora["total_transacciones"].sum()
total_fraudes = df_hora["total_fraudes"].sum()
pct_fraude_global = round(total_fraudes * 100 / total_transacciones, 2)
importe_medio = round(df_producto["importe_medio"].mean(), 2)

def kpi_card(titulo, valor, color):
    return dbc.Card(
        dbc.CardBody([
            html.H6(titulo, className="text-muted"),
            html.H3(valor, style={"color": color})
        ]),
        className="text-center shadow"
    )

app.layout = dbc.Container([

    # Header
    dbc.Row([
        dbc.Col(html.H2("🏦 Dashboard Sobre Fraude con datos obtenidos de Kaggle",
                className="text-white text-center my-4"))
    ]),

    # KPI cards
    dbc.Row([
        dbc.Col(kpi_card("Total Transacciones", f"{total_transacciones:,}", "#17a2b8"), md=3),
        dbc.Col(kpi_card("Total Fraudes", f"{total_fraudes:,}", "#dc3545"), md=3),
        dbc.Col(kpi_card("% Fraude Global", f"{pct_fraude_global}%", "#ffc107"), md=3),
        dbc.Col(kpi_card("Importe Medio", f"${importe_medio}", "#28a745"), md=3),
    ], className="mb-4"),

    # Fila 1: hora + tarjeta
    dbc.Row([
        dbc.Col(dcc.Graph(figure=fig_hora), md=6),
        dbc.Col(dcc.Graph(figure=fig_tarjeta), md=6),
    ], className="mb-4"),

    # Fila 2: producto + mensual
    dbc.Row([
        dbc.Col(dcc.Graph(figure=fig_producto), md=5),
        dbc.Col(dcc.Graph(figure=fig_mensual), md=7),
    ], className="mb-4"),

], fluid=True, style={"backgroundColor": "#1a1a2e", "minHeight": "100vh","fontFamily": "'Oxygen', sans-serif"})

# --- Arrancar servidor ---
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)