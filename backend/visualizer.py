
import plotly.graph_objects as go
import streamlit as st
import pandas as pd
import numpy as np

def render_universe(df, current_year):
    if df is None or df.height == 0:
        st.warning("No data to visualize.")
        return

    # 2. Calculate positions for the selected year
    delta_t = current_year - 2016.0
    
    import polars as pl
    
    if abs(delta_t) > 0.1:
        df_view = df.with_columns([
            (pl.col("x0") + pl.col("v_lx") * delta_t).alias("real_x"),
            (pl.col("y0") + pl.col("v_ly") * delta_t).alias("real_y"),
            (pl.col("z0") + pl.col("v_lz") * delta_t).alias("real_z"),
        ])
    else:
        df_view = df.with_columns([
            pl.col("x0").alias("real_x"),
            pl.col("y0").alias("real_y"),
            pl.col("z0").alias("real_z"),
            pl.col("source_id")
        ])
        
    pdf = df_view.select(["real_x", "real_y", "real_z", "phot_g_mean_mag", "distance_ly", "source_id"]).to_pandas()
    
    # Plotly Visualization
    
    # Normalize Magnitude for Sizing
    min_mag = pdf['phot_g_mean_mag'].min()
    max_mag = pdf['phot_g_mean_mag'].max()
    
    if max_mag == min_mag:
        norm_mag = np.zeros(len(pdf)) + 0.5
    else:
         # Invert so brighter (lower mag) is higher value (1.0)
        norm_mag = 1.0 - (pdf['phot_g_mean_mag'] - min_mag) / (max_mag - min_mag)
    
    # Size tuning for 25k stars
    # Brightest: Size 5
    # Dimmest: Size 1
    sizes = norm_mag * 4 + 1 
    
    # Color: White (bright) to Blue/Grey (dim)
    colors = []
    for nm in norm_mag:
        r = int(100 + 155 * nm)
        g = int(100 + 155 * nm)
        b = 255
        a = 0.3 + 0.7 * nm # More transparent for dim stars
        colors.append(f"rgba({r},{g},{b},{a})")

    fig = go.Figure()

    # Stars
    fig.add_trace(go.Scatter3d(
        x=pdf["real_x"],
        y=pdf["real_y"],
        z=pdf["real_z"],
        mode='markers',
        marker=dict(
            size=sizes,
            color=colors,
            line=dict(width=0),
            opacity=0.8
        ),
        text=[f"ID: {sid}<br>Mag: {m:.2f}<br>Dist: {d:.1f} ly" for sid, m, d in zip(pdf["source_id"], pdf["phot_g_mean_mag"], pdf["distance_ly"])],
        hoverinfo='text',
        name='Stars'
    ))

    # Earth
    fig.add_trace(go.Scatter3d(
        x=[0], y=[0], z=[0],
        mode='markers',
        marker=dict(size=5, color='red', symbol='circle'),
        name='Earth',
        hoverinfo='name'
    ))

    # Layout
    fig.update_layout(
        scene=dict(
            xaxis=dict(title='X (LY)', backgroundcolor="black", gridcolor="#333", showbackground=True, zerolinecolor="#666"),
            yaxis=dict(title='Y (LY)', backgroundcolor="black", gridcolor="#333", showbackground=True, zerolinecolor="#666"),
            zaxis=dict(title='Z (LY)', backgroundcolor="black", gridcolor="#333", showbackground=True, zerolinecolor="#666"),
            aspectmode='cube',
            bgcolor="black"
        ),
        margin=dict(l=0, r=0, b=0, t=0),
        paper_bgcolor="black",
        height=800,
        showlegend=False,
        title=dict(text=f"Year {current_year:.0f} | {len(pdf)} Stars Loaded", x=0.5, y=0.95, font=dict(color="white"))
    )

    st.plotly_chart(fig, width="stretch", key="universe_plot", on_select="ignore")
