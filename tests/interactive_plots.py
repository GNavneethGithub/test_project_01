import numpy as np
import plotly.graph_objects as go
import dash
from dash import dcc, html, callback, Input, Output
import dash_bootstrap_components as dbc
import json
import os
from pathlib import Path
import hashlib
import pickle

# ============================================================================
# CONFIGURE YOUR PARAMETER RANGES
# ============================================================================
SLIDER_RANGES = {
    'f': {'min': 1.0, 'max': 10, 'step': 2, 'default': 1.0, 'label': 'f (frequency)', 'unit': 'Hz'},
    'psiM': {'min': 0, 'max': 80, 'step': 10, 'default': 60, 'label': 'psiM (roll amplitude)', 'unit': '°', 'convert_to_radians': True},
    'psiC': {'min': 1.0, 'max': 4.0, 'step': 0.5, 'default': 1.09, 'label': 'psiC (roll shape)', 'unit': ''},
    'Dopsi': {'min': -90, 'max': 90, 'step': 10, 'default': -90, 'label': 'Dopsi (roll phase)', 'unit': '°', 'convert_to_radians': True},
    'psi0': {'min': -90, 'max': 90, 'step': 10, 'default': 90, 'label': 'psi0 (roll base)', 'unit': '°', 'convert_to_radians': True},
    'thetaM': {'min': 0, 'max': 30, 'step': 10, 'default': 0, 'label': 'thetaM (pitch amplitude)', 'unit': '°', 'convert_to_radians': True},
    'Dotheta': {'min': -90, 'max': 90, 'step': 10, 'default': 0, 'label': 'Dotheta (pitch phase)', 'unit': '°', 'convert_to_radians': True},
    'thetaN': {'min': 1.0, 'max': 2, 'step': 1, 'default': 1.0, 'label': 'thetaN (pitch freq)', 'unit': ''},
    'theta0': {'min': -90, 'max': 90, 'step': 10, 'default': 0, 'label': 'theta0 (pitch base)', 'unit': '°', 'convert_to_radians': True},
    'phiM': {'min': 0, 'max': 90, 'step': 10, 'default': 90, 'label': 'phiM (yaw amplitude)', 'unit': '°', 'convert_to_radians': True},
    'phiK': {'min': 0.01, 'max': 0.8, 'step': 0.05, 'default': 0.14, 'label': 'phiK (yaw limit)', 'unit': ''},
    'Dophi': {'min': -90, 'max': 90, 'step': 10, 'default': 0, 'label': 'Dophi (yaw phase)', 'unit': '°', 'convert_to_radians': True},
    'phi0': {'min': -90, 'max': 90, 'step': 10, 'default': 0, 'label': 'phi0 (yaw base)', 'unit': '°', 'convert_to_radians': True}
}

# Cache directory
CACHE_DIR = Path('./animation_cache')
CACHE_DIR.mkdir(exist_ok=True)
METADATA_FILE = CACHE_DIR / 'metadata.json'

# ============================================================================
# CACHE MANAGEMENT
# ============================================================================

def load_metadata():
    """Load metadata about cached computations"""
    if METADATA_FILE.exists():
        with open(METADATA_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_metadata(metadata):
    """Save metadata"""
    with open(METADATA_FILE, 'w') as f:
        json.dump(metadata, f, indent=2)

def get_param_hash(slider_values):
    """
    Create unique hash from slider values
    This hash identifies each unique parameter set
    """
    param_str = json.dumps({k: float(v) for k, v in zip(SLIDER_RANGES.keys(), slider_values)})
    return hashlib.md5(param_str.encode()).hexdigest()

def cache_file_path(param_hash):
    """Get file path for cached frames"""
    return CACHE_DIR / f'frames_{param_hash}.pkl'

def metadata_file_path(param_hash):
    """Get file path for metadata"""
    return CACHE_DIR / f'meta_{param_hash}.json'

def is_cached(param_hash):
    """Check if computation exists in cache"""
    return cache_file_path(param_hash).exists()

def load_cached_frames(param_hash):
    """Load frames from disk"""
    try:
        with open(cache_file_path(param_hash), 'rb') as f:
            return pickle.load(f)
    except Exception as e:
        print(f"❌ Error loading cache: {e}")
        return None

def save_cached_frames(param_hash, frames_data, slider_values):
    """Save frames to disk"""
    try:
        # Save frames
        with open(cache_file_path(param_hash), 'wb') as f:
            pickle.dump(frames_data, f)
        
        # Save metadata
        metadata = {
            'param_hash': param_hash,
            'parameters': {k: float(v) for k, v in zip(SLIDER_RANGES.keys(), slider_values)},
            'num_frames': len(frames_data),
            'created': str(np.datetime64('now'))
        }
        
        with open(metadata_file_path(param_hash), 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"💾 Cached: {param_hash}")
        return True
    except Exception as e:
        print(f"❌ Error saving cache: {e}")
        return False

def get_cache_stats():
    """Get statistics about cache"""
    cached_files = list(CACHE_DIR.glob('frames_*.pkl'))
    cache_size = sum(f.stat().st_size for f in cached_files) / (1024 * 1024)  # MB
    
    return {
        'num_cached': len(cached_files),
        'size_mb': cache_size
    }

# ============================================================================
# FUNCTIONS
# ============================================================================

def xyz(t, f, psiM, psiC, Dopsi, psi0, thetaM, Dotheta, thetaN, theta0, phiM, phi0, phiK, Dophi):
    """Generate 3D parametric coordinates (Euler angles)"""
    psi = psi0 + psiM * (1.0 / np.tanh(psiC)) * np.tanh(psiC * np.sin(2 * f * np.pi * t + Dopsi))
    theta = theta0 + thetaM * np.cos(Dotheta + 2 * f * np.pi * t * thetaN)
    phi = phi0 + (phiM * np.arcsin(phiK * np.sin(2 * f * np.pi * t + Dophi))) / np.arcsin(phiK)
    return np.array([psi, theta, phi])

def tBW(angles):
    """Create rotation matrix from Euler angles"""
    xa, ya, za = angles
    cxa = np.cos(xa); cya = np.cos(ya); cza = np.cos(za)
    sxa = np.sin(xa); sya = np.sin(ya); sza = np.sin(za)
    
    return np.array([
        [cya * cza, cza * sxa * sya - cxa * sza, cxa * cza * sya + sxa * sza],
        [cya * sza, cxa * cza + sxa * sya * sza, -(cza * sxa) + cxa * sya * sza],
        [-sya, cya * sxa, cxa * cya]
    ])

# Geometry
k = 20
cmax = 1.0
Rmax = 1.5

def cr(r):
    return cmax * np.sqrt(1.0 - (1.0 - r**2 / Rmax**2)**2)

xPoints = np.linspace(0, Rmax, k)
yPoints = -cr(xPoints)
zPoints = np.zeros_like(xPoints)

rpwWListLeading = np.column_stack([xPoints, np.zeros_like(xPoints), np.zeros_like(xPoints)])
rpwWList = np.column_stack([xPoints, yPoints, zPoints])

def rpwBListLeading(angles):
    rotation_matrix = tBW(angles)
    return (rotation_matrix @ rpwWListLeading.T).T

def rpwBList(angles):
    rotation_matrix = tBW(angles)
    return (rotation_matrix @ rpwWList.T).T

def compute_frames(params, num_frames=150):
    """Compute all frames from t=0 to 1/f"""
    f = params['f']
    time_values = np.linspace(0, 1/f, num_frames)
    
    frames_data = {}
    for idx, t in enumerate(time_values):
        angles = xyz(t, params['f'], params['psiM'], params['psiC'], params['Dopsi'], 
                     params['psi0'], params['thetaM'], params['Dotheta'], params['thetaN'], 
                     params['theta0'], params['phiM'], params['phi0'], params['phiK'], params['Dophi'])
        
        pts1 = rpwBListLeading(angles)
        pts2 = rpwBList(angles)
        
        frames_data[idx] = {
            'pts1': pts1,
            'pts2': pts2,
            'angles': angles,
            't': float(t)
        }
    
    return frames_data

# ============================================================================
# DASH APP
# ============================================================================

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Build sliders
slider_components = []
for param_name, config in SLIDER_RANGES.items():
    slider_components.append(
        dbc.Col([
            html.Label(f"{config['label']}", className="fw-bold small"),
            dcc.Slider(
                id=f'slider-{param_name}',
                min=config['min'],
                max=config['max'],
                step=config['step'],
                value=config['default'],
                marks={config['min']: f"{config['min']:.2g}", config['max']: f"{config['max']:.2g}"},
                tooltip={"placement": "bottom", "always_visible": False}
            ),
            html.Div(id=f'value-{param_name}', className="text-center small mt-2")
        ], width=12, className="mb-3")
    )

# Layout
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2("🎬 3D Parametric Animation (FILE-CACHED)", className="mb-1"),
            html.P("💾 Persistent disk caching | Check file before recomputing", className="text-success fw-bold")
        ], width=12)
    ], className="mb-4"),
    
    dbc.Row([
        # Sliders
        dbc.Col([
            html.H5("⚙️ Parameters", className="fw-bold mb-3"),
            dbc.Row(slider_components, className="g-2"),
            html.Hr(),
            html.Div(id='cache-info', className="p-3 bg-info text-white rounded small"),
            html.Hr(),
            html.Button("🗑️ Clear Cache", id='btn-clear-cache', className="btn btn-danger w-100 mt-2"),
            html.Div(id='clear-cache-msg', className="mt-2 small text-center")
        ], width=3, className="bg-light p-3 rounded"),
        
        # Plot
        dbc.Col([
            dcc.Graph(id='3d-plot', style={'height': '800px'})
        ], width=9)
    ], className="g-3"),
    
    # Angles
    dbc.Row([
        dbc.Col(html.Div(id='angle-psi', className="p-3 bg-light rounded text-center"), width=4),
        dbc.Col(html.Div(id='angle-theta', className="p-3 bg-light rounded text-center"), width=4),
        dbc.Col(html.Div(id='angle-phi', className="p-3 bg-light rounded text-center"), width=4),
    ], className="mt-4")
], fluid=True, className="p-4")

# ============================================================================
# CALLBACKS
# ============================================================================

@callback(
    Output('3d-plot', 'figure'),
    Output('angle-psi', 'children'),
    Output('angle-theta', 'children'),
    Output('angle-phi', 'children'),
    Output('cache-info', 'children'),
    [Input(f'slider-{param}', 'value') for param in SLIDER_RANGES.keys()],
    prevent_initial_call=False
)
def update_animation(*slider_values):
    """
    MAIN CALLBACK:
    1. Check if computation exists in file cache
    2. If YES: Load from file (instant)
    3. If NO: Compute and save to file
    4. Render animation
    """
    import time
    
    # Convert slider values to parameters
    params = {}
    for i, param_name in enumerate(SLIDER_RANGES.keys()):
        value = slider_values[i]
        config = SLIDER_RANGES[param_name]
        
        if config.get('convert_to_radians', False):
            params[param_name] = value * np.pi / 180.0
        else:
            params[param_name] = value
    
    param_hash = get_param_hash(slider_values)
    
    # CHECK CACHE
    if is_cached(param_hash):
        print(f"📂 Loading from cache: {param_hash}")
        start_time = time.time()
        frames_data = load_cached_frames(param_hash)
        load_time = time.time() - start_time
        cache_status = "✅ LOADED FROM CACHE"
        computation_time = f"{load_time:.3f}s"
    else:
        print(f"🔄 Computing new: {param_hash}")
        start_time = time.time()
        frames_data = compute_frames(params, num_frames=150)
        compute_time = time.time() - start_time
        
        # SAVE TO FILE
        save_cached_frames(param_hash, frames_data, slider_values)
        cache_status = "💾 COMPUTED & SAVED"
        computation_time = f"{compute_time:.2f}s"
    
    f = params['f']
    axis_scale = 2 * Rmax
    num_frames = len(frames_data)
    time_values = np.linspace(0, 1/f, num_frames)
    
    # Extract frames
    frames = []
    angles_list = []
    for idx in range(num_frames):
        frame = frames_data[idx]
        frames.append((frame['pts1'], frame['pts2']))
        angles_list.append(frame['angles'])
    
    # Create figure with animation frames
    fig = go.Figure()
    
    # Add all frames
    for frame_idx, (pts1, pts2) in enumerate(frames):
        for i in range(0, len(pts1), 2):
            fig.add_trace(go.Scatter3d(
                x=[pts1[i, 0], pts2[i, 0]],
                y=[pts1[i, 1], pts2[i, 1]],
                z=[pts1[i, 2], pts2[i, 2]],
                mode='lines',
                line=dict(color='red', width=3),
                hoverinfo='skip',
                showlegend=(frame_idx == 0),
                visible=(frame_idx == 0),
                name='Structure'
            ))
    
    # Add axes
    for x, y, z, color, name in [
        ([-axis_scale, axis_scale], [0, 0], [0, 0], 'red', 'X'),
        ([0, 0], [-axis_scale, axis_scale], [0, 0], 'green', 'Y'),
        ([0, 0], [0, 0], [-axis_scale, axis_scale], 'blue', 'Z')
    ]:
        fig.add_trace(go.Scatter3d(
            x=x, y=y, z=z,
            mode='lines',
            line=dict(color=color, width=3),
            name=f'{name}-axis',
            hoverinfo='skip',
            visible=True
        ))
    
    # Create animation frames
    animation_frames = []
    for frame_idx in range(num_frames):
        frame_data = []
        
        for i in range(0, len(frames[frame_idx][0]), 2):
            pts1, pts2 = frames[frame_idx]
            frame_data.append(go.Scatter3d(
                x=[pts1[i, 0], pts2[i, 0]],
                y=[pts1[i, 1], pts2[i, 1]],
                z=[pts1[i, 2], pts2[i, 2]],
                mode='lines',
                line=dict(color='red', width=3),
                hoverinfo='skip',
                visible=True,
                name='Structure'
            ))
        
        for x, y, z, color, name in [
            ([-axis_scale, axis_scale], [0, 0], [0, 0], 'red', 'X'),
            ([0, 0], [-axis_scale, axis_scale], [0, 0], 'green', 'Y'),
            ([0, 0], [0, 0], [-axis_scale, axis_scale], 'blue', 'Z')
        ]:
            frame_data.append(go.Scatter3d(
                x=x, y=y, z=z,
                mode='lines',
                line=dict(color=color, width=3),
                name=f'{name}-axis',
                hoverinfo='skip',
                visible=True
            ))
        
        t_val = time_values[frame_idx]
        animation_frames.append(go.Frame(data=frame_data, name=f't={t_val:.3f}'))
    
    fig.frames = animation_frames
    
    # Update layout
    fig.update_layout(
        title=f'Animation: t=0 to 1/f={1/f:.3f}s | {num_frames} Frames',
        updatemenus=[{
            'type': 'buttons',
            'showactive': False,
            'buttons': [
                {'label': '▶ Play', 'method': 'animate', 'args': [None, {'frame': {'duration': 50, 'redraw': True}, 'fromcurrent': True, 'transition': {'duration': 0}}]},
                {'label': '⏸ Pause', 'method': 'animate', 'args': [[None], {'frame': {'duration': 0, 'redraw': False}, 'mode': 'immediate', 'transition': {'duration': 0}}]}
            ]
        }],
        sliders=[{
            'active': 0,
            'steps': [{'args': [[f.name], {'frame': {'duration': 0, 'redraw': True}, 'mode': 'immediate', 'transition': {'duration': 0}}], 'label': f't={time_values[i]:.3f}', 'method': 'animate'} 
                     for i, f in enumerate(animation_frames)]
        }],
        scene=dict(
            xaxis=dict(range=[-axis_scale, axis_scale]),
            yaxis=dict(range=[-axis_scale, axis_scale]),
            zaxis=dict(range=[-axis_scale, axis_scale]),
            aspectmode='cube',
            camera=dict(eye=dict(x=1.5, y=1.5, z=1.5))
        ),
        hovermode='closest',
        margin=dict(l=0, r=0, t=80, b=0),
        height=800
    )
    
    # Get angles
    angles_start = angles_list[0]
    angles_mid = angles_list[len(angles_list)//2]
    angles_end = angles_list[-1]
    
    psi_display = html.Div([
        html.H6("ψ (Roll)"),
        html.P(f"{np.degrees(angles_start[0]):.1f}° → {np.degrees(angles_end[0]):.1f}°", 
               className="fs-6 fw-bold text-danger"),
        html.P(f"Mid: {np.degrees(angles_mid[0]):.1f}°", className="small text-muted")
    ])
    
    theta_display = html.Div([
        html.H6("θ (Pitch)"),
        html.P(f"{np.degrees(angles_start[1]):.1f}° → {np.degrees(angles_end[1]):.1f}°", 
               className="fs-6 fw-bold text-success"),
        html.P(f"Mid: {np.degrees(angles_mid[1]):.1f}°", className="small text-muted")
    ])
    
    phi_display = html.Div([
        html.H6("φ (Yaw)"),
        html.P(f"{np.degrees(angles_start[2]):.1f}° → {np.degrees(angles_end[2]):.1f}°", 
               className="fs-6 fw-bold text-primary"),
        html.P(f"Mid: {np.degrees(angles_mid[2]):.1f}°", className="small text-muted")
    ])
    
    # Cache info
    stats = get_cache_stats()
    cache_info = html.Div([
        html.P(cache_status, className="mb-1 fw-bold"),
        html.P(f"Time: {computation_time}", className="mb-1"),
        html.P(f"Frames: {num_frames}", className="mb-1"),
        html.Hr(className="my-2"),
        html.P(f"📁 Cached Sets: {stats['num_cached']}", className="mb-1"),
        html.P(f"💾 Cache Size: {stats['size_mb']:.1f} MB", className="mb-0")
    ], className="small")
    
    return fig, psi_display, theta_display, phi_display, cache_info

@callback(
    Output('clear-cache-msg', 'children'),
    Input('btn-clear-cache', 'n_clicks'),
    prevent_initial_call=True
)
def clear_cache(n_clicks):
    """Clear all cached files"""
    try:
        for file in CACHE_DIR.glob('frames_*.pkl'):
            file.unlink()
        for file in CACHE_DIR.glob('meta_*.json'):
            file.unlink()
        
        msg = html.P("✅ Cache cleared!", className="text-success fw-bold")
    except Exception as e:
        msg = html.P(f"❌ Error: {e}", className="text-danger fw-bold")
    
    return msg

if __name__ == '__main__':
    print(f"""
    ╔════════════════════════════════════════════════════════════╗
    ║    File-Cached 3D Parametric Animation                     ║
    ║    Persistent disk caching for parameter sets              ║
    ╚════════════════════════════════════════════════════════════╝
    
    HOW IT WORKS:
    1️⃣  Check if parameter set cached on disk
    2️⃣  If YES: Load from file (<10ms)
    3️⃣  If NO: Compute frames (~0.3s) → Save to file
    4️⃣  Render animation instantly
    
    CACHE LOCATION: ./animation_cache/
    
    Files stored:
    • frames_HASH.pkl  (numpy arrays)
    • meta_HASH.json   (metadata)
    
    BENEFITS:
    ✅ Persistent across sessions
    ✅ No redundant computation
    ✅ Fast lookup
    ✅ Easy to analyze/compare
    
    Starting... Open http://127.0.0.1:8050/
    """)
    app.run(debug=False)



