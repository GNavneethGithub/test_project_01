import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d.art3d import Line3DCollection
from matplotlib.animation import FuncAnimation

# =====================
# Utility: build a params dict for a wing
# =====================
def build_params(
    f=1.0,
    psiM=60 * np.pi / 180.0,
    psiC=1.09,
    Dopsi=-90 * np.pi / 180.0,
    psi0=90 * np.pi / 180.0,
    thetaM=0.0,
    Dotheta=0.0,
    thetaN=1.0,
    theta0=0.0,
    phiM=90 * np.pi / 180.0,
    phi0=0.0,
    phiK=0.14,
    Dophi=0.0
):
    """
    Pack parameters into a dict and compute a few derived constants for speed.
    """
    params = dict(
        f=float(f),
        psiM=float(psiM),
        psiC=float(psiC),
        Dopsi=float(Dopsi),
        psi0=float(psi0),
        thetaM=float(thetaM),
        Dotheta=float(Dotheta),
        thetaN=float(thetaN),
        theta0=float(theta0),
        phiM=float(phiM),
        phi0=float(phi0),
        phiK=float(phiK),
        Dophi=float(Dophi),
    )

    # derived constants
    params['TWO_PI_F'] = 2.0 * np.pi * params['f']
    # safe inverses (assume psiC and phiK are valid; if phiK==0 arcsin(0)=0 -> division by zero avoided)
    params['INV_TANH_PSI_C'] = 1.0 / np.tanh(params['psiC']) if params['psiC'] != 0 else 1.0
    # handle phiK near zero: if phiK==0 set inv asin to 1 to avoid div by zero (phi term becomes phi0)
    if abs(params['phiK']) < 1e-12:
        params['INV_ASIN_PHI_K'] = 1.0
    else:
        params['INV_ASIN_PHI_K'] = 1.0 / np.arcsin(params['phiK'])

    return params


# =====================
# Angle generator using a specific params dict
# =====================
def xyz_with_params(t, params):
    """
    Compute Euler angles (psi, theta, phi) using the per-wing params dict.
    """
    phase = params['TWO_PI_F'] * t

    psi = params['psi0'] + params['psiM'] * params['INV_TANH_PSI_C'] * np.tanh(
        params['psiC'] * np.sin(phase + params['Dopsi'])
    )

    theta = params['theta0'] + params['thetaM'] * np.cos(params['Dotheta'] + phase * params['thetaN'])

    # avoid domain errors in arcsin by clipping inside [-1,1]
    sin_arg = np.clip(params['phiK'] * np.sin(phase + params['Dophi']), -1.0, 1.0)
    phi = params['phi0'] + params['phiM'] * np.arcsin(sin_arg) * params['INV_ASIN_PHI_K']

    return np.array([psi, theta, phi])


# =====================
# Rotation matrix tBW (ZYX Euler to rotation matrix)
# =====================
def tBW(angles):
    xa, ya, za = angles
    cxa = np.cos(xa); sxa = np.sin(xa)
    cya = np.cos(ya); sya = np.sin(ya)
    cza = np.cos(za); sza = np.sin(za)
    return np.array([
        [cya * cza,         cza * sxa * sya - cxa * sza,   cxa * cza * sya + sxa * sza],
        [cya * sza,         cxa * cza + sxa * sya * sza,   -(cza * sxa) + cxa * sya * sza],
        [-sya,              cya * sxa,                     cxa * cya]
    ])


# =====================
# Wing geometry builder (returns world points and a Line3DCollection)
# =====================
def make_wing(params, side='right', k=100, cmax=1.0, Rmax=1.5, color='red', alpha=0.9):
    """
    Build world-frame points and Line3DCollection for a wing.

    - params: dict from build_params
    - side: 'right' or 'left' (mirrors x coordinates)
    - returns: (collection, leading_world_pts, trailing_world_pts)
    """
    # radial coordinate from root to tip (use positive r and mirror for left)
    r = np.linspace(0, Rmax, k)

    # profile function
    def cr(rvals):
        return cmax * np.sqrt(1.0 - (1.0 - rvals**2 / Rmax**2)**2)

    y_profile = -cr(r)
    z_profile = np.zeros_like(r)

    if side == 'right':
        x = r
    elif side == 'left':
        x = -r
    else:
        raise ValueError("side must be 'right' or 'left'")

    leading = np.column_stack([x, np.zeros_like(r), z_profile])   # (k,3)
    trailing = np.column_stack([x, y_profile, z_profile])         # (k,3)

    segments0 = np.stack([leading, trailing], axis=1)  # (k,2,3)
    collection = Line3DCollection(segments0, linewidths=1.5, alpha=alpha)
    collection.set_color(color)

    return {
        'collection': collection,
        'leading': leading,
        'trailing': trailing,
        'params': params
    }


# =====================
# Example parameter sets for left and right (customize as needed)
# =====================

# Right wing parameters (example)
right_params = build_params(
    f=1.0,
    
    psiM=60 * np.pi / 180.0,
    psiC=1.09,
    Dopsi=-90 * np.pi / 180.0,
    psi0=90 * np.pi / 180.0,
    
    thetaM=0.0,
    Dotheta=0.0,
    thetaN=1.0,
    theta0=0.0,
    
    phiM=30 * np.pi / 180.0,
    phi0=45 * np.pi / 180.0,
    phiK=0.14,
    Dophi=0.0
)

# Left wing parameters (different example values; feel free to change)
left_params = build_params(
    f=1.0,                       # could be different frequency
    
    psiM=60 * np.pi / 180.0,     # different amplitude
    psiC=0.9,
    Dopsi=-90 * np.pi / 180.0,
    psi0=90 * np.pi / 180.0,
    
    thetaM=0.0,
    Dotheta=0.0,
    thetaN=1.0,
    theta0=0.0,
    
    phiM=30 * np.pi / 180.0,
    phi0=-45 * np.pi / 180.0,
    phiK=0.12,
    Dophi= -180* np.pi / 180.0
)

# =====================
# Build wings
# =====================

# geometry / display parameters
k = 120
cmax = 1.0
Rmax = 1.5

right_wing = make_wing(right_params, side='right', k=k, cmax=cmax, Rmax=Rmax, color='red', alpha=0.9)
left_wing  = make_wing(left_params,  side='left',  k=k, cmax=cmax, Rmax=Rmax, color='blue', alpha=0.9)

# =====================
# Figure setup
# =====================

fig = plt.figure(figsize=(10, 10))
ax = fig.add_subplot(111, projection='3d')

axis_scale = 2 * Rmax
ax.set_xlim([-axis_scale, axis_scale])
ax.set_ylim([-axis_scale, axis_scale])
ax.set_zlim([-axis_scale, axis_scale])
ax.set_xlabel('X'); ax.set_ylabel('Y'); ax.set_zlabel('Z')

# Remove box and grid (do this once)
ax.xaxis.pane.fill = False
ax.yaxis.pane.fill = False
ax.zaxis.pane.fill = False
ax.grid(False)

# Static axis lines (created once)
x_axis_line, = ax.plot([-axis_scale, axis_scale], [0, 0], [0, 0], 'k-', linewidth=2)
y_axis_line, = ax.plot([0, 0], [-axis_scale, axis_scale], [0, 0], 'k-', linewidth=2)
z_axis_line, = ax.plot([0, 0], [0, 0], [-axis_scale, axis_scale], 'k-', linewidth=2)

# Bold labels at axis tips
txtX = ax.text(axis_scale, 0, 0, r'$\mathbf{X}$', fontsize=16, zorder=10)
txtY = ax.text(0, axis_scale, 0, r'$\mathbf{Y}$', fontsize=16, zorder=10)
txtZ = ax.text(0, 0, axis_scale, r'$\mathbf{Z}$', fontsize=16, zorder=10)

# add both collections to axes
ax.add_collection3d(right_wing['collection'])
ax.add_collection3d(left_wing['collection'])

# =====================
# Animation setup
# =====================

# number of frames and time-step selection: we choose a dt tuned to each wing's f
# To keep frames consistent across wings, animation time uses a single dt based on a reference f (choose max f)
f_ref = max(right_params['f'], left_params['f'])
num_frames = 120
dt = 1.0 / ((num_frames - 1) * f_ref)

# =====================
# Animation function
# =====================
def animate(frame):
    t_global = frame * dt

    # Right wing: compute angles using its params; if it has a different f, convert global time to its phase.
    # We can either use the same absolute time t_global for both (typical), or rescale per-wing if you want
    # independent phase control. Here we use the same real time t_global for both wings.
    angles_R = xyz_with_params(t_global, right_wing['params'])
    angles_L = xyz_with_params(t_global, left_wing['params'])

    Rmat_R = tBW(angles_R)
    Rmat_L = tBW(angles_L)

    # Rotate world-frame points
    ptsR_lead = (Rmat_R @ right_wing['leading'].T).T
    ptsR_trail = (Rmat_R @ right_wing['trailing'].T).T
    ptsL_lead = (Rmat_L @ left_wing['leading'].T).T
    ptsL_trail = (Rmat_L @ left_wing['trailing'].T).T

    # Update segments in each collection
    right_segments = np.stack([ptsR_lead, ptsR_trail], axis=1)
    left_segments  = np.stack([ptsL_lead,  ptsL_trail],  axis=1)

    right_wing['collection'].set_segments(right_segments)
    left_wing['collection'].set_segments(left_segments)

    # Ensure axis labels stay on top visually
    txtX.set_zorder(10); txtY.set_zorder(10); txtZ.set_zorder(10)

    # Update title with both wings' primary angles (showing right and left psi for clarity)
    title = (
        f't = {t_global:.3f} (frame {frame}/{num_frames - 1})\n'
        f'Right ψ={np.degrees(angles_R[0]):.1f}° Left ψ={np.degrees(angles_L[0]):.1f}°'
    )
    ax.set_title(title, fontsize=10)

    # return artists that changed
    return right_wing['collection'], left_wing['collection'], ax.title

# =====================
# Run animation
# =====================
anim = FuncAnimation(fig, animate, frames=num_frames, interval=50, blit=False, repeat=True)

plt.tight_layout()
plt.show()
