"""
single_wing_animation.py

Animate one wing only (modular; later you can create more Wing instances and overlay).
"""

import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d.art3d import Line3DCollection
from matplotlib.animation import FuncAnimation

# -------------------------
# Motion parameter builder
# -------------------------
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
    phiM=30 * np.pi / 180.0,
    phi0=45 * np.pi / 180.0,
    phiK=0.14,
    Dophi=0.0
):
    """Return params dict with a few precomputed constants."""
    params = dict(
        f=float(f), psiM=float(psiM), psiC=float(psiC),
        Dopsi=float(Dopsi), psi0=float(psi0),
        thetaM=float(thetaM), Dotheta=float(Dotheta),
        thetaN=float(thetaN), theta0=float(theta0),
        phiM=float(phiM), phi0=float(phi0), phiK=float(phiK),
        Dophi=float(Dophi)
    )
    params['TWO_PI_F'] = 2.0 * np.pi * params['f']
    params['INV_TANH_PSI_C'] = 1.0 / np.tanh(params['psiC']) if params['psiC'] != 0 else 1.0
    params['INV_ASIN_PHI_K'] = 1.0 if abs(params['phiK']) < 1e-12 else 1.0 / np.arcsin(params['phiK'])
    return params

def xyz_with_params(t, params):
    """Compute Euler angles (psi, theta, phi) from params at time t."""
    phase = params['TWO_PI_F'] * t
    psi = params['psi0'] + params['psiM'] * params['INV_TANH_PSI_C'] * np.tanh(
        params['psiC'] * np.sin(phase + params['Dopsi']))
    theta = params['theta0'] + params['thetaM'] * np.cos(params['Dotheta'] + phase * params['thetaN'])
    sin_arg = np.clip(params['phiK'] * np.sin(phase + params['Dophi']), -1.0, 1.0)
    phi = params['phi0'] + params['phiM'] * np.arcsin(sin_arg) * params['INV_ASIN_PHI_K']
    return np.array([psi, theta, phi])

def tBW(angles):
    """Rotation matrix (ZYX Euler) from angles = [psi, theta, phi]."""
    xa, ya, za = angles
    cxa = np.cos(xa); sxa = np.sin(xa)
    cya = np.cos(ya); sya = np.sin(ya)
    cza = np.cos(za); sza = np.sin(za)
    return np.array([
        [cya * cza,         cza * sxa * sya - cxa * sza,   cxa * cza * sya + sxa * sza],
        [cya * sza,         cxa * cza + sxa * sya * sza,   -(cza * sxa) + cxa * sya * sza],
        [-sya,              cya * sxa,                     cxa * cya]
    ])

# -------------------------
# Single Wing (world geometry + collection + metadata)
# -------------------------
class Wing:
    def __init__(self, params, side='right', k=120, cmax=1.0, Rmax=1.5,
                 color='red', transparency=0.0, visible=True):
        """
        Single wing.
        transparency: 0.0 => opaque ; 1.0 => fully transparent
        side: 'right' or 'left' (mirrors x)
        """
        self.params = params
        self.k = int(k)
        self.cmax = float(cmax)
        self.Rmax = float(Rmax)
        self.side = side
        self.color = color
        self.transparency = float(min(max(transparency, 0.0), 1.0))
        self.visible = bool(visible)

        # build profile in world frame (leading & trailing)
        r = np.linspace(0.0, self.Rmax, self.k)
        y_profile = -self.cmax * np.sqrt(1.0 - (1.0 - r**2 / self.Rmax**2)**2)
        z_profile = np.zeros_like(r)
        x = r if side == 'right' else -r

        self.leading = np.column_stack([x, np.zeros_like(r), z_profile])   # (k,3)
        self.trailing = np.column_stack([x, y_profile, z_profile])        # (k,3)

        segments0 = np.stack([self.leading, self.trailing], axis=1)        # (k,2,3)
        self.collection = Line3DCollection(segments0, linewidths=1.5)
        self.collection.set_color(self.color)
        self.collection.set_alpha(1.0 - self.transparency)
        self.collection.set_visible(self.visible)

        # leading-edge scatter (will be created in Animator.init_plot)
        self.leading_scatter = None

    def set_transparency(self, transparency):
        self.transparency = float(min(max(transparency, 0.0), 1.0))
        self.collection.set_alpha(1.0 - self.transparency)

    def set_color(self, color):
        self.color = color
        self.collection.set_color(color)

    def set_visible(self, visible):
        self.visible = bool(visible)
        self.collection.set_visible(self.visible)
        if self.leading_scatter is not None:
            self.leading_scatter.set_visible(self.visible)

    def rotated_segments_and_lead(self, Rmat):
        """Return rotated segments (k,2,3) and rotated leading points (k,3)."""
        pts_lead = (Rmat @ self.leading.T).T
        pts_trail = (Rmat @ self.trailing.T).T
        segments = np.stack([pts_lead, pts_trail], axis=1)
        return segments, pts_lead

# -------------------------
# Simple 3D grid (XY, XZ, YZ planes)
# -------------------------
def make_3d_grid(axis_scale, n_lines=11):
    grid_lines = []
    xs = np.linspace(-axis_scale, axis_scale, n_lines)
    ys = np.linspace(-axis_scale, axis_scale, n_lines)
    zs = np.linspace(-axis_scale, axis_scale, n_lines)

    # XY plane at z=0
    for x in xs:
        grid_lines.append([(x, -axis_scale, 0.0), (x, axis_scale, 0.0)])
    for y in ys:
        grid_lines.append([(-axis_scale, y, 0.0), (axis_scale, y, 0.0)])

    # XZ plane at y=0
    for x in xs:
        grid_lines.append([(x, 0.0, -axis_scale), (x, 0.0, axis_scale)])
    for z in zs:
        grid_lines.append([(-axis_scale, 0.0, z), (axis_scale, 0.0, z)])

    # YZ plane at x=0
    for y in ys:
        grid_lines.append([(0.0, y, -axis_scale), (0.0, y, axis_scale)])
    for z in zs:
        grid_lines.append([(0.0, -axis_scale, z), (0.0, axis_scale, z)])

    return Line3DCollection(grid_lines, linewidths=0.6, linestyles='--', alpha=0.35)

# -------------------------
# Animator for single wing
# -------------------------
class SingleWingAnimator:
    def __init__(self, wing: Wing, ax, num_frames=240, hide_non_animated=False,
                 axis_scale=None, n_grid_lines=11,
                 leading_dot_color='blue', leading_dot_size=10):
        """
        wing: Wing instance (single)
        ax: 3D axes
        """
        if not hasattr(wing, 'collection'):
            raise TypeError("wing must be a Wing instance")
        self.wing = wing
        self.ax = ax
        self.num_frames = int(num_frames)
        self.hide_non_animated = bool(hide_non_animated)  # for single-wing this is unused but kept for API parity
        self.leading_dot_color = leading_dot_color
        self.leading_dot_size = leading_dot_size

        freqs = [wing.params['f']]
        f_ref = max(freqs) if freqs else 1.0
        self.dt = 1.0 / ((self.num_frames - 1) * f_ref) if self.num_frames > 1 else 0.01

        self.axis_scale = axis_scale or 2.0 * wing.Rmax
        self.n_grid_lines = int(n_grid_lines)

        # grid and collections set in init_plot
        self.grid_collection = None

    def init_plot(self):
        # draw grid
        self.grid_collection = make_3d_grid(self.axis_scale, n_lines=self.n_grid_lines)
        self.grid_collection.set_color((0.4, 0.4, 0.4))
        self.ax.add_collection3d(self.grid_collection)

        # add wing line collection
        self.ax.add_collection3d(self.wing.collection)

        # add leading-edge scatter (initial world leading points)
        lead = self.wing.leading
        sc = self.ax.scatter(lead[:,0], lead[:,1], lead[:,2],
                             s=self.leading_dot_size, c=self.leading_dot_color, depthshade=True)
        sc.set_visible(self.wing.visible)
        self.wing.leading_scatter = sc

    def animate(self, frame):
        t = frame * self.dt
        angles = xyz_with_params(t, self.wing.params)
        R = tBW(angles)

        segments, pts_lead = self.wing.rotated_segments_and_lead(R)
        self.wing.collection.set_segments(segments)

        # update leading scatter (3D scatter uses _offsets3d)
        if self.wing.leading_scatter is not None:
            xs, ys, zs = pts_lead[:,0], pts_lead[:,1], pts_lead[:,2]
            self.wing.leading_scatter._offsets3d = (xs, ys, zs)
            self.wing.leading_scatter.set_visible(self.wing.collection.get_visible())

        # update title with angles (degrees)
        title = f't={t:.3f}  ψ={np.degrees(angles[0]):.1f}°  θ={np.degrees(angles[1]):.1f}°  φ={np.degrees(angles[2]):.1f}°'
        self.ax.set_title(title, fontsize=10)

        # return visible artists so blitting (if enabled) knows what to redraw
        artists = []
        if self.wing.collection.get_visible():
            artists.append(self.wing.collection)
        if self.wing.leading_scatter is not None and self.wing.leading_scatter.get_visible():
            artists.append(self.wing.leading_scatter)
        artists += [self.grid_collection, self.ax.title]
        return artists

# -------------------------
# Example: configure single wing and run animation
# -------------------------
if __name__ == '__main__':
    # full motion params (you can change these)
    params = build_params(
        f=1.0,

        psiM=60 * np.pi / 180.0,
        psiC=1.09,
        Dopsi=-90 * np.pi / 180.0,
        psi0=90 * np.pi / 180.0,

        thetaM=0.0,
        thetaN=1.0,
        Dotheta=0.0,        
        theta0=0.0,

        phiM=30 * np.pi / 180.0,
        phiK=0.14,
        
        # Dophi=0 * np.pi / 180.0, # 1st quadrant motion for first wing
        # phi0=45 * np.pi / 180.0  # 1st quadrant motion for first wing
        
        # Dophi=-90 * np.pi / 180.0, # 2nd quadrant motion for second wing
        # phi0=135 * np.pi / 180.0  # 2nd quadrant motion for second wing
        
        # Dophi=0 * np.pi / 180.0,   # 3rd quadrant motion for thrid wing
        # phi0= -135 * np.pi / 180.0 # 3rd quadrant motion for thrid wing
        
        # Dophi=0 * np.pi / 180.0, # 4th quadrant motion for fourth wing
        # phi0=-45 * np.pi / 180.0 # 4th quadrant motion for fourth wing

    )

    # create single wing (right side)
    wing = Wing(params, side='right', k=120, cmax=1.0, Rmax=1.5,
                color='teal', transparency=0.1, visible=True)

    # plot setup
    fig = plt.figure(figsize=(9,9))
    ax = fig.add_subplot(111, projection='3d')
    axis_scale = 2 * wing.Rmax
    ax.set_xlim([-axis_scale, axis_scale]); ax.set_ylim([-axis_scale, axis_scale]); ax.set_zlim([-axis_scale, axis_scale])
    ax.set_xlabel('X'); ax.set_ylabel('Y'); ax.set_zlabel('Z')
    ax.xaxis.pane.fill = True; ax.yaxis.pane.fill = True; ax.zaxis.pane.fill = True
    ax.xaxis.pane.set_edgecolor('lightgray'); ax.yaxis.pane.set_edgecolor('lightgray'); ax.zaxis.pane.set_edgecolor('lightgray')

    # axis lines and labels
    ax.plot([-axis_scale, axis_scale], [0,0], [0,0], 'k-', linewidth=2)
    ax.plot([0,0], [-axis_scale, axis_scale], [0,0], 'k-', linewidth=2)
    ax.plot([0,0], [0,0], [-axis_scale, axis_scale], 'k-', linewidth=2)
    ax.text(axis_scale, 0, 0, r'$\mathbf{X}$', fontsize=14, zorder=10)
    ax.text(0, axis_scale, 0, r'$\mathbf{Y}$', fontsize=14, zorder=10)
    ax.text(0, 0, axis_scale, r'$\mathbf{Z}$', fontsize=14, zorder=10)

    # animator
    animator = SingleWingAnimator(wing, ax, num_frames=240, leading_dot_color='blue', leading_dot_size=8)
    animator.init_plot()

    anim = FuncAnimation(fig, animator.animate, frames=240, interval=40, blit=False, repeat=True)
    plt.tight_layout()
    plt.show()

    # Optional: to save (uncomment and install ffmpeg)
    # anim.save('single_wing.mp4', writer='ffmpeg', fps=25, dpi=150)
