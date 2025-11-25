"""
multi_wing_individual_params.py

Each wing has its own full motion parameter set and display settings.
"""

import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d.art3d import Line3DCollection
from matplotlib.animation import FuncAnimation

# -------------------------
# Motion builder (same signature for each wing)
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
    phase = params['TWO_PI_F'] * t
    psi = params['psi0'] + params['psiM'] * params['INV_TANH_PSI_C'] * np.tanh(params['psiC'] * np.sin(phase + params['Dopsi']))
    theta = params['theta0'] + params['thetaM'] * np.cos(params['Dotheta'] + phase * params['thetaN'])
    sin_arg = np.clip(params['phiK'] * np.sin(phase + params['Dophi']), -1.0, 1.0)
    phi = params['phi0'] + params['phiM'] * np.arcsin(sin_arg) * params['INV_ASIN_PHI_K']
    return np.array([psi, theta, phi])

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

# -------------------------
# Wing class (geometry + collection + leading points)
# -------------------------
class Wing:
    def __init__(self, motion_params, side='right', k=120, cmax=1.0, Rmax=1.5,
                 color='red', transparency=0.0, visible=True, leading_dot_color='blue', leading_dot_size=6):
        """
        motion_params: dict of motion parameters (f, psiM, ..., Dophi) — will be passed to build_params if raw input provided
        transparency: 0.0 => opaque, 1.0 => fully transparent (alpha = 1 - transparency)
        """
        # accept either a dict of raw keyword args or an already-built params dict
        if not isinstance(motion_params, dict):
            raise TypeError("motion_params must be a dict")
        # if TWO_PI_F not present assume caller passed raw kwargs and build a params dict
        if 'TWO_PI_F' not in motion_params:
            # treat motion_params as raw kwargs -> pass through build_params (note: missing keys defaulted)
            self.params = build_params(**motion_params)
        else:
            # already built params dict
            self.params = motion_params

        self.k = int(k)
        self.cmax = float(cmax)
        self.Rmax = float(Rmax)
        self.side = side
        self.color = color
        self.transparency = float(min(max(transparency, 0.0), 1.0))
        self.visible = bool(visible)
        self.leading_dot_color = leading_dot_color
        self.leading_dot_size = leading_dot_size

        # geometry (world frame)
        r = np.linspace(0.0, self.Rmax, self.k)
        y_profile = -self.cmax * np.sqrt(1.0 - (1.0 - r**2 / self.Rmax**2)**2)
        z_profile = np.zeros_like(r)
        x = r if side == 'right' else -r

        self.leading = np.column_stack([x, np.zeros_like(r), z_profile])
        self.trailing = np.column_stack([x, y_profile, z_profile])
        segments0 = np.stack([self.leading, self.trailing], axis=1)

        self.collection = Line3DCollection(segments0, linewidths=1.5)
        self.collection.set_color(self.color)
        self.collection.set_alpha(1.0 - self.transparency)
        self.collection.set_visible(self.visible)

        # leading scatter placeholder (created by manager)
        self.leading_scatter = None

    # convenience
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
        pts_lead = (Rmat @ self.leading.T).T
        pts_trail = (Rmat @ self.trailing.T).T
        return np.stack([pts_lead, pts_trail], axis=1), pts_lead

# -------------------------
# 3D grid helper
# -------------------------
def make_3d_grid(axis_scale, n_lines=11):
    grid_lines = []
    xs = np.linspace(-axis_scale, axis_scale, n_lines)
    ys = np.linspace(-axis_scale, axis_scale, n_lines)
    zs = np.linspace(-axis_scale, axis_scale, n_lines)

    # XY plane (z=0)
    for x in xs:
        grid_lines.append([(x, -axis_scale, 0.0), (x, axis_scale, 0.0)])
    for y in ys:
        grid_lines.append([(-axis_scale, y, 0.0), (axis_scale, y, 0.0)])

    # XZ plane (y=0)
    for x in xs:
        grid_lines.append([(x, 0.0, -axis_scale), (x, 0.0, axis_scale)])
    for z in zs:
        grid_lines.append([(-axis_scale, 0.0, z), (axis_scale, 0.0, z)])

    # YZ plane (x=0)
    for y in ys:
        grid_lines.append([(0.0, y, -axis_scale), (0.0, y, axis_scale)])
    for z in zs:
        grid_lines.append([(0.0, -axis_scale, z), (0.0, axis_scale, z)])

    return Line3DCollection(grid_lines, linewidths=0.6, linestyles='--', alpha=0.35)

# -------------------------
# Overlay manager supports independent motion params per wing
# -------------------------

class OverlayAnimator:
    def __init__(
        self,
        wings,
        ax,
        animated_wing_index=None,
        num_frames=240,
        hide_non_animated=False,
        leading_dot_color=None,
        leading_dot_size=None,
        global_axis_scale=None,
        n_grid_lines=11
    ):
        """
        OverlayAnimator manages multiple Wing objects.

        Parameters:
          - wings: list of Wing instances (each must have .params, .collection, .leading)
          - ax: matplotlib 3D axes
          - animated_wing_index: int or None (None => animate all)
          - num_frames: total frames
          - hide_non_animated: if True and animating a single wing, hide others
          - leading_dot_color: global color for leading-edge dots (overridden by per-wing leading_dot_color if present)
          - leading_dot_size: global size for leading-edge dots (overridden by per-wing leading_dot_size if present)
          - global_axis_scale: optional axis scale (otherwise computed from wings)
          - n_grid_lines: number of grid lines per plane
        """
        # basic checks
        if not all(hasattr(w, 'params') for w in wings):
            raise TypeError("wings must be Wing instances with .params")

        self.wings = wings
        self.ax = ax
        self.animated_wing_index = animated_wing_index
        self.num_frames = int(num_frames)
        self.hide_non_animated = bool(hide_non_animated)

        # leading-dot defaults (allow either calling keyword)
        # prefer explicit leading_dot_color/size, otherwise look for attributes on animator
        self.leading_dot_default_color = leading_dot_color if leading_dot_color is not None else 'blue'
        self.leading_dot_default_size = leading_dot_size if leading_dot_size is not None else 6

        # grid / axis config
        self.axis_scale = global_axis_scale
        self.n_grid_lines = int(n_grid_lines)

        # choose reference frequency from all wings (use max f)
        freqs = [w.params.get('f', 1.0) for w in wings] if wings else [1.0]
        self.f_ref = max(freqs) if freqs else 1.0
        self.dt = 1.0 / ((self.num_frames - 1) * self.f_ref) if self.num_frames > 1 else 0.01

        # placeholder for grid collection created in init_plot
        self.grid_collection = None

    def init_plot(self, axis_scale=None, n_grid_lines=None):
        """Create grid, add all wing collections and per-wing leading scatters."""
        axis_scale = axis_scale or self.axis_scale or (2.0 * max(w.Rmax for w in self.wings))
        n_grid_lines = n_grid_lines or self.n_grid_lines

        # draw grid
        self.grid_collection = make_3d_grid(axis_scale, n_lines=n_grid_lines)
        self.grid_collection.set_color((0.45, 0.45, 0.45))
        self.ax.add_collection3d(self.grid_collection)

        # add wing line collections and create leading scatters
        for w in self.wings:
            self.ax.add_collection3d(w.collection)

            # per-wing leading dot style takes precedence if the Wing has attributes,
            # otherwise fall back to animator defaults passed in constructor
            lead_color = getattr(w, 'leading_dot_color', None) or self.leading_dot_default_color
            lead_size = getattr(w, 'leading_dot_size', None) or self.leading_dot_default_size

            lead = w.leading  # world-frame points
            sc = self.ax.scatter(lead[:, 0], lead[:, 1], lead[:, 2],
                                 s=lead_size, c=lead_color, depthshade=True)
            sc.set_visible(w.visible)
            w.leading_scatter = sc

        # apply the initial visibility / transparency rules
        self.apply_visibility_alpha_rules()

    def apply_visibility_alpha_rules(self):
        """Set visibility and alpha (1 - transparency) for all wings according to current state."""
        for idx, w in enumerate(self.wings):
            if self.animated_wing_index is None:
                # animate all -> respect per-wing flags
                w.collection.set_visible(bool(w.visible))
                w.collection.set_alpha(1.0 - w.transparency)
                if w.leading_scatter is not None:
                    w.leading_scatter.set_visible(bool(w.visible))
            else:
                # single-wing animation: animated wing respects its flags, others hidden or shown per hide_non_animated
                if idx == self.animated_wing_index:
                    w.collection.set_visible(bool(w.visible))
                    w.collection.set_alpha(1.0 - w.transparency)
                    if w.leading_scatter is not None:
                        w.leading_scatter.set_visible(bool(w.visible))
                else:
                    if self.hide_non_animated:
                        w.collection.set_visible(False)
                        if w.leading_scatter is not None:
                            w.leading_scatter.set_visible(False)
                    else:
                        w.collection.set_visible(bool(w.visible))
                        w.collection.set_alpha(1.0 - w.transparency)
                        if w.leading_scatter is not None:
                            w.leading_scatter.set_visible(bool(w.visible))

    # runtime controls (unchanged API)
    def set_animated_wing_index(self, index):
        if index is not None and not (0 <= index < len(self.wings)):
            raise IndexError("index out of range")
        self.animated_wing_index = index
        self.apply_visibility_alpha_rules()

    def set_hide_non_animated(self, flag):
        self.hide_non_animated = bool(flag)
        self.apply_visibility_alpha_rules()

    def set_wing_visibility(self, index, visible):
        self.wings[index].set_visible(visible)
        self.apply_visibility_alpha_rules()

    def set_wing_transparency(self, index, transparency):
        self.wings[index].set_transparency(transparency)
        self.apply_visibility_alpha_rules()

    def set_wing_color(self, index, color):
        self.wings[index].set_color(color)

    def animate(self, frame):
        t = frame * self.dt

        if self.animated_wing_index is None:
            indices = range(len(self.wings))
        else:
            indices = [self.animated_wing_index]

        for idx in indices:
            w = self.wings[idx]
            if not w.collection.get_visible():
                if w.leading_scatter is not None:
                    w.leading_scatter.set_visible(False)
                continue
            angles = xyz_with_params(t, w.params)
            R = tBW(angles)
            segments, pts_lead = w.rotated_segments_and_lead(R)
            w.collection.set_segments(segments)
            if w.leading_scatter is not None:
                xs, ys, zs = pts_lead[:, 0], pts_lead[:, 1], pts_lead[:, 2]
                w.leading_scatter._offsets3d = (xs, ys, zs)
                w.leading_scatter.set_visible(True)

        # update title
        if self.animated_wing_index is None:
            title = f't = {t:.3f} (frame {frame}/{self.num_frames - 1}) — animating ALL wings'
        else:
            a = self.wings[self.animated_wing_index]
            ang = xyz_with_params(t, a.params) if a.collection.get_visible() else np.array([np.nan, np.nan, np.nan])
            title = f't = {t:.3f} (frame {frame}/{self.num_frames - 1}) — wing {self.animated_wing_index} ψ={np.degrees(ang[0]):.1f}°'
        self.ax.set_title(title, fontsize=10)

        # return visible artists so animation redraws correctly
        artists = [w.collection for w in self.wings if w.collection.get_visible()]
        artists += [w.leading_scatter for w in self.wings if (w.leading_scatter is not None and w.leading_scatter.get_visible())]
        artists += [self.grid_collection, self.ax.title]
        return artists


# -------------------------
# Example: define each wing with its own full motion params
# -------------------------
if __name__ == '__main__':
    # Example wings_spec: each entry contains 'motion' dict with full parameter list,
    # plus display options color/transparency/visible/side/leading-dot style.

        # f=1.0,
        # psiM=60 * np.pi / 180.0,psiC=1.09,Dopsi=-90 * np.pi / 180.0,psi0=90 * np.pi / 180.0,
        # thetaM=0.0,thetaN=1.0,Dotheta=0.0,theta0=0.0,
        # phiM=30 * np.pi / 180.0,phiK=0.14,Dophi=0 * np.pi / 180.0, phi0=45 * np.pi / 180.0   

    wings_spec = [
        # {
        #     'motion': dict(
        #         f=1.0,
        #         psiM=60 * np.pi / 180.0, psiC=1.09, Dopsi=-90 * np.pi / 180.0, psi0=90 * np.pi / 180.0,
        #         thetaM=0.0, Dotheta=0.0, thetaN=1.0, theta0=0.0,
        #         phiM=30 * np.pi / 180.0, phiK=0.14, Dophi=0.0* np.pi / 180.0, phi0=45 * np.pi / 180.0
        #     ),
        #     'side': 'right', 'color': 'red', 'transparency': 0.8, 'visible': True
        # },


        # Dophi=180 * np.pi / 180.0, # 2nd quadrant motion for fourth wing
        # phi0=-45 * np.pi / 180.0 # 2nd quadrant motion for fourth wing

        {
            'motion': dict(
                f=1.1,
                psiM=60 * np.pi / 180.0, psiC=1.09, Dopsi=-90 * np.pi / 180.0, psi0=90 * np.pi / 180.0,
                thetaM=0.0, Dotheta=0.0, thetaN=1.0, theta0=0.0,
                phiM=30 * np.pi / 180.0, phiK=0.14, Dophi=180 * np.pi / 180.0, phi0=-45 * np.pi / 180.0
            ),
            'side': 'left', 'color': 'magenta', 'transparency': 0.8, 'visible': True
        },


        # Dophi=0 * np.pi / 180.0,   # 3rd quadrant motion for thrid wing
        # phi0= -135 * np.pi / 180.0 # 3rd quadrant motion for thrid wing        
        {
            'motion': dict(
                f=1.1,
                psiM=60 * np.pi / 180.0, psiC=1.09, Dopsi=-90 * np.pi / 180.0, psi0=90 * np.pi / 180.0,
                thetaM=0.0, Dotheta=0.0, thetaN=1.0, theta0=0.0,
                phiM=30 * np.pi / 180.0, phiK=0.14, Dophi=0 * np.pi / 180.0, phi0=-135 * np.pi / 180.0
            ),
            'side': 'right', 'color': 'green', 'transparency': 0.8, 'visible': True
        },



        # # Dophi=-180 * np.pi / 180.0, # 4th  quadrant motion for second wing
        # # phi0=135 * np.pi / 180.0  # 4th  quadrant motion for second wing
        # {
        #     'motion': dict(
        #         f=1.0,
        #         psiM=60 * np.pi / 180.0, psiC=1.09, Dopsi=-90 * np.pi / 180.0, psi0=90 * np.pi / 180.0,
        #         thetaM=0.0, Dotheta=0.0, thetaN=1.0, theta0=0.0,
        #         phiM=30 * np.pi / 180.0, phiK=0.14, Dophi=-180 * np.pi / 180.0, phi0=135 * np.pi / 180.0
        #     ),
        #     'side': 'left', 'color': 'blue', 'transparency': 0.8, 'visible': True
        # },        
    ]

    # build Wing objects from wings_spec
    wings = []
    for spec in wings_spec:
        motion = spec.get('motion', {})
        # pass raw motion dict to Wing — Wing will call build_params if needed
        w = Wing(
            motion_params=motion,
            side=spec.get('side', 'right'),
            k=spec.get('k', 120),
            cmax=spec.get('cmax', 1.0),
            Rmax=spec.get('Rmax', 1.5),
            color=spec.get('color', 'gray'),
            transparency=spec.get('transparency', 0.0),
            visible=spec.get('visible', True),
            leading_dot_color=spec.get('leading_dot_color', 'cyan'),
            leading_dot_size=spec.get('leading_dot_size', 6)
        )
        wings.append(w)

    # plotting
    fig = plt.figure(figsize=(10,10))
    ax = fig.add_subplot(111, projection='3d')
    axis_scale = 2 * max(w.Rmax for w in wings)
    ax.set_xlim([-axis_scale, axis_scale]); ax.set_ylim([-axis_scale, axis_scale]); ax.set_zlim([-axis_scale, axis_scale])
    ax.set_xlabel('X'); ax.set_ylabel('Y'); ax.set_zlabel('Z')
    ax.xaxis.pane.fill = True; ax.yaxis.pane.fill = True; ax.zaxis.pane.fill = True
    ax.xaxis.pane.set_edgecolor('lightgray'); ax.yaxis.pane.set_edgecolor('lightgray'); ax.zaxis.pane.set_edgecolor('lightgray')
    ax.grid(True)
    ax.plot([-axis_scale, axis_scale], [0,0], [0,0], 'k-', linewidth=2)
    ax.plot([0,0], [-axis_scale, axis_scale], [0,0], 'k-', linewidth=2)
    ax.plot([0,0], [0,0], [-axis_scale, axis_scale], 'k-', linewidth=2)
    ax.text(axis_scale, 0, 0, r'$\mathbf{X}$', fontsize=14, zorder=10)
    ax.text(0, axis_scale, 0, r'$\mathbf{Y}$', fontsize=14, zorder=10)
    ax.text(0, 0, axis_scale, r'$\mathbf{Z}$', fontsize=14, zorder=10)

    # add collections
    for w in wings:
        ax.add_collection3d(w.collection)

    # animation config: set animated_wing_index to an int to animate only one wing, or None for all
    animated_wing_index = None
    hide_non_animated = False
    num_frames = 300

    animator = OverlayAnimator(wings, ax, animated_wing_index=animated_wing_index,
                               num_frames=num_frames, hide_non_animated=hide_non_animated,
                               leading_dot_color='cyan', leading_dot_size=7)
    animator.init_plot(axis_scale=axis_scale, n_grid_lines=15)

    anim = FuncAnimation(fig, animator.animate, frames=num_frames, interval=40, blit=False, repeat=True)
    plt.tight_layout()
    plt.show()
