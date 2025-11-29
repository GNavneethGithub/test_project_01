"""
multi_wing_individual_params_with_projections.py

Same as original but shows XY, YZ, XZ projections and 3D view in a single figure
with 4 subplots (2x2).  Updated OverlayAnimator to manage 2D projection collections.
"""

import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d.art3d import Line3DCollection
from matplotlib.animation import FuncAnimation
from matplotlib.collections import LineCollection  # 2D line collections

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
        if not isinstance(motion_params, dict):
            raise TypeError("motion_params must be a dict")
        if 'TWO_PI_F' not in motion_params:
            self.params = build_params(**motion_params)
        else:
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

        # 3D collection
        self.collection = Line3DCollection(segments0, linewidths=1.5)
        self.collection.set_color(self.color)
        self.collection.set_alpha(1.0 - self.transparency)
        self.collection.set_visible(self.visible)

        # placeholders for 2D projection line collections and scatters (created by animator)
        self.linecol_xy = None
        self.linecol_yz = None
        self.linecol_xz = None
        self.leading_scatter_3d = None
        self.leading_scatter_xy = None
        self.leading_scatter_yz = None
        self.leading_scatter_xz = None

    # convenience
    def set_transparency(self, transparency):
        self.transparency = float(min(max(transparency, 0.0), 1.0))
        self.collection.set_alpha(1.0 - self.transparency)
        # 2D line alpha if they exist
        for lc in (self.linecol_xy, self.linecol_yz, self.linecol_xz):
            if lc is not None:
                lc.set_alpha(1.0 - self.transparency)

    def set_color(self, color):
        self.color = color
        self.collection.set_color(color)
        for lc in (self.linecol_xy, self.linecol_yz, self.linecol_xz):
            if lc is not None:
                lc.set_color(color)

    def set_visible(self, visible):
        self.visible = bool(visible)
        self.collection.set_visible(self.visible)
        for sc in (self.leading_scatter_3d, self.leading_scatter_xy, self.leading_scatter_yz, self.leading_scatter_xz):
            if sc is not None:
                sc.set_visible(self.visible)
        for lc in (self.linecol_xy, self.linecol_yz, self.linecol_xz):
            if lc is not None:
                lc.set_visible(self.visible)

    def rotated_segments_and_lead(self, Rmat):
        pts_lead = (Rmat @ self.leading.T).T
        pts_trail = (Rmat @ self.trailing.T).T
        return np.stack([pts_lead, pts_trail], axis=1), pts_lead

# -------------------------
# 3D grid helper (unchanged)
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
        axes,
        animated_wing_index=None,
        num_frames=240,
        hide_non_animated=False,
        leading_dot_color=None,
        leading_dot_size=None,
        global_axis_scale=None,
        n_grid_lines=11
    ):
        """
        axes: either a single 3D-mpl axes (old behaviour) OR a tuple:
              (ax3d, ax_xy, ax_yz, ax_xz)
        """
        if not all(hasattr(w, 'params') for w in wings):
            raise TypeError("wings must be Wing instances with .params")

        self.wings = wings
        # axes handling: support both single-ax backwards compat and tuple-of-axes
        if isinstance(axes, tuple) or isinstance(axes, list):
            if len(axes) != 4:
                raise ValueError("When passing axes as tuple/list provide (ax3d, ax_xy, ax_yz, ax_xz)")
            self.ax3d, self.ax_xy, self.ax_yz, self.ax_xz = axes
        else:
            # single 3D axis -> keep older behaviour and set 2D axes to None
            self.ax3d = axes
            self.ax_xy = self.ax_yz = self.ax_xz = None

        self.animated_wing_index = animated_wing_index
        self.num_frames = int(num_frames)
        self.hide_non_animated = bool(hide_non_animated)

        self.leading_dot_default_color = leading_dot_color if leading_dot_color is not None else 'blue'
        self.leading_dot_default_size = leading_dot_size if leading_dot_size is not None else 6

        self.axis_scale = global_axis_scale
        self.n_grid_lines = int(n_grid_lines)

        freqs = [w.params.get('f', 1.0) for w in wings] if wings else [1.0]
        self.f_ref = max(freqs) if freqs else 1.0
        self.dt = 1.0 / ((self.num_frames - 1) * self.f_ref) if self.num_frames > 1 else 0.01

        self.grid_collection = None

    def init_plot(self, axis_scale=None, n_grid_lines=None):
        axis_scale = axis_scale or self.axis_scale or (2.0 * max(w.Rmax for w in self.wings))
        n_grid_lines = n_grid_lines or self.n_grid_lines

        # 3D grid (only for ax3d if present)
        if self.ax3d is not None:
            self.grid_collection = make_3d_grid(axis_scale, n_lines=n_grid_lines)
            self.grid_collection.set_color((0.45, 0.45, 0.45))
            self.ax3d.add_collection3d(self.grid_collection)
            # axes limits and labels
            self.ax3d.set_xlim([-axis_scale, axis_scale]); self.ax3d.set_ylim([-axis_scale, axis_scale]); self.ax3d.set_zlim([-axis_scale, axis_scale])
            self.ax3d.set_xlabel('X'); self.ax3d.set_ylabel('Y'); self.ax3d.set_zlabel('Z')
            self.ax3d.xaxis.pane.fill = True; self.ax3d.yaxis.pane.fill = True; self.ax3d.zaxis.pane.fill = True
            self.ax3d.xaxis.pane.set_edgecolor('lightgray'); self.ax3d.yaxis.pane.set_edgecolor('lightgray'); self.ax3d.zaxis.pane.set_edgecolor('lightgray')
            self.ax3d.plot([-axis_scale, axis_scale], [0,0], [0,0], 'k-', linewidth=2)
            self.ax3d.plot([0,0], [-axis_scale, axis_scale], [0,0], 'k-', linewidth=2)
            self.ax3d.plot([0,0], [0,0], [-axis_scale, axis_scale], 'k-', linewidth=2)
            self.ax3d.text(axis_scale, 0, 0, r'$\mathbf{X}$', fontsize=12, zorder=10)
            self.ax3d.text(0, axis_scale, 0, r'$\mathbf{Y}$', fontsize=12, zorder=10)
            self.ax3d.text(0, 0, axis_scale, r'$\mathbf{Z}$', fontsize=12, zorder=10)

        # 2D axes configuration
        for ax2 in (self.ax_xy, self.ax_yz, self.ax_xz):
            if ax2 is not None:
                ax2.set_xlim([-axis_scale, axis_scale]); ax2.set_ylim([-axis_scale, axis_scale])
                ax2.grid(True); ax2.set_aspect('equal', 'box')

        if self.ax_xy is not None:
            self.ax_xy.set_xlabel('X'); self.ax_xy.set_ylabel('Y'); self.ax_xy.set_title('XY projection')
        if self.ax_yz is not None:
            self.ax_yz.set_xlabel('Y'); self.ax_yz.set_ylabel('Z'); self.ax_yz.set_title('YZ projection')
        if self.ax_xz is not None:
            self.ax_xz.set_xlabel('X'); self.ax_xz.set_ylabel('Z'); self.ax_xz.set_title('XZ projection')

        # add wing line collections (3D) and create leading scatters and 2D projections
        for w in self.wings:
            # add the 3D collection
            if self.ax3d is not None:
                self.ax3d.add_collection3d(w.collection)

            # create per-wing 2D projection line collections from initial geometry (world-frame)
            # create XY: lines between leading and trailing, as pairs of (x,y)
            segs_xy = [ [(p1[0], p1[1]), (p2[0], p2[1])] for p1,p2 in np.stack([w.leading, w.trailing], axis=1) ]
            segs_yz = [ [(p1[1], p1[2]), (p2[1], p2[2])] for p1,p2 in np.stack([w.leading, w.trailing], axis=1) ]
            segs_xz = [ [(p1[0], p1[2]), (p2[0], p2[2])] for p1,p2 in np.stack([w.leading, w.trailing], axis=1) ]

            if self.ax_xy is not None:
                w.linecol_xy = LineCollection(segs_xy, linewidths=1.5)
                w.linecol_xy.set_color(w.color); w.linecol_xy.set_alpha(1.0 - w.transparency)
                w.linecol_xy.set_visible(w.visible)
                self.ax_xy.add_collection(w.linecol_xy)

                # leading scatter in XY
                lead = w.leading
                w.leading_scatter_xy = self.ax_xy.scatter(lead[:,0], lead[:,1], s=w.leading_dot_size, c=w.leading_dot_color)
                w.leading_scatter_xy.set_visible(w.visible)

            if self.ax_yz is not None:
                w.linecol_yz = LineCollection(segs_yz, linewidths=1.5)
                w.linecol_yz.set_color(w.color); w.linecol_yz.set_alpha(1.0 - w.transparency)
                w.linecol_yz.set_visible(w.visible)
                self.ax_yz.add_collection(w.linecol_yz)

                # leading scatter in YZ (plot y vs z)
                w.leading_scatter_yz = self.ax_yz.scatter(lead[:,1], lead[:,2], s=w.leading_dot_size, c=w.leading_dot_color)
                w.leading_scatter_yz.set_visible(w.visible)

            if self.ax_xz is not None:
                w.linecol_xz = LineCollection(segs_xz, linewidths=1.5)
                w.linecol_xz.set_color(w.color); w.linecol_xz.set_alpha(1.0 - w.transparency)
                w.linecol_xz.set_visible(w.visible)
                self.ax_xz.add_collection(w.linecol_xz)

                # leading scatter in XZ (plot x vs z)
                w.leading_scatter_xz = self.ax_xz.scatter(lead[:,0], lead[:,2], s=w.leading_dot_size, c=w.leading_dot_color)
                w.leading_scatter_xz.set_visible(w.visible)

            # 3D leading scatter (if ax3d present)
            if self.ax3d is not None:
                lead = w.leading
                w.leading_scatter_3d = self.ax3d.scatter(lead[:,0], lead[:,1], lead[:,2], s=w.leading_dot_size, c=w.leading_dot_color)
                w.leading_scatter_3d.set_visible(w.visible)

        # apply initial visibility/transparency rules
        self.apply_visibility_alpha_rules()

    def apply_visibility_alpha_rules(self):
        for idx, w in enumerate(self.wings):
            if self.animated_wing_index is None:
                v = bool(w.visible)
                w.collection.set_visible(v)
                w.collection.set_alpha(1.0 - w.transparency)
                for lc in (w.linecol_xy, w.linecol_yz, w.linecol_xz):
                    if lc is not None:
                        lc.set_visible(v); lc.set_alpha(1.0 - w.transparency)
                for sc in (w.leading_scatter_3d, w.leading_scatter_xy, w.leading_scatter_yz, w.leading_scatter_xz):
                    if sc is not None:
                        sc.set_visible(v)
            else:
                if idx == self.animated_wing_index:
                    v = bool(w.visible)
                    w.collection.set_visible(v)
                    w.collection.set_alpha(1.0 - w.transparency)
                else:
                    if self.hide_non_animated:
                        w.collection.set_visible(False)
                        for lc in (w.linecol_xy, w.linecol_yz, w.linecol_xz):
                            if lc is not None: lc.set_visible(False)
                        for sc in (w.leading_scatter_3d, w.leading_scatter_xy, w.leading_scatter_yz, w.leading_scatter_xz):
                            if sc is not None: sc.set_visible(False)
                    else:
                        v = bool(w.visible)
                        w.collection.set_visible(v)
                        w.collection.set_alpha(1.0 - w.transparency)
                        for lc in (w.linecol_xy, w.linecol_yz, w.linecol_xz):
                            if lc is not None: lc.set_visible(v); lc.set_alpha(1.0 - w.transparency)
                        for sc in (w.leading_scatter_3d, w.leading_scatter_xy, w.leading_scatter_yz, w.leading_scatter_xz):
                            if sc is not None: sc.set_visible(v)

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
                # hide 2D leading if they exist
                for sc in (w.leading_scatter_xy, w.leading_scatter_yz, w.leading_scatter_xz):
                    if sc is not None:
                        sc.set_visible(False)
                continue

            angles = xyz_with_params(t, w.params)
            R = tBW(angles)
            segments3d, pts_lead = w.rotated_segments_and_lead(R)
            # update 3D segments
            w.collection.set_segments(segments3d)

            # update 3D leading scatter
            if w.leading_scatter_3d is not None:
                xs, ys, zs = pts_lead[:,0], pts_lead[:,1], pts_lead[:,2]
                w.leading_scatter_3d._offsets3d = (xs, ys, zs)
                w.leading_scatter_3d.set_visible(True)

            # update 2D projections
            # create 2D segments in requested orders
            segs_xy = [ [(p1[0], p1[1]), (p2[0], p2[1])] for p1,p2 in segments3d ]
            segs_yz = [ [(p1[1], p1[2]), (p2[1], p2[2])] for p1,p2 in segments3d ]
            segs_xz = [ [(p1[0], p1[2]), (p2[0], p2[2])] for p1,p2 in segments3d ]

            if w.linecol_xy is not None:
                w.linecol_xy.set_segments(segs_xy)
                w.linecol_xy.set_visible(w.visible)
            if w.linecol_yz is not None:
                w.linecol_yz.set_segments(segs_yz)
                w.linecol_yz.set_visible(w.visible)
            if w.linecol_xz is not None:
                w.linecol_xz.set_segments(segs_xz)
                w.linecol_xz.set_visible(w.visible)

            # update 2D leading scatters
            xs, ys, zs = pts_lead[:,0], pts_lead[:,1], pts_lead[:,2]
            if w.leading_scatter_xy is not None:
                # set_offsets expects Nx2 array-like
                w.leading_scatter_xy.set_offsets(np.column_stack([xs, ys]))
                w.leading_scatter_xy.set_visible(w.visible)
            if w.leading_scatter_yz is not None:
                w.leading_scatter_yz.set_offsets(np.column_stack([ys, zs]))
                w.leading_scatter_yz.set_visible(w.visible)
            if w.leading_scatter_xz is not None:
                w.leading_scatter_xz.set_offsets(np.column_stack([xs, zs]))
                w.leading_scatter_xz.set_visible(w.visible)

        # update title for 3D axes only (if present)
        if self.ax3d is not None:
            if self.animated_wing_index is None:
                title = f't = {t:.3f} (frame {frame}/{self.num_frames - 1}) — animating ALL wings'
            else:
                a = self.wings[self.animated_wing_index]
                ang = xyz_with_params(t, a.params) if a.collection.get_visible() else np.array([np.nan, np.nan, np.nan])
                title = f't = {t:.3f} (frame {frame}/{self.num_frames - 1}) — wing {self.animated_wing_index} ψ={np.degrees(ang[0]):.1f}°'
            self.ax3d.set_title(title, fontsize=9)

        # return visible artists for blitting (we're not using blit here but keep list)
        artists = []
        for w in self.wings:
            if w.collection.get_visible():
                artists.append(w.collection)
            for sc in (w.leading_scatter_3d, w.leading_scatter_xy, w.leading_scatter_yz, w.leading_scatter_xz):
                if sc is not None and sc.get_visible():
                    artists.append(sc)
            for lc in (w.linecol_xy, w.linecol_yz, w.linecol_xz):
                if lc is not None and lc.get_visible():
                    artists.append(lc)
        if self.grid_collection is not None:
            artists.append(self.grid_collection)
        return artists

# -------------------------
# Example usage
# -------------------------
if __name__ == '__main__':

    wings_spec = [
        # 1st quadrant motion for fourth wing     
        {
            'motion': dict(
                f=1.0,
                psiM=60 * np.pi / 180.0, psiC=1.09, Dopsi=-90 * np.pi / 180.0, psi0=90 * np.pi / 180.0,
                thetaM=0.0, Dotheta=0.0, thetaN=1.0, theta0=0.0,
                phiM=30 * np.pi / 180.0, phiK=0.14, Dophi=0.0* np.pi / 180.0, phi0=45 * np.pi / 180.0
            ),
            'side': 'right', 'color': 'blue', 'transparency': 0.8, 'visible': True
        },


        # 2nd quadrant motion for fourth wing
        {
            'motion': dict(
                f=1.0,
                psiM=60 * np.pi / 180.0, psiC=1.09, Dopsi=-90 * np.pi / 180.0, psi0=90 * np.pi / 180.0,
                thetaM=0.0, Dotheta=0.0, thetaN=1.0, theta0=0.0,
                phiM=30 * np.pi / 180.0, phiK=0.14, Dophi=180 * np.pi / 180.0, phi0=-45 * np.pi / 180.0
            ),
            'side': 'left', 'color': 'blue', 'transparency': 0.8, 'visible': True
        },

        # 3rd quadrant motion for thrid wing   
        {
            'motion': dict(
                f=1.0,
                psiM=60 * np.pi / 180.0, psiC=1.09, Dopsi=-90 * np.pi / 180.0, psi0=90 * np.pi / 180.0,
                thetaM=0.0, Dotheta=0.0, thetaN=1.0, theta0=0.0,
                phiM=30 * np.pi / 180.0, phiK=0.14, Dophi=0 * np.pi / 180.0, phi0=-135 * np.pi / 180.0
            ),
            'side': 'right', 'color': 'blue', 'transparency': 0.8, 'visible': True
        },
        
        # 4th  quadrant motion for second wing    
        {
            'motion': dict(
                f=1.0,
                psiM=60 * np.pi / 180.0, psiC=1.09, Dopsi=-90 * np.pi / 180.0, psi0=90 * np.pi / 180.0,
                thetaM=0.0, Dotheta=0.0, thetaN=1.0, theta0=0.0,
                phiM=30 * np.pi / 180.0, phiK=0.14, Dophi=-180 * np.pi / 180.0, phi0=135 * np.pi / 180.0
            ),
            'side': 'left', 'color': 'blue', 'transparency': 0.8, 'visible': True
        },  
      
    ]

    # build Wing objects from wings_spec
    wings = []
    for spec in wings_spec:
        motion = spec.get('motion', {})
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

    # create 2x2 figure: 3D top-left, XY top-right, YZ bottom-left, XZ bottom-right
    fig = plt.figure(figsize=(12, 9))
    ax3d = fig.add_subplot(2, 2, 1, projection='3d')
    ax_xy = fig.add_subplot(2, 2, 2)
    ax_yz = fig.add_subplot(2, 2, 3)
    ax_xz = fig.add_subplot(2, 2, 4)

    axis_scale = 2 * max(w.Rmax for w in wings)
    animated_wing_index = None
    hide_non_animated = False
    num_frames = 300

    animator = OverlayAnimator(wings, (ax3d, ax_xy, ax_yz, ax_xz),
                               animated_wing_index=animated_wing_index,
                               num_frames=num_frames, hide_non_animated=hide_non_animated,
                               leading_dot_color='cyan', leading_dot_size=7)
    animator.init_plot(axis_scale=axis_scale, n_grid_lines=15)

    anim = FuncAnimation(fig, animator.animate, frames=num_frames, interval=40, blit=False, repeat=True)
    plt.tight_layout()
    plt.show()
