import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

# Set up the figure and axis
fig, ax = plt.subplots(figsize=(10, 6))
ax.set_xlim(0, 4 * np.pi)
ax.set_ylim(-1.5, 1.5)
ax.grid(True, alpha=0.3)
ax.set_xlabel('x')
ax.set_ylabel('sin(x)')
ax.set_title('Point Moving Along sin(x)')

# Plot the sine curve
x = np.linspace(0, 4 * np.pi, 1000)
y = np.sin(x)
ax.plot(x, y, 'b-', linewidth=2, label='sin(x)')

# Initialize the point that will move
point, = ax.plot([], [], 'ro', markersize=10, label='Moving Point')
ax.legend()

# Animation function
def animate(frame):
    # Calculate the position based on frame number
    x_pos = frame * (4 * np.pi) / 100  # Move across 4π in 100 frames
    y_pos = np.sin(x_pos)
    
    point.set_data([x_pos], [y_pos])
    return point,

# Create animation
# frames: number of frames, interval: delay between frames in milliseconds
anim = FuncAnimation(fig, animate, frames=100, interval=50, blit=True, repeat=True)

plt.show()