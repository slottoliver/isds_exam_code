import plotly.graph_objects as go
import plotly.io as pio
from PIL import Image
import os
from user_library.shared_functions import _test_mkdir


def plot_panel(df, x_values, y_values, color, save=False):
    
    fig = go.Figure()
    
    # set color scale
    base_color = (146,29,32)
    colors = df[color].unique().tolist()
    num_colors = len(colors)
    factor = 40
    color_scale = [
        f'rgb({base_color[0]+(i*factor)}, {base_color[1]+(i*factor)}, {base_color[2]+(i*factor)})' \
            for i in range(num_colors)
    ]
    
    # add traces
    for idx, color_ in enumerate(colors):
        temp = df.query(f'{color} == "{color_}"')
        x_, y_, z_ = [temp[column].tolist() for column in [x_values, y_values, 'Timestamp']]
        fig.add_trace(
            go.Scatter3d(
                x=x_, 
                y=y_, 
                z=[f'{z.year}Q{z.quarter}' for z in z_],
                name=color_,
                mode='markers',
                marker=dict(
                    color=color_scale[idx],
                    opacity=0.7,
                    size=11,
                )
            )
        )
        
    # update axis
    fig.update_layout(
        scene = dict(
            xaxis = dict(
                backgroundcolor='rgb(238, 216, 216, 1)',
                gridcolor='rgb(226, 209, 209, 1)',
                zeroline=False,
                linecolor='grey',
                showline=True
            ),
            yaxis = dict(
                backgroundcolor='rgb(247, 226, 228, 1)',
                gridcolor='rgb(226, 209, 209, 1)',
                zeroline=False,
                linecolor='grey',
                showline=True
            ),
            zaxis = dict(
                backgroundcolor='rgb(220, 181, 181, 1)',
                gridcolor='rgb(226, 209, 209, 1)',
                zeroline=False,
                linecolor='grey',
                showline=True
            ),
            aspectratio = dict(
                x=1, 
                y=1.1, 
                z=1
            ),
            xaxis_title=x_values,
            yaxis_title=y_values,
            zaxis_title='Timestamp'
        ),
        scene_camera = dict(
            eye = dict(
                x=-1.3, 
                y=1.3, 
                z=0.2
            ),
            center = dict(
                x=0, 
                y=0.12, 
                z=-0.12
            )
        ),
        width = 550,
        height = 400,
        margin = dict(
            r=0, l=0,
            b=0, t=0
        ),
        legend = dict(
            orientation='h',
            xanchor='center',
            x=0.5,
            y=1
        )
    )
    
    # set font and size
    fig.update_layout(
        font_family = 'Serif',
        font_size = 11
    )
    
    # save
    if save:
        title = f'{x_values} and {y_values}'.replace(' ', '_').replace('/', '')
        if title[-1] == '.':
            title += 'png'
        else:
            title += '.png'
        _test_mkdir('images')
        pio.write_image(fig, f'images/{title}', scale=2, width=500)
    
    return fig

def create_gif(path, keyword, destination):
    _test_mkdir(destination)
    images = sorted([f for f in os.listdir(path) if f.endswith('.png') and f.startswith(keyword)])
    duration = 2.5e+3
    load = [Image.open(os.path.join(path, f)) for f in images]
    load[0].save(os.path.join(destination, keyword + '.gif'), save_all=True, append_images=load[1:], duration=duration, loop=0)
    
# create word cloud