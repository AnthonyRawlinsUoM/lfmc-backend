import logging
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import pandas as pd
from uuid import uuid4
import asyncio

plt.switch_backend('agg')

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug("logger set to DEBUG")


class MPEGFormatter:

    async def format(self, data, variable):

        video_path = "/FuelModels/queries/"
        video_name = str(uuid4()) + ".mp4"
        video_filepath = video_path + video_name

        # Writer = animation.writers['ffmpeg']
        # writer = Writer(fps=15, metadata=dict(artist='Me'), bitrate=1800)

        frames = []
        fig = plt.figure(figsize=(16, 9), dpi=120)
        plt.ylabel('latitude')
        plt.xlabel('longitude')
        # plt.title(data.attrs["long_name"])  # TODO - not in all datasets!
        logger.debug("\n--> Building MP4")

        times = data[variable]['time'].sortby('time')
        ts = len(times)

        for t in range(0, ts):
            b = data.sel(time=times[t].data)
            if 'mask' in data.data_vars:
                im = b[variable].where(data['mask'] > 0, drop=True)
            else:
                im = b[variable]

            # plt.text(3, 1, "%s" % b["time"].values)
            frame = plt.imshow(im, cmap='viridis_r', animated=True)
            # Push onto array of frames
            frames.append([frame])
            logger.debug("\n--> Generated frame %s of %s" % (t + 1, ts))

        vid = animation.ArtistAnimation(
            fig, frames, interval=50, blit=True, repeat_delay=1000)
        vid.save(video_filepath, writer='ffmpeg', codec='mpeg4')
        logger.debug("\n--> Successfully wrote temp MP4 file.")

        return video_filepath
