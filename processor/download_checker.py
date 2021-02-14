#!/usr/bin/env python3
from itertools import product

from utils import create_filename
from commons.log import log
from commons.util import exists, filename

from .processor import Processor


class DownloadChecker(Processor):
    """
        Preprocessor for splitting original videos
    """

    def __init__(self, args=None):
        super().__init__('download_checker', args)
        self.url = self.get_arg("url")

    def run(self, group, rows):
        """
        Example: http://csr.bu.edu/ftp/asl/asllvd/asl-data2/quicktime/
        <session>/scene<scene#>-camera<camera#>.mov
        """
        if group:
            self.download_files_in_metadata(group, rows, self.url,
                                            self.get_cameras(),
                                            self.output_dir)

    def download_files_in_metadata(self, group, rows, urls, cameras, output_dir):
        # ext = extension(url)
        extensions = ["vid", "mov"]
        exts_cams = list(product(extensions, cameras))

        import pandas as pd
        output = pd.DataFrame(
            columns=["video", "# files", "file"] + [f"{e} - cam {c}" for e, c in exts_cams])

        for idx, (session, scene) in enumerate([group]):
            current = dict()

            if session and scene:
                for ext, cam in exts_cams:
                    # Download file:
                    src_url = self.create_source_url(urls[ext],
                                                     session=session,
                                                     scene=scene,
                                                     camera=cam)
                    # log(f"    Downloading '{src_url}'...", 2)

                    current["video"] = f"{session}/scene_{scene}"
                    current["# files"] = len(rows)
                    # current["file"] = ""
                    current[f"{ext} - cam {cam}"] = "Y" if self.is_downloadable(
                        src_url) else "N"

            output = output.append(current, ignore_index=True)
            output = output.append([
                {
                    "video": current["video"],
                    "file": create_filename(session_or_sign=row.label,
                                            person=row.consultant,
                                            scene=row.scene)
                }
                for row in rows.itertuples()], ignore_index=True)

        file = "downloadable_files.csv"
        output.to_csv(file, mode='a', header=not exists(file))

    def create_source_url(self, url, session, scene, camera):
        return url.format(session=session, scene=int(scene), camera=camera)

    def is_downloadable(self, url):
        from requests import get
        return get(url, stream=True).ok
