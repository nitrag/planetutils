from __future__ import absolute_import, unicode_literals

import os
import argparse
from typing import List

from planetutils import log
from osgeo import gdal
from osgeo.gdal import DEMProcessing, DEMProcessingOptions, Dataset, Band
import multiprocessing

CPU_COUNT = multiprocessing.cpu_count()


class ElevationDEM:

    def __init__(self, processing='hillshade',
                 in_path='./data', out_path='./tiles',
                 out_format='JPEG', extension='jpg',
                 z_factor=4, compute_edges=True, combined=True, filter_shading=False):
        self.processing = processing
        self.in_path = in_path
        self.out_path = out_path
        self.extension = extension
        self.format = out_format
        self.z_factor = z_factor
        self.combined = combined
        self.compute_edges = compute_edges
        self.filter_shading = filter_shading

    def generate(self):
        found_tiles = set()
        exists_tiles = set()
        generate_tiles = set()
        for root, dirs, files in os.walk(self.in_path):
            path = root.split(os.sep)
            for file in files:
                if '.tif' in file:
                    if self.filter_shading:
                        try:
                            gtif = gdal.Open(root + os.sep + file)
                            if gtif.GetRasterBand(1).ComputeStatistics(0)[2] < 30:
                                continue
                        except:
                            pass
                    found_tiles.add((path[-2], path[-1], file.split('.')[0]))

        for z, x, y in found_tiles:
            od = self.tile_path(z, x, y)
            od[2] = od[2].replace('tif', self.extension)
            op = os.path.join(self.out_path, *od)
            if self.tile_exists(op):
                exists_tiles.add((z, x, y))
            else:
                os.makedirs(os.path.join(self.out_path, z, x), exist_ok=True)
                generate_tiles.add((z, x, y))

        log.info(f'Exist: {len(exists_tiles)} :: Remaining: {len(generate_tiles)}')
        tile_arr = {(z, x, y) for z, x, y in generate_tiles}
        with multiprocessing.Pool() as pool:
            for x in pool.imap_unordered(self.terrainerize, tile_arr):
                pass

    @staticmethod
    def tile_exists(op):
        return os.path.exists(op)

    @staticmethod
    def tile_path(z, x, y):
        return list(map(str, [z, x, str(y) + '.tif']))

    def terrainerize(self, zxy):
        z, x, y = zxy
        input_file = os.path.join(self.in_path, z, x, y) + '.tif'
        output_file = os.path.join(self.out_path, z, x, y) + '.' + self.extension
        try:
            DEMProcessing(output_file, input_file, self.processing, options=DEMProcessingOptions(
                format=self.format,
                zFactor=self.z_factor,
                combined=self.combined,
                computeEdges=self.compute_edges
            ))
        except Exception as exc:
            log.error(f'Error generating {z}/{x}/{y}')
        else:
            log.info(f'Generated {output_file}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--inpath', help='Location of input elevation tiles (TIF).', default='.')
    parser.add_argument('--outpath', help='Output path for elevation tiles.', default='.')
    parser.add_argument('--filter', help='Filters out tiles which would have minimal shading', action='store_true')
    parser.add_argument('--processing', help='DEM process.', default='hillshade')
    parser.add_argument('--format', help='Download format', default='jpeg')

    args = parser.parse_args()

    job = ElevationDEM(in_path=args.inpath, out_path=args.outpath, processing=args.processing,
                       out_format=args.format, filter_shading=args.filter)
    job.generate()


if __name__ == '__main__':
    main()
