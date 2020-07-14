from __future__ import absolute_import, unicode_literals

import os
import argparse
from math import floor
from typing import List

from planetutils import log
from osgeo import gdal
from osgeo.gdal import DEMProcessing, DEMProcessingOptions, Dataset, Band
import multiprocessing

CPU_COUNT = multiprocessing.cpu_count()


class ElevationDEM:

    def __init__(self, processing='hillshade',
                 in_path='./data', out_path='./tiles',
                 exist_path=None, skip_existing=False, zoom=None,
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
        self.exist_path = exist_path if exist_path else out_path
        self.skip_existing = skip_existing
        self.zoom = zoom
        os.environ['GDAL_PAM_ENABLED'] = 'NO'  # suppress xml

    def generate(self):
        in_tiles = []
        exist_tiles = []

        pool = multiprocessing.Pool(processes=floor(multiprocessing.cpu_count() * 1.5))

        in_tiles = [x for x in pool.map_async(self.gather_tiles, self.walk_dir(self.in_path)).get()
                    for x in x]
        log.info(f'Total tiles: {len(in_tiles)}')

        if self.filter_shading:
            tiles_to_process = [x for x in pool.map_async(self.check_density, in_tiles).get() if x is not None]
            log.info(f'Tiles after filter: {len(tiles_to_process)}')
        else:
            tiles_to_process = [slippy for _, slippy in in_tiles]

        if self.skip_existing:
            exist_tiles = [x for x in pool.map_async(self.filter_exists, self.walk_dir(self.exist_path)).get()
                           for x in x]
            log.info(f'Existing tiles: {len(exist_tiles)}')
        in_tiles.clear()

        create_dirs = set()
        generate_tiles = set()
        for z, x, y in tiles_to_process:
            if self.skip_existing and '%s/%s/%s' % (z, x, y) in exist_tiles:
                continue
            create_dirs.add((z, x))
            generate_tiles.add((z, x, y))
        log.info(f'Tiles to generate: {len(generate_tiles)}')
        tiles_to_process.clear()
        exist_tiles.clear()

        for z, x in create_dirs:
            os.makedirs(os.path.join(self.out_path, z, x), exist_ok=True)
        create_dirs.clear()

        for z, x, y in pool.imap_unordered(self.terrainerize, generate_tiles):
            log.info(f'Generated {z}/{x}/{y}')

    @staticmethod
    def walk_dir(path):
        return [(root, files) for root, _, files in os.walk(path) if len(files) > 0]

    def gather_tiles(self, rf):
        root, files = rf
        path = root.split(os.sep)
        tifs = []
        for file in files:
            if '.tif' in file and '.xml' not in file:
                z = path[-2]
                x = path[-1]
                y = file.split('.')[0]
                if self.zoom is not None and self.zoom != z:
                    continue
                tifs.append((root + os.sep + file, (z, x, y)))
        return tifs

    def filter_exists(self, rf):
        root, files = rf
        path = root.split(os.sep)
        existing = []
        for file in files:
            if '.jpg' in file and '.xml' not in file:
                existing.append("%s/%s/%s" % (path[-2], path[-1], file.split('.')[0]))
        return existing

    @staticmethod
    def check_density(t):
        file_path, slippy = t
        try:
            gtif = gdal.Open(file_path)
            if gtif.GetRasterBand(1).ComputeStatistics(0)[2] < 30:
                return None
        except:
            pass
        return slippy

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
            return zxy
        except Exception as exc:
            return zxy


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--inpath', help='Location of input elevation tiles (TIF).', default='.')
    parser.add_argument('--outpath', help='Output path for elevation tiles.', default='.')
    parser.add_argument('--zoom', help='Only generate certain zoom', default=None)
    parser.add_argument('--existpath', help='Different folder to check existing. Useful for processing on '
                                            'faster drives then copying elsewhere for cold storage.', default='.')
    parser.add_argument('--skip-existing', help='Filters out tiles which already exist', action='store_true')
    parser.add_argument('--filter', help='Filters out tiles which would have minimal shading', action='store_true')
    parser.add_argument('--processing', help='DEM process.', default='hillshade')
    parser.add_argument('--format', help='Download format', default='jpeg')

    args = parser.parse_args()

    job = ElevationDEM(in_path=args.inpath, out_path=args.outpath,
                       exist_path=args.existpath, skip_existing=args.skip_existing, zoom=args.zoom,
                       processing=args.processing, out_format=args.format, filter_shading=args.filter)
    job.generate()


if __name__ == '__main__':
    main()
