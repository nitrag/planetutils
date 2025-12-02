#!/usr/bin/env python
from __future__ import absolute_import, unicode_literals, print_function
import argparse
import sys

from . import log
from .bbox import load_features_csv, load_features_geojson, load_feature_string
from .mbtiles_cutter import MBTilesCutter


def main():
    parser = argparse.ArgumentParser(
        description='Cut (delete) tiles in MBTiles file within polygon boundaries',
        epilog='Example: mbtiles_cutout --geojson polygon.geojson --min-zoom 8 --max-zoom 12 tiles.mbtiles'
    )

    # Required: MBTiles file
    parser.add_argument('mbtiles_file',
                       help='Path to MBTiles file to modify')

    # Shape input (one required)
    shape_group = parser.add_mutually_exclusive_group(required=True)
    shape_group.add_argument('--geojson',
                            help='GeoJSON file with polygon(s) or bbox(es)')
    shape_group.add_argument('--csv',
                            help='CSV file with bounding box(es). Format: name,left,bottom,right,top')
    shape_group.add_argument('--bbox',
                            help='Single bounding box: left,bottom,right,top')

    # Zoom levels
    parser.add_argument('--min-zoom', type=int, default=0,
                       help='Minimum zoom level (inclusive, default: 0)')
    parser.add_argument('--max-zoom', type=int, default=22,
                       help='Maximum zoom level (inclusive, default: 22)')

    # Performance
    parser.add_argument('--batch-size', type=int, default=1000,
                       help='Number of tiles to process per batch (default: 1000)')

    # Standard flags
    parser.add_argument('--verbose', action='store_true',
                       help='Verbose output')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without modifying the database')

    args = parser.parse_args()

    # Set up logging
    if args.verbose:
        log.set_verbose()

    # Validate zoom levels
    if args.min_zoom < 0 or args.max_zoom > 22:
        log.error("Zoom levels must be between 0 and 22")
        sys.exit(1)

    if args.min_zoom > args.max_zoom:
        log.error("min-zoom must be <= max-zoom")
        sys.exit(1)

    # Load features
    try:
        if args.geojson:
            log.info("Loading features from GeoJSON: %s" % args.geojson)
            features = load_features_geojson(args.geojson)
        elif args.csv:
            log.info("Loading features from CSV: %s" % args.csv)
            features = load_features_csv(args.csv)
        elif args.bbox:
            log.info("Loading bounding box: %s" % args.bbox)
            features = {'bbox': load_feature_string(args.bbox)}
        else:
            log.error("No input specified (this should not happen)")
            sys.exit(1)

        log.info("Loaded %d feature(s)" % len(features))

    except Exception as e:
        log.error("Error loading features: %s" % str(e))
        sys.exit(1)

    # Create cutter
    try:
        cutter = MBTilesCutter(
            mbtiles_path=args.mbtiles_file,
            batch_size=args.batch_size,
            dry_run=args.dry_run
        )

        # Get initial tile count
        initial_count = cutter.get_tile_count()
        log.info("Initial tile count: %d" % initial_count)

    except Exception as e:
        log.error("Error opening MBTiles file: %s" % str(e))
        sys.exit(1)

    # Process features
    try:
        log.info("Processing tiles from zoom %d to %d" % (args.min_zoom, args.max_zoom))

        if args.dry_run:
            log.info("DRY RUN MODE: No changes will be made")

        total_affected = cutter.process_features(
            features=features,
            min_zoom=args.min_zoom,
            max_zoom=args.max_zoom
        )

        # Get final tile count
        final_count = cutter.get_tile_count()

        # Summary
        log.info("-" * 50)
        log.info("Summary:")
        log.info("  Tiles affected: %d" % total_affected)
        log.info("  Initial count:  %d" % initial_count)
        log.info("  Final count:    %d" % final_count)

        if args.dry_run:
            log.info("  (DRY RUN - no changes made)")

        cutter.close()

    except Exception as e:
        log.error("Error processing tiles: %s" % str(e))
        if hasattr(cutter, 'close'):
            cutter.close()
        sys.exit(1)


if __name__ == '__main__':
    main()
