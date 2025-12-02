#!/usr/bin/env python
from __future__ import absolute_import, unicode_literals
import sqlite3
import os

from . import log
from .tile_math import (
    tile_center_lonlat,
    point_in_polygon,
    get_polygon_coords,
    get_tiles_in_bbox
)


class MBTilesCutter(object):
    """
    Cut (delete) tiles from MBTiles database within polygon boundaries.

    MBTiles format:
    - SQLite database with 'tiles' table
    - Columns: zoom_level (int), tile_column (int), tile_row (int), tile_data (blob)
    - Coordinates are in TMS format (Y from bottom)
    """

    def __init__(self, mbtiles_path, batch_size=1000, dry_run=False):
        """
        Initialize MBTiles cutter.

        Args:
            mbtiles_path: Path to MBTiles SQLite database file
            batch_size: Number of tiles to delete per batch (default: 1000)
            dry_run: If True, report what would be deleted without modifying database
        """
        self.mbtiles_path = mbtiles_path
        self.batch_size = batch_size
        self.dry_run = dry_run

        # Validate file exists
        if not os.path.exists(mbtiles_path):
            raise ValueError("MBTiles file does not exist: %s" % mbtiles_path)

        # Connect to database
        self.conn = sqlite3.connect(mbtiles_path)
        self.conn.row_factory = sqlite3.Row

        # Validate it's an MBTiles file
        self._validate_mbtiles()

    def _validate_mbtiles(self):
        """Validate that this is a valid MBTiles database."""
        cursor = self.conn.cursor()

        # Check for tiles table
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='tiles'
        """)

        if not cursor.fetchone():
            raise ValueError("Not a valid MBTiles file: missing 'tiles' table")

        # Check schema
        cursor.execute("PRAGMA table_info(tiles)")
        columns = {row[1] for row in cursor.fetchall()}
        required = {'zoom_level', 'tile_column', 'tile_row', 'tile_data'}

        if not required.issubset(columns):
            raise ValueError("Invalid MBTiles schema: missing required columns")

        log.debug("MBTiles validation successful")

    def process_features(self, features, min_zoom, max_zoom):
        """
        Process multiple features (polygons/bboxes) and delete matching tiles.

        Args:
            features: dict of {name: Feature} from bbox module
            min_zoom: minimum zoom level (inclusive)
            max_zoom: maximum zoom level (inclusive)

        Returns:
            Total number of tiles affected
        """
        total_affected = 0

        for name, feature in features.items():
            log.info("Processing feature: %s" % name)

            affected = self._process_single_feature(feature, min_zoom, max_zoom)

            log.info("Feature '%s': %d tiles affected" % (name, affected))
            total_affected += affected

        if not self.dry_run and total_affected > 0:
            self.conn.commit()
            log.info("Changes committed to database")
        elif self.dry_run:
            log.info("DRY RUN: No changes made to database")

        return total_affected

    def _process_single_feature(self, feature, min_zoom, max_zoom):
        """
        Process a single feature and delete tiles within it.

        Args:
            feature: Feature object from bbox module
            min_zoom: minimum zoom level
            max_zoom: maximum zoom level

        Returns:
            Number of tiles affected
        """
        # Extract polygon coordinates
        polygon = get_polygon_coords(feature.geometry)

        if not polygon:
            log.warning("Could not extract polygon coordinates from feature")
            return 0

        log.debug("Polygon has %d vertices" % len(polygon))

        # Get bbox to limit tile scan
        bbox = feature.bbox()
        log.debug("Feature bbox: %s" % str(bbox))

        # Count affected tiles
        affected = 0

        # Process zoom levels
        for zoom in range(min_zoom, max_zoom + 1):
            log.debug("Processing zoom level %d" % zoom)

            # Get tiles in bbox at this zoom
            tiles_to_check = get_tiles_in_bbox(bbox, zoom)

            log.debug("Checking %d tiles at zoom %d" % (len(tiles_to_check), zoom))

            # Test each tile
            tiles_to_delete = []
            for tile_col, tile_row in tiles_to_check:
                # Get tile center in lon/lat
                lon, lat = tile_center_lonlat(zoom, tile_col, tile_row)

                # Test if center is in polygon
                if point_in_polygon([lon, lat], polygon):
                    tiles_to_delete.append((zoom, tile_col, tile_row))

            if tiles_to_delete:
                log.debug("Found %d tiles inside polygon at zoom %d" %
                         (len(tiles_to_delete), zoom))

                # Delete in batches
                for i in range(0, len(tiles_to_delete), self.batch_size):
                    batch = tiles_to_delete[i:i + self.batch_size]
                    affected += self._delete_tiles(batch)

        return affected

    def _delete_tiles(self, tiles):
        """
        Delete tiles from database.

        Args:
            tiles: list of (zoom, col, row) tuples

        Returns:
            Number of tiles deleted
        """
        if not tiles:
            return 0

        if self.dry_run:
            log.debug("DRY RUN: Would delete %d tiles" % len(tiles))
            return len(tiles)

        cursor = self.conn.cursor()

        # Delete tiles one by one (more compatible than complex IN query)
        deleted = 0
        for z, x, y in tiles:
            cursor.execute("""
                DELETE FROM tiles
                WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?
            """, (z, x, y))

            if cursor.rowcount > 0:
                deleted += 1

        log.debug("Deleted %d tiles" % deleted)
        return deleted

    def get_tile_count(self):
        """
        Get total number of tiles in database.

        Returns:
            int: total tile count
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM tiles")
        count = cursor.fetchone()[0]
        return count

    def close(self):
        """Close database connection."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()

    def __del__(self):
        """Cleanup: close database connection."""
        self.close()
