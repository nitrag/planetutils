#!/usr/bin/env python
from __future__ import absolute_import, unicode_literals
import sqlite3
import os

from . import log
from .tile_math import (
    tile_center_lonlat,
    point_in_polygon,
    get_polygon_coords,
    get_tiles_in_bbox,
    lon_to_tile_x,
    lat_to_tile_y
)


class MBTilesCutter(object):
    """
    Cut (delete) tiles from MBTiles database within polygon boundaries.

    Supports both MBTiles schema types:
    - Denormalized: 'tiles' table stores all data directly
    - Normalized: coordinate table (map/tiles_shallow) + blob table (images/tiles_data)
      with 'tiles' view joining them

    Coordinates are in TMS format (Y from bottom).
    For normalized schemas, automatically cleans up orphaned tile blobs.
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

        # Detect schema type and set appropriate table names
        # Sets: self.tile_table_name, self.blob_table_name, self.fk_column
        self._detect_schema_type()

        log.debug("Using table '%s' for tile operations" % self.tile_table_name)

    def _validate_mbtiles(self):
        """Validate that this is a valid MBTiles database."""
        cursor = self.conn.cursor()

        # Check for tiles table or view
        cursor.execute("PRAGMA table_info(tiles)")
        if not cursor.fetchone():
            raise ValueError("Not a valid MBTiles file: missing 'tiles' table/view")

        # Check schema - tiles must have required columns
        cursor.execute("PRAGMA table_info(tiles)")
        columns = {row[1] for row in cursor.fetchall()}
        required = {'zoom_level', 'tile_column', 'tile_row', 'tile_data'}

        if not required.issubset(columns):
            raise ValueError("Invalid MBTiles schema: missing required columns")

        log.debug("MBTiles validation successful")

    def _detect_schema_type(self):
        """
        Detect schema type and set appropriate table names.

        Sets:
            self.tile_table_name: Table to DELETE from (tiles/map/tiles_shallow)
            self.blob_table_name: Table with tile blobs (None/images/tiles_data)
            self.fk_column: Foreign key column name (None/tile_id/tile_data_id)
        """
        cursor = self.conn.cursor()

        # Check if tiles is a view
        cursor.execute("""
            SELECT type FROM sqlite_master
            WHERE name = 'tiles'
        """)
        result = cursor.fetchone()

        if result and result[0] == 'view':
            # Normalized schema detected
            log.debug("Detected normalized schema (tiles is a view)")

            # Check for map/images convention
            cursor.execute("SELECT name FROM sqlite_master WHERE name = 'map'")
            if cursor.fetchone():
                self.tile_table_name = 'map'
                self.blob_table_name = 'images'
                self.fk_column = 'tile_id'
                log.debug("Using map/images convention")
                return

            # Check for tiles_shallow/tiles_data convention
            cursor.execute("SELECT name FROM sqlite_master WHERE name = 'tiles_shallow'")
            if cursor.fetchone():
                self.tile_table_name = 'tiles_shallow'
                self.blob_table_name = 'tiles_data'
                self.fk_column = 'tile_data_id'
                log.debug("Using tiles_shallow/tiles_data convention")
                return

            raise ValueError(
                "Normalized schema detected (tiles is a view) but could not find:\n"
                "  - map/images tables, or\n"
                "  - tiles_shallow/tiles_data tables"
            )
        else:
            # Denormalized schema: tiles is a real table
            log.debug("Detected denormalized schema (tiles table)")
            self.tile_table_name = 'tiles'
            self.blob_table_name = None
            self.fk_column = None

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

            # Clean up orphaned images in normalized schema
            orphaned = self._cleanup_orphaned_images()
            if orphaned > 0:
                self.conn.commit()
                log.info("Cleaned up %d orphaned images" % orphaned)
        elif self.dry_run:
            log.info("DRY RUN: No changes made to database")

            # Preview orphan cleanup for normalized schema
            orphaned = self._cleanup_orphaned_images()
            if orphaned > 0:
                log.info("DRY RUN: Would clean up %d orphaned images" % orphaned)

        return total_affected

    def _process_single_feature(self, feature, min_zoom, max_zoom):
        """
        Process a single feature - routes to optimized path for rectangles.

        Args:
            feature: Feature object from bbox module
            min_zoom: minimum zoom level
            max_zoom: maximum zoom level

        Returns:
            Number of tiles affected
        """
        if feature.is_rectangle():
            log.debug("Using optimized bbox-based deletion")
            return self._process_rectangle_feature(feature, min_zoom, max_zoom)
        else:
            log.debug("Using polygon-based deletion")
            return self._process_polygon_feature(feature, min_zoom, max_zoom)

    def _process_rectangle_feature(self, feature, min_zoom, max_zoom):
        """
        Process a rectangular feature using optimized range-based deletion.

        Args:
            feature: Feature object where is_rectangle() == True
            min_zoom: minimum zoom level
            max_zoom: maximum zoom level

        Returns:
            Number of tiles affected
        """
        # Get bbox coordinates
        bbox = feature.bbox()
        left, bottom, right, top = bbox
        log.debug("Processing rectangle bbox: %s" % str(bbox))

        affected = 0

        # Process each zoom level
        for zoom in range(min_zoom, max_zoom + 1):
            log.debug("Processing zoom level %d" % zoom)

            # Calculate tile coordinate ranges
            x_min = lon_to_tile_x(left, zoom)
            x_max = lon_to_tile_x(right, zoom)
            y_min = lat_to_tile_y(bottom, zoom)
            y_max = lat_to_tile_y(top, zoom)

            # Safety: ensure min/max order
            if y_min > y_max:
                y_min, y_max = y_max, y_min

            log.debug("Tile range: X[%d-%d] Y[%d-%d]" % (x_min, x_max, y_min, y_max))

            # Delete tiles in range
            count = self._delete_tiles_in_range(zoom, x_min, x_max, y_min, y_max)
            affected += count

            if count > 0:
                log.debug("Deleted %d tiles at zoom %d" % (count, zoom))

        return affected

    def _process_polygon_feature(self, feature, min_zoom, max_zoom):
        """
        Process a single feature using polygon-based deletion.

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
                DELETE FROM %s
                WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?
            """ % self.tile_table_name, (z, x, y))

            if cursor.rowcount > 0:
                deleted += 1

        log.debug("Deleted %d tiles" % deleted)
        return deleted

    def _delete_tiles_in_range(self, zoom, x_min, x_max, y_min, y_max):
        """
        Delete all tiles within a coordinate range using a single SQL query.

        Args:
            zoom: zoom level
            x_min, x_max: tile_column range (inclusive)
            y_min, y_max: tile_row range (inclusive)

        Returns:
            Number of tiles deleted
        """
        if self.dry_run:
            # In dry run, count how many tiles would be deleted
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM %s
                WHERE zoom_level = ?
                  AND tile_column >= ? AND tile_column <= ?
                  AND tile_row >= ? AND tile_row <= ?
            """ % self.tile_table_name, (zoom, x_min, x_max, y_min, y_max))

            count = cursor.fetchone()[0]
            log.debug("DRY RUN: Would delete %d tiles" % count)
            return count

        cursor = self.conn.cursor()
        cursor.execute("""
            DELETE FROM %s
            WHERE zoom_level = ?
              AND tile_column >= ? AND tile_column <= ?
              AND tile_row >= ? AND tile_row <= ?
        """ % self.tile_table_name, (zoom, x_min, x_max, y_min, y_max))

        deleted = cursor.rowcount
        log.debug("Deleted %d tiles" % deleted)
        return deleted

    def _cleanup_orphaned_images(self):
        """Clean up orphaned blobs after deleting from coordinate table."""
        if self.blob_table_name is None:
            return 0  # Only needed for normalized schema

        if self.dry_run:
            cursor = self.conn.cursor()
            # Dynamic query based on detected schema
            query = """
                SELECT COUNT(*)
                FROM %s
                WHERE %s NOT IN (SELECT DISTINCT %s FROM %s)
            """ % (self.blob_table_name, self.fk_column, self.fk_column, self.tile_table_name)

            cursor.execute(query)
            count = cursor.fetchone()[0]
            log.debug("DRY RUN: Would delete %d orphaned blobs from %s" % (count, self.blob_table_name))
            return count

        cursor = self.conn.cursor()
        # Dynamic query based on detected schema
        query = """
            DELETE FROM %s
            WHERE %s NOT IN (SELECT DISTINCT %s FROM %s)
        """ % (self.blob_table_name, self.fk_column, self.fk_column, self.tile_table_name)

        cursor.execute(query)
        deleted = cursor.rowcount

        if deleted > 0:
            log.debug("Cleaned up %d orphaned blobs from %s" % (deleted, self.blob_table_name))
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
