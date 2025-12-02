from __future__ import absolute_import, unicode_literals
import tempfile
import os
import sqlite3
import unittest

from planetutils.mbtiles_cutter import MBTilesCutter
from planetutils.tile_math import (
    point_in_polygon,
    tile_center_lonlat,
    get_polygon_coords,
    lat_to_tile_y,
    lon_to_tile_x,
    get_tiles_in_bbox
)
from planetutils.bbox import Feature


class TestTileMath(unittest.TestCase):
    """Test tile math utility functions."""

    def test_point_in_polygon_square(self):
        """Test point in square polygon."""
        polygon = [
            [-122.5, 37.5],
            [-122.0, 37.5],
            [-122.0, 38.0],
            [-122.5, 38.0],
            [-122.5, 37.5]
        ]

        # Point inside
        self.assertTrue(point_in_polygon([-122.25, 37.75], polygon))

        # Points outside
        self.assertFalse(point_in_polygon([-123.0, 37.75], polygon))
        self.assertFalse(point_in_polygon([-122.25, 38.5], polygon))
        self.assertFalse(point_in_polygon([-121.5, 37.75], polygon))

    def test_point_in_polygon_triangle(self):
        """Test point in triangular polygon."""
        polygon = [
            [0, 0],
            [10, 0],
            [5, 10],
            [0, 0]
        ]

        self.assertTrue(point_in_polygon([5, 5], polygon))
        self.assertTrue(point_in_polygon([3, 3], polygon))
        self.assertFalse(point_in_polygon([0, 10], polygon))
        self.assertFalse(point_in_polygon([11, 5], polygon))

    def test_tile_center(self):
        """Test tile center calculation."""
        # Zoom 0, tile 0,0 should be near center of world
        lon, lat = tile_center_lonlat(0, 0, 0)
        # Center should be around (0, 0) but TMS affects the exact values
        self.assertGreater(lon, -180)
        self.assertLess(lon, 180)
        self.assertGreater(lat, -85)
        self.assertLess(lat, 85)

        # Zoom 1, tile 0,0 should be in southwest quadrant
        lon, lat = tile_center_lonlat(1, 0, 0)
        self.assertLess(lon, 0)  # Western hemisphere
        self.assertLess(lat, 0)  # Southern hemisphere

    def test_lon_to_tile_x(self):
        """Test longitude to tile X conversion."""
        # At zoom 0, there's only 1 tile (x=0)
        self.assertEqual(lon_to_tile_x(-180, 0), 0)
        self.assertEqual(lon_to_tile_x(0, 0), 0)
        self.assertEqual(lon_to_tile_x(180, 0), 0)

        # At zoom 1, there are 2 tiles
        self.assertEqual(lon_to_tile_x(-180, 1), 0)
        self.assertEqual(lon_to_tile_x(-1, 1), 0)
        self.assertEqual(lon_to_tile_x(1, 1), 1)
        self.assertEqual(lon_to_tile_x(180, 1), 1)

    def test_lat_to_tile_y(self):
        """Test latitude to tile Y conversion."""
        # At zoom 0, there's only 1 tile (y=0)
        y = lat_to_tile_y(0, 0)
        self.assertEqual(y, 0)

        # At zoom 1, there are 2 tiles
        # Positive latitude (north) should give higher Y in TMS
        y_north = lat_to_tile_y(45, 1)
        y_south = lat_to_tile_y(-45, 1)
        self.assertGreater(y_north, y_south)

    def test_get_tiles_in_bbox(self):
        """Test getting tiles within bounding box."""
        # Small bbox at zoom 2
        bbox = [-10, -10, 10, 10]
        tiles = get_tiles_in_bbox(bbox, 2)

        # Should get at least one tile
        self.assertGreater(len(tiles), 0)

        # All tiles should be tuples of (x, y)
        for tile in tiles:
            self.assertEqual(len(tile), 2)
            x, y = tile
            self.assertGreaterEqual(x, 0)
            self.assertGreaterEqual(y, 0)
            self.assertLess(x, 2**2)
            self.assertLess(y, 2**2)


class TestGetPolygonCoords(unittest.TestCase):
    """Test polygon coordinate extraction from GeoJSON."""

    def test_polygon(self):
        """Test extracting coords from Polygon geometry."""
        geom = {
            "type": "Polygon",
            "coordinates": [
                [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]],
                [[2, 2], [2, 8], [8, 8], [8, 2], [2, 2]]  # hole
            ]
        }
        coords = get_polygon_coords(geom)
        self.assertEqual(len(coords), 5)
        self.assertEqual(coords[0], [0, 0])
        self.assertEqual(coords[-1], [0, 0])  # Closed polygon

    def test_multipolygon(self):
        """Test extracting coords from MultiPolygon geometry."""
        geom = {
            "type": "MultiPolygon",
            "coordinates": [
                [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
                [[[20, 20], [30, 20], [30, 30], [20, 30], [20, 20]]]
            ]
        }
        coords = get_polygon_coords(geom)
        # Should return first polygon
        self.assertEqual(len(coords), 5)
        self.assertEqual(coords[0], [0, 0])

    def test_linestring_bbox(self):
        """Test converting LineString (bbox format) to polygon."""
        geom = {
            "type": "LineString",
            "coordinates": [[-122.5, 37.5], [-122.0, 38.0]]
        }
        coords = get_polygon_coords(geom)
        self.assertEqual(len(coords), 5)
        # Should form closed rectangle
        self.assertEqual(coords[0], coords[-1])
        # Check corners
        self.assertEqual(coords[0], [-122.5, 37.5])  # bottom-left
        self.assertEqual(coords[2], [-122.0, 38.0])  # top-right


class TestMBTilesCutter(unittest.TestCase):
    """Test MBTilesCutter class with temporary database."""

    def setUp(self):
        """Create a temporary MBTiles file for testing."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.mbtiles')
        self.temp_db.close()

        # Create MBTiles schema
        conn = sqlite3.connect(self.temp_db.name)
        conn.execute("""
            CREATE TABLE tiles (
                zoom_level INTEGER,
                tile_column INTEGER,
                tile_row INTEGER,
                tile_data BLOB,
                PRIMARY KEY (zoom_level, tile_column, tile_row)
            )
        """)
        conn.execute("""
            CREATE TABLE metadata (
                name TEXT,
                value TEXT
            )
        """)

        # Insert some test tiles at zoom 2 (world is 4x4 tiles)
        # Insert tiles covering whole world
        for x in range(4):
            for y in range(4):
                conn.execute(
                    "INSERT INTO tiles VALUES (?, ?, ?, ?)",
                    (2, x, y, b'fake_tile_data')
                )

        conn.commit()
        conn.close()

    def tearDown(self):
        """Clean up temporary file."""
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)

    def test_validate_mbtiles(self):
        """Test MBTiles validation."""
        cutter = MBTilesCutter(self.temp_db.name)
        self.assertIsNotNone(cutter)
        cutter.close()

    def test_invalid_file(self):
        """Test error handling for invalid file."""
        with self.assertRaises(ValueError):
            MBTilesCutter('/nonexistent/file.mbtiles')

    def test_get_tile_count(self):
        """Test getting tile count."""
        cutter = MBTilesCutter(self.temp_db.name)
        count = cutter.get_tile_count()
        self.assertEqual(count, 16)  # 4x4 tiles at zoom 2
        cutter.close()

    def test_cut_operation_dry_run(self):
        """Test cut operation in dry run mode."""
        # Create a feature covering western hemisphere
        feature = Feature(geometry={
            "type": "Polygon",
            "coordinates": [[
                [-180, -85], [0, -85], [0, 85], [-180, 85], [-180, -85]
            ]]
        })

        cutter = MBTilesCutter(self.temp_db.name, dry_run=True)
        affected = cutter.process_features(
            features={'test': feature},
            min_zoom=2,
            max_zoom=2
        )

        # Should report tiles but not actually delete
        self.assertGreater(affected, 0)

        # Verify tiles still exist
        count = cutter.get_tile_count()
        self.assertEqual(count, 16)  # All tiles still there

        cutter.close()

    def test_cut_operation_real(self):
        """Test actual cut operation."""
        # Create a feature covering western hemisphere
        feature = Feature(geometry={
            "type": "Polygon",
            "coordinates": [[
                [-180, -85], [0, -85], [0, 85], [-180, 85], [-180, -85]
            ]]
        })

        cutter = MBTilesCutter(self.temp_db.name, dry_run=False)
        initial_count = cutter.get_tile_count()

        affected = cutter.process_features(
            features={'test': feature},
            min_zoom=2,
            max_zoom=2
        )

        self.assertGreater(affected, 0)

        # Verify some tiles were deleted
        final_count = cutter.get_tile_count()
        self.assertLess(final_count, initial_count)
        self.assertEqual(initial_count - final_count, affected)

        cutter.close()

    def test_cut_small_bbox(self):
        """Test cutting tiles within a small bounding box."""
        # Bbox centered on tile (1,2) at zoom 2 (center at -45°, 40.98°)
        # Make bbox large enough to contain this tile center
        feature = Feature(geometry={
            "type": "Polygon",
            "coordinates": [[
                [-60, 20], [-30, 20], [-30, 50], [-60, 50], [-60, 20]
            ]]
        })

        cutter = MBTilesCutter(self.temp_db.name, dry_run=False)
        initial_count = cutter.get_tile_count()

        affected = cutter.process_features(
            features={'small_box': feature},
            min_zoom=2,
            max_zoom=2
        )

        # Should affect at least 1 tile (tile 1,2 at -45, 40.98)
        self.assertGreater(affected, 0)

        # Should not delete all tiles
        final_count = cutter.get_tile_count()
        self.assertLess(affected, initial_count)
        self.assertGreater(final_count, 0)

        cutter.close()

    def test_multiple_features(self):
        """Test processing multiple features."""
        # Two separate polygons covering different tile centers
        # Tile (0,1) center: -135, -40.98
        # Tile (3,2) center: 135, 40.98
        feature1 = Feature(geometry={
            "type": "Polygon",
            "coordinates": [[
                [-150, -50], [-120, -50], [-120, -30], [-150, -30], [-150, -50]
            ]]
        })
        feature2 = Feature(geometry={
            "type": "Polygon",
            "coordinates": [[
                [120, 30], [150, 30], [150, 50], [120, 50], [120, 30]
            ]]
        })

        cutter = MBTilesCutter(self.temp_db.name, dry_run=False)
        initial_count = cutter.get_tile_count()

        affected = cutter.process_features(
            features={'box1': feature1, 'box2': feature2},
            min_zoom=2,
            max_zoom=2
        )

        # Should affect 2 tiles (one from each feature)
        self.assertGreaterEqual(affected, 2)

        final_count = cutter.get_tile_count()
        self.assertLess(final_count, initial_count)

        cutter.close()


if __name__ == '__main__':
    unittest.main()
