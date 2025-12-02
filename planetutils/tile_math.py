#!/usr/bin/env python
from __future__ import absolute_import, unicode_literals, division
import math

def tile_center_lonlat(z, x, y):
    """
    Get center point of tile in lon/lat coordinates.

    Assumes TMS coordinate system (Y from bottom, as used in MBTiles).

    Args:
        z: zoom level (0-22)
        x: tile column
        y: tile row (TMS: 0 at bottom/south)

    Returns:
        (lon, lat) tuple in degrees
    """
    n = 2 ** z

    # Center of tile (0.5 offset)
    x_center = (x + 0.5) / n

    # For TMS: Y=0 is at bottom, so we need to flip for Web Mercator calculation
    # which expects Y=0 at top
    y_center_xyz = ((n - 1) - y + 0.5) / n

    # Convert to lon/lat
    lon = x_center * 360.0 - 180.0

    # Web Mercator inverse formula (expects XYZ coordinates)
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y_center_xyz)))
    lat = math.degrees(lat_rad)

    return lon, lat


def point_in_polygon(point, polygon):
    """
    Test if point is inside polygon using ray casting algorithm.

    Args:
        point: [lon, lat] or (lon, lat)
        polygon: list of [lon, lat] coordinates forming polygon boundary
                 First and last points should be the same (closed polygon)

    Returns:
        bool: True if point is inside polygon, False otherwise
    """
    x, y = point
    n = len(polygon)
    inside = False

    p1x, p1y = polygon[0]
    for i in range(1, n + 1):
        p2x, p2y = polygon[i % n]

        # Ray casting: cast horizontal ray from point to the right
        # Count intersections with polygon edges
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside

        p1x, p1y = p2x, p2y

    return inside


def get_polygon_coords(geometry):
    """
    Extract polygon coordinates from GeoJSON geometry.

    Handles Polygon, MultiPolygon, and bbox-style LineString geometries.

    Args:
        geometry: GeoJSON geometry dict with 'type' and 'coordinates'

    Returns:
        List of [lon, lat] coordinates forming the outer ring of the polygon
        For MultiPolygon, returns the first polygon's outer ring
    """
    geom_type = geometry.get('type', '')
    coords = geometry.get('coordinates', [])

    if geom_type == 'Polygon':
        # Return outer ring (first ring, others are holes)
        if coords and len(coords) > 0:
            return coords[0]
        return []

    elif geom_type == 'MultiPolygon':
        # Return first polygon's outer ring
        if coords and len(coords) > 0 and len(coords[0]) > 0:
            return coords[0][0]
        return []

    elif geom_type == 'LineString':
        # bbox module uses LineString for simple bboxes
        # Format: [[left, bottom], [right, top]]
        # Convert to closed polygon
        if len(coords) >= 2:
            left, bottom = coords[0]
            right, top = coords[1]
            return [
                [left, bottom],
                [right, bottom],
                [right, top],
                [left, top],
                [left, bottom]
            ]
        return []

    else:
        # Unknown geometry type or invalid format
        return []


def lat_to_tile_y(lat, zoom):
    """
    Convert latitude to tile Y coordinate (TMS).

    Args:
        lat: latitude in degrees (-85.05 to 85.05)
        zoom: zoom level

    Returns:
        int: tile Y coordinate (TMS: 0 at bottom)
    """
    # Clamp latitude to Web Mercator bounds
    lat = max(-85.05112878, min(85.05112878, lat))

    lat_rad = math.radians(lat)
    n = 2 ** zoom

    # Calculate Y in XYZ coordinates (0 at top)
    y_xyz = (1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n

    # Convert XYZ to TMS (flip Y axis: Y_tms = (2^z - 1) - Y_xyz)
    y_tms = int((n - 1) - int(y_xyz))

    return y_tms


def lon_to_tile_x(lon, zoom):
    """
    Convert longitude to tile X coordinate.

    Args:
        lon: longitude in degrees (-180 to 180)
        zoom: zoom level

    Returns:
        int: tile X coordinate
    """
    n = 2 ** zoom
    x = int((lon + 180.0) / 360.0 * n)

    # Clamp to valid range
    x = max(0, min(n - 1, x))

    return x


def get_tiles_in_bbox(bbox, zoom):
    """
    Get all tile coordinates that intersect with a bounding box at given zoom.

    Args:
        bbox: [left, bottom, right, top] in degrees
        zoom: zoom level (0-22)

    Returns:
        List of (tile_column, tile_row) tuples in TMS coordinates
    """
    left, bottom, right, top = bbox

    # Get tile range
    x_min = lon_to_tile_x(left, zoom)
    x_max = lon_to_tile_x(right, zoom)

    # Get Y coordinates for top and bottom
    y_top = lat_to_tile_y(top, zoom)
    y_bottom = lat_to_tile_y(bottom, zoom)

    # In TMS, higher latitude = higher Y value
    # So bottom (lower lat) gives smaller Y, top (higher lat) gives larger Y
    # But we need to handle cases where this might be reversed
    y_min = min(y_top, y_bottom)
    y_max = max(y_top, y_bottom)

    # Generate all tiles in range
    tiles = []
    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            tiles.append((x, y))

    return tiles
