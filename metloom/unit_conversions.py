"""
Optional unit conversion helpers built on ``pint``.

metloom clients infer a units string for every variable they return (stored in
the ``{variable.name}_units`` column). Those strings come from a variety of
external APIs and are not always directly parseable by pint
(e.g. ``"DEG F"``, ``"w/m^2"``, ``"CFS"``, ``"wmoUnit:degC"``). This module
normalizes those strings and provides a single conversion entry point.

pint is only exercised when a caller explicitly requests a conversion by
passing ``desired_units`` to a ``get_*`` method. When no conversion is
requested none of this code runs and the returned data is unchanged.
"""
import logging

import numpy as np
import pint

LOG = logging.getLogger("metloom.unit_conversions")

# A single shared registry. pint Quantities can only interact when they share
# the same UnitRegistry, so everything in metloom must use this instance.
UREG = pint.UnitRegistry()

# Map the messy unit strings returned by the various data sources onto strings
# that ``UREG`` can parse. Keys are compared case-insensitively after stripping
# surrounding whitespace (see ``normalize_unit``).
_NORMALIZE = {
    # temperature
    "deg f": "degF",
    "degf": "degF",
    "deg_f": "degF",
    "°f": "degF",
    "deg c": "degC",
    "degc": "degC",
    "deg_c": "degC",
    "°c": "degC",
    "celsius": "degC",
    "fahrenheit": "degF",
    # length / depth
    "inches": "inch",
    "in": "inch",
    "feet": "foot",
    "ft": "foot",
    "meters": "meter",
    "metres": "meter",
    "millimeters": "millimeter",
    "millimetres": "millimeter",
    "centimeters": "centimeter",
    "centimetres": "centimeter",
    # irradiance
    "w/m^2": "watt/meter**2",
    "w/m2": "watt/meter**2",
    "w m-2": "watt/meter**2",
    "watt/m^2": "watt/meter**2",
    "watt/meter^2": "watt/meter**2",
    "watts/meter^2": "watt/meter**2",
    "watts per square meter": "watt/meter**2",
    # flow / volume
    "cfs": "foot**3/second",
    "ft3/s": "foot**3/second",
    "ft^3/s": "foot**3/second",
    "cubic feet per second": "foot**3/second",
    "ac-ft": "acre_foot",
    "acre-ft": "acre_foot",
    "ac_ft": "acre_foot",
    # ratios / angles
    "pct": "percent",
    "%": "percent",
    "degrees": "degree",
    # rates
    "in/hr": "inch/hour",
    "m/s": "meter/second",
}


def normalize_unit(raw):
    """
    Normalize a source units string into something pint can parse.

    Args:
        raw: the units string inferred by a client (e.g. ``"DEG F"``,
            ``"wmoUnit:degC"``). May be ``None``.
    Returns:
        A pint-parseable string, or ``None`` if ``raw`` is ``None``. Unknown
        strings are returned unchanged so anything already valid still works.
    """
    if raw is None:
        return None
    key = str(raw).strip()
    if not key:
        return None
    # NWS returns units like "wmoUnit:degC" - strip the namespace prefix
    if ":" in key and key.lower().startswith("wmounit"):
        key = key.split(":", 1)[1]
    return _NORMALIZE.get(key.lower(), key)


def convert_series(values, from_unit, to_unit):
    """
    Convert an array of values from one unit to another.

    Offset units (degF <-> degC) are handled correctly via ``pint.Quantity``.
    NaNs are preserved. On any failure (undefined unit, incompatible
    dimensions) a warning is logged and the values are returned unchanged so a
    unit hiccup never crashes a data pull.

    Args:
        values: array-like of numeric values
        from_unit: source units string (raw, will be normalized)
        to_unit: target units string (raw, will be normalized)
    Returns:
        numpy array of converted magnitudes (plain floats, never a
        pint.Quantity), or the original values if conversion was not possible.
    """
    from_norm = normalize_unit(from_unit)
    to_norm = normalize_unit(to_unit)
    if from_norm is None or to_norm is None:
        LOG.warning(
            "Cannot convert with missing units (from=%r, to=%r); "
            "returning values unchanged", from_unit, to_unit
        )
        return values
    try:
        magnitude = np.asarray(values, dtype="float64")
        quantity = UREG.Quantity(magnitude, from_norm)
        return quantity.to(to_norm).magnitude
    except Exception as e:
        LOG.warning(
            "Failed converting from %r to %r (%s); returning values unchanged",
            from_unit, to_unit, e
        )
        return values
