# NOAA decoder fixture

`gfs-20261019-00-f116.grib2` is the actual 735-byte NOAA/NCEP GFS 00 UTC 19 October 2026 subset downloaded during the prior isolated review. Valid time: 23 October 20:00 UTC = 24 October 06:00 Brisbane. Government meteorological model data; no private records or third-party artwork.

Source: NOAA NOMADS `filter_gfs_0p25.pl`, file `gfs.t00z.pgrb2.0p25.f116`, directory `/gfs.20261019/00/atmos`, variables UGRD/VGRD/GUST, levels 10 m above ground/surface, bbox 152.5–154°E, 28–26°S.

Tests retain the real geometry/encoding and alter forecast lead times only in memory to exercise validation. Those altered values are fixtures, never published forecasts. Runtime acquisition uses only freshly downloaded files.
